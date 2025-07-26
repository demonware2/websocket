const mysql = require('mysql2/promise');
const Redis = require('ioredis');
const axios = require('axios');
const { AuthenticationError, logInfo, logWarning, logError } = require('../Helper/errorHandler');
const { enqueueMessage, setProcessMessageFunction } = require('../Helper/whatsappUtils');

const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
});

redis.on('error', (err) => console.error('Redis Client Error', err));

const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
};

let db;
let dbDynamic;

let messageQueues = new Map();
let activeProcesses = new Map();

async function initDatabase() {
    try {
        db = await mysql.createConnection(dbConfig);
        logInfo('Connected to the database');
    } catch (error) {
        process.exit(1);
        throw AuthenticationError('Error connecting to the database', 'Failed to connect to the database : ' + error);
    }
}

async function loadSettings(secret_whatsapp) {
    try {
        let keyRedis = `whatsapp_bot_key_2:${secret_whatsapp}`;
        let settings = await redis.hgetall(keyRedis);

        if (Object.keys(settings).length === 0) {
            const [rows] = await db.execute('SELECT * FROM whatsapp_bot WHERE secret_key = ?', [secret_whatsapp]);

            if (rows.length > 0) {
                settings = rows[0];
                await redis.hmset(keyRedis, settings);
            } else {
                return false;
            }
        }

        const processedSettings = {
            port: parseInt(settings.port) || 0,
            type: settings.type || 'group',
            allowed_group_ids: settings.group_id ? settings.group_id.split(',') : [],
            is_bot_active: settings.is_bot_active === '1' || settings.is_bot_active === 1 ? true : false,
            bot_model: settings.bot_model || 'basic',
            max_processes: parseInt(settings.process) || 3
        };

        return processedSettings;
    } catch (error) {
        throw error;
    }
}

async function loadBotResponses(secret_key, command, placement) {
    try {
        let keyRedis = `bot_response_basic_3:${secret_key}:${command}`;
        let response = await redis.hgetall(keyRedis);

        if (Object.keys(response).length === 0) {
            const [rows] = await db.execute('SELECT * FROM bot_basic_model WHERE secret_key = ? AND command = ? AND bot_placement = ?', [secret_key, command, placement]);

            if (rows.length > 0) {
                response = rows[0];
                if (typeof response.data === 'string') {
                    try {
                        JSON.parse(response.data);
                    } catch (e) {
                        response.data = JSON.stringify({ tahun: { bind: true } });
                    }
                } else if (typeof response.data === 'object') {
                    response.data = JSON.stringify(response.data);
                }
                await redis.hmset(keyRedis, response);
            } else {
                return false;
            }
        }

        response.is_value = response.is_value === '1' || response.is_value === 1 ? true : false;
        response.is_response = response.is_response === '1' || response.is_response === 1 ? true : false;

        if (response.data) {
            try {
                response.data = JSON.parse(response.data);
            } catch (error) {
                response.data = { tahun: { bind: true } };
            }
        }

        return response;
    } catch (error) {
        throw error;
    }
}

function formatIndonesianNumber(number) {
    return new Intl.NumberFormat('id-ID', {
        style: 'decimal',
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
    }).format(number);
}

function formatQueryResult(queryResult, isValue, isResponse, responseTemplate, inputData) {
    try {
        if (!queryResult || queryResult.length === 0) {
            throw new AuthenticationError('No results found', 'No results found');
        }

        let formattedResult = queryResult;

        if (isValue) {
            formattedResult = queryResult.map(row => Object.values(row)[0]);
        }

        let resultString;
        if (Array.isArray(formattedResult)) {
            resultString = formattedResult.map(item => {
                if (typeof item === 'object') {
                    return JSON.stringify(item);
                }
                if (typeof item === 'number' || (typeof item === 'string' && !isNaN(item))) {
                    return formatIndonesianNumber(Number(item));
                }
                return item.toString();
            }).join(', ');
        } else if (typeof formattedResult === 'object') {
            resultString = JSON.stringify(formattedResult);
        } else if (typeof formattedResult === 'number' || (typeof formattedResult === 'string' && !isNaN(formattedResult))) {
            resultString = formatIndonesianNumber(Number(formattedResult));
        } else {
            resultString = formattedResult.toString();
        }

        if (isResponse) {
            let response = responseTemplate;
            response = response.replace(/{{data}}/g, resultString);
            for (const [key, value] of Object.entries(inputData)) {
                response = response.replace(new RegExp(`{{${key}}}`, 'g'), value);
            }
            return response;
        } else {
            return resultString;
        }
    } catch (error) {
        throw error;
    }
}

async function processMessage(chatType, chatId, message, whatsappPort, secret_whatsapp, messageDelay) {
    try {
        let settings = await loadSettings(secret_whatsapp);

        if (!settings) {
            console.log('Failed to load setting')
            return { success: false, message: 'Failed to load settings' };
        }

        if (!settings.is_bot_active) {
            console.log('Bot is not active');
            return { success: false, message: 'Bot is not active' };
        }

        if (settings.type === 'group') {
            if (chatType !== 'group') {
                console.log('Group chat required')
                return { success: false, message: 'Group chat required' };
            }

            if (!settings.allowed_group_ids.includes(chatId)) {
                console.log('Group not allowed')
                return { success: false, message: 'Group not allowed' };
            }
        } else {
            if (chatType !== 'private') {
                console.log('Private chat required')
                return { success: false, message: 'Private chat required' };
            }
        }

        console.log('sukses')

        if (!messageQueues.has(whatsappPort)) {
            messageQueues.set(whatsappPort, []);
        }

        messageQueues.get(whatsappPort).push({ chatId, message, chatType });

        if (!activeProcesses.has(whatsappPort)) {
            activeProcesses.set(whatsappPort, 0);
        }

        if (activeProcesses.get(whatsappPort) < settings.max_processes) {
            processQueue(whatsappPort, settings, secret_whatsapp, messageDelay);
        }

        console.log('Sukses')

        return { success: true, message: 'Message queued for processing' };
    } catch (error) {
        throw error;
    }

}

async function processQueue(whatsappPort, settings, secret_whatsapp, messageDelay) {
    try {
        let queue = messageQueues.get(whatsappPort);
        let activeCount = activeProcesses.get(whatsappPort);

        while (queue.length > 0 && activeCount < settings.max_processes) {
            activeCount++;
            activeProcesses.set(whatsappPort, activeCount);

            let { chatId, message, chatType } = queue.shift();
            if (settings.bot_model === 'basic') {
                processItem(chatId, message, whatsappPort, secret_whatsapp, chatType, messageDelay).finally(() => {
                    activeCount--;
                    activeProcesses.set(whatsappPort, activeCount);
                    if (queue.length > 0 && activeCount < settings.max_processes) {
                        processQueue(whatsappPort, settings, secret_whatsapp);
                    }
                });
                continue;
            } else {
                activeCount--;
                activeProcesses.set(whatsappPort, activeCount);
                if (queue.length > 0 && activeCount < settings.max_processes) {
                    processQueue(whatsappPort, settings, secret_whatsapp, messageDelay);
                }
            }
        }
    } catch (error) {
        throw error;
    }
}

function parseCommandAndData(message) {
    let fullCommand = message.slice(6).trim();
    let parts = fullCommand.split(/\s+/);
    let command = parts[0];
    let dataArray = parts.slice(1);
    let dataString = dataArray.join(' ');

    return { command, dataString, dataArray };
}

function parseDataBinding(dataObject) {
    const bindings = {};

    if (typeof dataObject !== 'object' || dataObject === null) {
        logWarning('Invalid data object');
        return { tahun: true };
    }

    for (const [key, value] of Object.entries(dataObject)) {
        if (typeof value === 'object' && value !== null) {
            bindings[key] = value.bind === true;
        }
    }

    return bindings;
}

async function processQuery(query, data, dataBindings, allowedDbName) {
    try {
        const lowerQuery = query.toLowerCase();

        const dbReferenceRegex = /`?(\w+)`?\s*\.\s*`?(\w+)`?/g;
        let match;
        while ((match = dbReferenceRegex.exec(query)) !== null) {
            const referencedDb = match[1].toLowerCase();
            if (referencedDb !== allowedDbName.toLowerCase()) {
                logError(`Unauthorized database reference: ${referencedDb}`);
                return false;
            }
        }

        const disallowedKeywords = [
            'create table', 'alter table', 'drop table',
            'create database', 'alter database', 'drop database',
            'truncate table', 'rename table',
            'create index', 'drop index',
            'grant', 'revoke',
            'create user', 'alter user', 'drop user',
            'create view', 'alter view', 'drop view',
            'create procedure', 'alter procedure', 'drop procedure',
            'create function', 'alter function', 'drop function',
            'create trigger', 'drop trigger',
            'create event', 'alter event', 'drop event'
        ];

        for (const keyword of disallowedKeywords) {
            if (lowerQuery.includes(keyword)) {
                logError(`Unauthorized operation detected: ${keyword}`);
                return false;
            }
        }

        if (lowerQuery.includes('execute') || lowerQuery.includes('call ')) {
            logError('Unauthorized operation detected: Stored procedures');
            return false;
        }

        if (lowerQuery.match(/into\s+outfile/i) || lowerQuery.match(/into\s+dumpfile/i)) {
            logError('Unauthorized operation detected: File operations');
            return false;
        }

        let parameterizedQuery = query;
        const params = [];

        for (const [key, isBound] of Object.entries(dataBindings)) {
            const regex = new RegExp(`{{${key}}}`, 'g');
            if (isBound) {
                parameterizedQuery = parameterizedQuery.replace(regex, '?');
                params.push(data[key]);
            } else {
                parameterizedQuery = parameterizedQuery.replace(regex, mysql.escape(data[key]));
            }
        }

        const [rows] = await dbDynamic.execute(parameterizedQuery, params);
        return rows;
    } catch (error) {
        throw error;
    }
}

async function getDBConfig(secret_key, placement) {
    try {
        let keyRedis = `whatsapp_bot_db_basic:${secret_key}`;
        let dbConfig = await redis.hgetall(keyRedis);

        if (Object.keys(dbConfig).length === 0) {
            const [rows] = await db.execute('SELECT * FROM bot_basic_model_query_config WHERE secret_key = ? AND placement = ?', [secret_key, placement]);

            if (rows.length > 0) {
                dbConfig = rows[0];
                await redis.hmset(keyRedis, dbConfig);
            } else {
                return false;
            }
        }

        const processedDBConfig = {
            host: dbConfig.db_host,
            user: dbConfig.db_user,
            password: dbConfig.db_password,
            database: dbConfig.db_name,
        };

        return processedDBConfig;
    } catch (error) {
        throw error;
    }
}

// Simple YouTube search replacement - no external dependencies
async function searchSongYoutube(query) {
    try {
        if (!query || query.trim() === '') {
            return { success: true, message: 'No query provided' };
        }

        if (!query.toLowerCase().includes('song') && !query.toLowerCase().includes('music')) {
            query += ' song';
        }

        const searchUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(query)}`;

        return {
            success: true,
            message: `🎵 *Music Search*\n\n🔍 Query: "${query}"\n\n▶️ Search on YouTube:\n${searchUrl}\n\n💡 Tip: Click the link to find and play your music!`
        };

    } catch (error) {
        logError('Error searching YouTube:', error);
        return {
            success: false,
            message: 'Error searching for music. Please try again later.'
        };
    }
}

async function getDayOffInfo(month) {
    try {
        const monthNumber = typeof month === 'string' ? parseInt(month, 10) : month;

        if (isNaN(monthNumber) || monthNumber < 1 || monthNumber > 12) {
            const currentDate = new Date();
            month = currentDate.getMonth() + 1;
        } else {
            month = monthNumber;
        }

        const response = await axios.get(`https://dayoffapi.vercel.app/api?month=${month}`);
        const data = response.data;

        if (data.length === 0) {
            return { success: true, message: 'No day off information found' };
        }

        const monthNames = ["January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ];

        let message = `📅 *Day Off Information for ${monthNames[month - 1]}*\n\n`;

        data.forEach(item => {
            const date = new Date(item.tanggal);
            const formattedDate = `${date.getDate()} ${monthNames[date.getMonth()]} ${date.getFullYear()}`;
            const holidayType = item.is_cuti ? "🏖️ Cuti Bersama" : "🎊 Hari Libur";

            message += `*${formattedDate}*\n`;
            message += `${holidayType}: ${item.keterangan}\n\n`;
        });

        return { success: true, message: message.trim() };
    } catch (error) {
        logError('Error fetching day off information:', error);
        throw error;
    }
}

async function processItem(chatId, message, whatsappPort, secret_whatsapp, chatType, messageDelay) {
    try {
        await new Promise(resolve => setTimeout(resolve, 5000));
        let { command, dataString, dataArray } = parseCommandAndData(message);

        console.log('masuk sini')

        let botResponse = await loadBotResponses(secret_whatsapp, command, 'whatsapp');

        if (!botResponse) {
            enqueueMessage(chatId, 'Command not found', whatsappPort, chatType, messageDelay, 'checkFalse', secret_whatsapp);
            return;
        }

        let responseMessage;
        console.log(botResponse.type);

        switch (botResponse.type) {
            case 'basic':
                responseMessage = botResponse.response;
                break;
            case 'query':
                const dataBindings = parseDataBinding(botResponse.data);
                const requiredDataCount = Object.keys(dataBindings).length;

                let dbConfig = await getDBConfig(secret_whatsapp, 'whatsapp');
                if (!dbConfig) {
                    logError('Failed to load DB config');
                    enqueueMessage(chatId, 'Error: Unable to process request', whatsappPort, chatType, messageDelay, 'checkFalse', secret_whatsapp);
                    return;
                }
                dbDynamic = await mysql.createConnection(dbConfig);

                if (dataArray.length < requiredDataCount) {
                    responseMessage = `Please provide ${requiredDataCount} data for this command.`;
                } else {
                    const data = {};
                    Object.keys(dataBindings).forEach((key, index) => {
                        if (index < requiredDataCount) {
                            data[key] = dataArray[index];
                        }
                    });

                    let queryResult = await processQuery(botResponse.query, data, dataBindings, dbConfig.database);
                    if (queryResult) {
                        responseMessage = formatQueryResult(
                            queryResult,
                            botResponse.is_value,
                            botResponse.is_response,
                            botResponse.response,
                            data
                        );
                    } else {
                        responseMessage = 'Error processing data.';
                    }
                }
                break;
            case 'searchsong':
                const searchResult = await searchSongYoutube(dataString);
                if (searchResult.success) {
                    responseMessage = searchResult.message;
                } else {
                    responseMessage = 'Error searching song.';
                }
                break;
            case 'dayoff':
                const dayOffInfo = await getDayOffInfo(dataArray[0]);
                console.log(dayOffInfo);
                if (dayOffInfo.success) {
                    responseMessage = dayOffInfo.message;
                } else {
                    responseMessage = 'Error fetching day off information.';
                }
                break;
            default:
                responseMessage = 'Unknown response type.';
        }

        enqueueMessage(chatId, responseMessage, whatsappPort, chatType, messageDelay, 'checkFalse', secret_whatsapp);

    } catch (error) {
        throw error;
    }

    await new Promise(resolve => setTimeout(resolve, 5000));
}

setProcessMessageFunction(processMessage);

initDatabase();

module.exports = { processMessage };