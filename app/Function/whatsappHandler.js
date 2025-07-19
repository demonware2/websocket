const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const WebSocket = require('ws');

const { enqueueMessage } = require('../Helper/whatsappUtils');
const { handleError, logInfo, logError } = require('../Helper/errorHandler');

const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
};

const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
});

const WEBHOOK_CACHE_DURATION = 3600; // 1 Hour
const MENU_TIMEOUT_DURATION = 300; // 5 Minutes

const botMenu = {
    main: {
        title: "Main Menu",
        options: {
            "1": { text: "Information", submenu: "info" },
            "2": { text: "User Management", submenu: "user" },
            "3": { text: "Settings", submenu: "settings" },
            "0": { text: "End Conversation", action: "endConversation" }
        }
    },
    info: {
        title: "Information Menu",
        options: {
            "1": { text: "About Us", action: "showAboutUs" },
            "2": { text: "Services", submenu: "services" },
            "3": { text: "Contact", action: "showContact" },
            "0": { text: "Back", action: "back" }
        }
    },

};

function handleWhatsapp(ws, request) {
    try {
        let searchParams = new URL(request.url, `http://${request.headers.host}`).searchParams;
        let whatsappPort = searchParams.get('port');
        let secret_whatsapp = searchParams.get('whatsapp_secret');

        ws.on('message', async (message) => {
            let data = JSON.parse(message);
            let userId = data.message.from;
            let isMe = data.message.fromMe;
            let isGroup = data.message.chatType === 'group';
            let to = data.message.to;
            let responseSent = false;

            if (userId == 'status@broadcast') {
                logInfo('Ignoring status@broadcast message');
                return;
            }
            
            const settings = await getSetting(secret_whatsapp);
            if (!settings) {
                throw new Error('Settings not found for the given secret key');
            }

            let dataSend = {
                chatType: data.message.chatType,
                chatId: data.chat.id,
                message: data.message.body,
                name: data.chat.name,
                sender: data.message.from,
                timestamp: data.message.timestamp
            };

            if(!isMe) {
                if (settings.webhookUrls && settings.webhookUrls.length > 0) {
                    await sendWebhooks(settings.webhookUrls, dataSend);
                    logInfo('Webhooks sent');
                } else {
                    logInfo('No webhook URLs found');
                }

                if(isGroup) {
                    if (data.message.body.startsWith('!check')) {
                        if (data.message.action === 'processMessageLocal') {
                            console.log('tes')
                            enqueueMessage(data.chat.id, data.message.body, whatsappPort, data.message.chatType, settings.messageDelay, 'checkTrue', secret_whatsapp);
                            responseSent = true;
                        } else {
                            throw new AuthenticationError('Invalid action', 'Invalid action', 400);
                        }
                    }
                } else { 
                    // if(settings.autoReply) {
                    //     if (data.message.action === 'processMessageLocal') {
                    //         if(!isGroup) {
                    //             let autoReply = await autoReplyWhatsapp(userId, settings.cooldownDuration);
                    //             if (autoReply) {
                    //                 enqueueMessage(data.chat.id, settings.autoReplyMessage, whatsappPort, data.message.chatType, settings.messageDelay, 'autoReply', secret_whatsapp);
                    //                 responseSent = true;
                    //             } else {
                    //                 logInfo(`Auto-reply skipped for ${userId} due to cooldown`);
                    //             }
                    //         } else {
                    //             logInfo('Ignoring auto-reply for group message');
                    //         }
                    //     } else {
                    //         throw new AuthenticationError('Invalid action', 'Invalid action', 400);
                    //     }

                    //     if (!responseSent && data.message.action === 'processMessageLocal') {
                    //         const botResponse = await handleBotMenu(userId, data.message.body, secret_whatsapp);
                    //         enqueueMessage(data.chat.id, botResponse, whatsappPort, data.message.chatType, settings.messageDelay, 'botMenu', secret_whatsapp);
                    //         responseSent = true;
                    //     }

                    //     console.log('tes')
                    // }

                    // logInfo('Stop Message');
                }
            } else {
                logInfo('Ignore message from self');
            }
        });

    } catch (error) {
        handleError(error);
    }
}

async function handleBotMenu(userId, message, secret_whatsapp) {
    const userStateKey = `botMenuState:${userId}:${secret_whatsapp}`;
    let userState = await redis.get(userStateKey);
    userState = userState ? JSON.parse(userState) : null;

    // Check if the conversation has ended
    if (!userState && message.toLowerCase() !== 'start') {
        return "The conversation has ended. Type 'start' to begin a new session.";
    }

    if (message.toLowerCase() === 'start' || !userState) {
        userState = { menuStack: ['main'] };
        await redis.set(userStateKey, JSON.stringify(userState), 'EX', MENU_TIMEOUT_DURATION);
        return displayMenu('main');
    }

    const currentMenuName = userState.menuStack[userState.menuStack.length - 1];
    const currentMenu = botMenu[currentMenuName];

    // Check if the current menu exists
    if (!currentMenu) {
        console.error(`Menu "${currentMenuName}" not found. Resetting to main menu.`);
        userState.menuStack = ['main'];
        await redis.set(userStateKey, JSON.stringify(userState), 'EX', MENU_TIMEOUT_DURATION);
        return `I'm sorry, there was an error. Let's start over.\n${displayMenu('main')}`;
    }

    if (message === '0') {
        if (userState.menuStack.length > 1) {
            userState.menuStack.pop();
            await redis.set(userStateKey, JSON.stringify(userState), 'EX', MENU_TIMEOUT_DURATION);
            return displayMenu(userState.menuStack[userState.menuStack.length - 1]);
        } else {
            // End the conversation
            await redis.del(userStateKey);
            return "Thank you for using our service. The conversation has ended. Type 'start' to begin a new session.";
        }
    }

    const selectedOption = currentMenu.options[message];
    if (!selectedOption) {
        return `I'm sorry, I didn't understand that input. ${displayMenu(currentMenuName)}`;
    }

    if (selectedOption.submenu) {
        userState.menuStack.push(selectedOption.submenu);
        await redis.set(userStateKey, JSON.stringify(userState), 'EX', MENU_TIMEOUT_DURATION);
        return displayMenu(selectedOption.submenu);
    }

    if (selectedOption.action) {
        const actionResult = handleMenuAction(selectedOption.action, userId, secret_whatsapp);
        if (selectedOption.action === 'endConversation') {
            await redis.del(userStateKey);
        } else {
            await redis.expire(userStateKey, MENU_TIMEOUT_DURATION);
        }
        return actionResult;
    }

    return `I'm sorry, that option is not implemented yet. ${displayMenu(currentMenuName)}`;
}


function handleMenuAction(action, userId, secret_whatsapp) {
    switch (action) {
        case 'endConversation':
            return "Thank you for using our service. The conversation has ended. Type 'start' to begin a new session.";
        case 'showAboutUs':
            return "We are a company dedicated to... \n\nWhat would you like to do next? " + displayMenu('info');
        case 'showContact':
            return "You can reach us at contact@example.com \n\nWhat would you like to do next? " + displayMenu('info');
        default:
            return `I'm sorry, that action is not implemented yet. What would you like to do next? ` + displayMenu('main');
    }
}

function displayMenu(menuName) {
    const currentMenu = botMenu[menuName];
    if (!currentMenu) {
        console.error(`Menu "${menuName}" not found in displayMenu function.`);
        return "I'm sorry, there was an error displaying the menu. Please type 'start' to begin again.";
    }
    let display = `${currentMenu.title}:\n`;
    for (const [key, option] of Object.entries(currentMenu.options)) {
        display += `${key}. ${option.text}\n`;
    }
    return display;
}

async function autoReplyWhatsapp(userId, cooldownDuration) {
    const key = `auto_reply_cooldown:${userId}`;
    const currentTime = Math.floor(Date.now() / 1000);
    
    const lastReplyTime = await redis.get(key);
    if (lastReplyTime) {
        const timeSinceLastReply = currentTime - parseInt(lastReplyTime);
        if (timeSinceLastReply < cooldownDuration) {
            return false;
        }
    }
    
    await redis.set(key, currentTime, 'EX', cooldownDuration);
    return true;
}

async function sendWebhooks(webhookUrls, data) {
    const promises = webhookUrls.map(url => {
        if (url.startsWith('http://') || url.startsWith('https://')) {
            return axios.post(url, data)
                .then(() => logInfo(`Webhook sent successfully to ${url}`))
                .catch(error => logError(`Error sending webhook to ${url}:`, error));
        } else if (url.startsWith('ws://') || url.startsWith('wss://')) {
            return new Promise((resolve, reject) => {
                const ws = new WebSocket(url);
                ws.on('open', () => {
                    ws.send(JSON.stringify(data));
                    logInfo(`Webhook sent successfully to ${url}`);
                    ws.close();
                    resolve();
                });
                ws.on('error', (error) => {
                    logError(`Error sending webhook to ${url}:`, error);
                    reject(error);
                });
            });
        } else {
            logError(`Unsupported protocol for webhook URL: ${url}`);
            return Promise.resolve();
        }
    });

    await Promise.all(promises);
}

function isValidWebhookUrl(url) {
    return url.startsWith('http://') || url.startsWith('https://') || 
           url.startsWith('ws://') || url.startsWith('wss://');
}


async function getSetting(secret_whatsapp) {
    const redisKey = `whatsapp_setting_main:${secret_whatsapp}`;
    let settings = await redis.get(redisKey);

    if (settings) {
        return JSON.parse(settings);
    }

    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute('SELECT * FROM whatsapp_bot WHERE secret_key = ?', [secret_whatsapp]);
    await connection.end();

    if (rows.length > 0) {
        settings = {
            webhookUrls: rows[0].webhook_url ? rows[0].webhook_url.split(',').map(url => url.trim()).filter(isValidWebhookUrl) : [],
            messageDelay: rows[0].message_delay || 5000,
            autoReply: rows[0].auto_reply === 1 ? true : false,
            cooldownDuration: rows[0].cooldown_duration || 600,
            autoReplyMessage: rows[0].auto_reply_message || 'This is auto reply!',
        };

        await redis.set(redisKey, JSON.stringify(settings), 'EX', WEBHOOK_CACHE_DURATION);
        return settings;
    }

    return null;
}

async function invalidateWebhookCache(secret_whatsapp) {
    const redisKey = `webhook_urls:${secret_whatsapp}`;
    await redis.del(redisKey);
}

module.exports = { handleWhatsapp };