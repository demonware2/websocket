require('dotenv').config();
const http = require('http');
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

const { verifyAuthentication, removeConnection } = require('./app/Helper/authMiddleware');
const { logInfo, logWarning, logError, logDebug } = require('./app/Helper/errorHandler');
const { getSystemInfo } = require('./app/Function/systemInformationMonitor');
const { getAllDataPM2, getLogsPM2 } = require('./app/Function/pm2DataHandler');
const { handleChat } = require('./app/Function/chatHandler');
const { handleWhatsapp } = require('./app/Function/whatsappHandler');
const { handleCallCenter } = require('./app/Function/callCenterHandler');
const { startCallCenterPersistence } = require('./app/Function/callCenterPersistence');

const PORT = parseInt(process.env.PORT) || 9950;

const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'siroum',
    charset: 'utf8mb4',
    acquireTimeout: 60000,
    timeout: 60000
};

const redis = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || null,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true
});

const connections = new Map();
const callCenterPersistence = startCallCenterPersistence();

function safeLog(level, message, data = {}) {
    const meta = typeof data === 'object' ? data : { data };
    switch ((level || 'info').toLowerCase()) {
        case 'error':
            logError(message, meta);
            break;
        case 'warn':
        case 'warning':
            logWarning(message, meta);
            break;
        case 'debug':
            logDebug(message, meta);
            break;
        default:
            logInfo(message, meta);
    }
}

function handleError(error, context = 'Unknown') {
    safeLog('error', `Error in ${context}:`, {
        message: error.message || 'Unknown error',
        stack: error.stack || 'No stack trace'
    });
}

async function getEditorContent(rppId) {
    try {
        const connection = await mysql.createConnection(dbConfig);
        const [rows] = await connection.execute(
            'SELECT value as content, id_pk as user_id, update_date FROM rpp_hasil_kajian WHERE rpp_id = ? AND menu = "editor" ORDER BY update_date DESC LIMIT 1',
            [rppId]
        );
        await connection.end();
        return rows[0] || null;
    } catch (error) {
        handleError(error, 'Database getEditorContent');
        return null;
    }
}

async function saveEditorContent(rppId, content, userId) {
    try {
        const connection = await mysql.createConnection(dbConfig);
        const uuid = uuidv4();
        
        await connection.execute(`
            INSERT INTO rpp_hasil_kajian (rpp_id, menu, pertanyaan, value, uuid, id_pk, is_must, create_date, update_date)
            VALUES (?, 'editor', 'Editor Content', ?, ?, ?, 0, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
            value = VALUES(value),
            id_pk = VALUES(id_pk),
            update_date = VALUES(update_date)
        `, [rppId, content, uuid, userId]);
        
        await connection.end();
        return true;
    } catch (error) {
        handleError(error, 'Database saveEditorContent');
        return false;
    }
}

class EditorHandler {
    constructor() {
        this.clients = new Map();
        this.startPeriodicSync();
    }

    async handleConnection(ws, user, rppId) {
        const clientId = uuidv4();
        
        this.clients.set(clientId, {
            ws,
            user,
            rppId,
            lastActivity: Date.now()
        });

        try {
            const cachedContent = await redis.get(`editor:${rppId}:content`);
            let editorData;
            
            if (cachedContent) {
                editorData = JSON.parse(cachedContent);
            } else {
                const dbData = await getEditorContent(rppId);
                editorData = {
                    content: dbData ? dbData.content : '',
                    userId: dbData ? dbData.user_id : null,
                    timestamp: Date.now(),
                    version: 1
                };
                await redis.setex(`editor:${rppId}:content`, 1800, JSON.stringify(editorData));
            }

            this.safeSend(ws, {
                type: 'editor_init',
                data: editorData,
                clientId,
                roomInfo: {
                    rppId,
                    activeUsers: this.getActiveUsers(rppId)
                }
            });

            await redis.sadd(`editor:${rppId}:clients`, clientId);
            
            this.broadcastToEditor(rppId, {
                type: 'user_joined',
                user: user.name || user.username,
                clientId,
                timestamp: Date.now()
            }, clientId);

            safeLog('info', `Editor client connected: ${clientId} for rpp_id: ${rppId}`);
            return clientId;

        } catch (error) {
            handleError(error, 'EditorHandler.handleConnection');
            return null;
        }
    }

    async handleMessage(ws, message, clientId) {
        try {
            const client = this.clients.get(clientId);
            if (!client) return;

            const { rppId } = client;
            client.lastActivity = Date.now();

            switch (message.type) {
                case 'editor_change':
                    await this.handleEditorChange(rppId, message.data, client);
                    break;
                case 'cursor_position':
                    await this.handleCursorPosition(rppId, message.data, clientId);
                    break;
                case 'editor_save':
                    await this.handleEditorSave(rppId, message.data, client);
                    break;
                case 'ping':
                    this.safeSend(ws, { type: 'pong' });
                    break;
            }
        } catch (error) {
            handleError(error, 'EditorHandler.handleMessage');
        }
    }

    async handleEditorChange(rppId, data, client) {
        try {
            const changeData = {
                content: data.content,
                userId: client.user.userId,
                userName: client.user.name || client.user.username,
                timestamp: Date.now(),
                version: await redis.incr(`editor:${rppId}:version`)
            };

            await redis.setex(`editor:${rppId}:content`, 1800, JSON.stringify(changeData));
            
            this.broadcastToEditor(rppId, {
                type: 'editor_update',
                data: changeData
            }, client.clientId);

        } catch (error) {
            handleError(error, 'EditorHandler.handleEditorChange');
        }
    }

    async handleCursorPosition(rppId, data, clientId) {
        try {
            const client = this.clients.get(clientId);
            if (!client) return;

            this.broadcastToEditor(rppId, {
                type: 'cursor_update',
                data: {
                    clientId,
                    userName: client.user.name || client.user.username,
                    position: data.position,
                    selection: data.selection
                }
            }, clientId);
        } catch (error) {
            handleError(error, 'EditorHandler.handleCursorPosition');
        }
    }

    async handleEditorSave(rppId, data, client) {
        try {
            const success = await saveEditorContent(rppId, data.content, client.user.userId);
            
            if (success) {
                await redis.del(`editor:${rppId}:content`);
                await redis.incr(`editor:${rppId}:saved_version`);
                
                this.broadcastToEditor(rppId, {
                    type: 'editor_saved',
                    data: {
                        userId: client.user.userId,
                        userName: client.user.name || client.user.username,
                        timestamp: Date.now()
                    }
                });
                
                safeLog('info', `Editor saved for rpp_id: ${rppId} by user: ${client.user.userId}`);
            }
        } catch (error) {
            handleError(error, 'EditorHandler.handleEditorSave');
        }
    }

    getActiveUsers(rppId) {
        const users = [];
        this.clients.forEach((client, clientId) => {
            if (client.rppId === rppId) {
                users.push({
                    clientId,
                    userName: client.user.name || client.user.username,
                    userId: client.user.userId
                });
            }
        });
        return users;
    }

    broadcastToEditor(rppId, message, excludeClientId = null) {
        this.clients.forEach((client, clientId) => {
            if (client.rppId === rppId && clientId !== excludeClientId) {
                this.safeSend(client.ws, message);
            }
        });
    }

    safeSend(ws, message) {
        try {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(message));
                return true;
            }
            return false;
        } catch (error) {
            // Silently ignore socket errors - connection may be closed
            return false;
        }
    }

    async handleDisconnection(clientId) {
        try {
            const client = this.clients.get(clientId);
            if (!client) return;

            const { rppId, user } = client;
            
            await redis.srem(`editor:${rppId}:clients`, clientId);
            this.clients.delete(clientId);
            
            this.broadcastToEditor(rppId, {
                type: 'user_left',
                user: user.name || user.username,
                clientId,
                timestamp: Date.now()
            });
            
            safeLog('info', `Editor client disconnected: ${clientId} for rpp_id: ${rppId}`);
        } catch (error) {
            handleError(error, 'EditorHandler.handleDisconnection');
        }
    }

    startPeriodicSync() {
        setInterval(async () => {
            try {
                const activeRppIds = new Set();
                this.clients.forEach(client => activeRppIds.add(client.rppId));
                
                for (const rppId of activeRppIds) {
                    await this.syncToDatabase(rppId);
                }
            } catch (error) {
                handleError(error, 'EditorHandler.startPeriodicSync');
            }
        }, 300000);
    }

    async syncToDatabase(rppId) {
        try {
            const cachedData = await redis.get(`editor:${rppId}:content`);
            if (cachedData) {
                const data = JSON.parse(cachedData);
                const currentVersion = await redis.get(`editor:${rppId}:version`) || 1;
                const savedVersion = await redis.get(`editor:${rppId}:saved_version`) || 0;
                
                if (parseInt(currentVersion) > parseInt(savedVersion)) {
                    const success = await saveEditorContent(rppId, data.content, data.userId);
                    if (success) {
                        await redis.set(`editor:${rppId}:saved_version`, currentVersion);
                        safeLog('info', `Auto-synced editor data for rpp_id: ${rppId}`);
                    }
                }
            }
        } catch (error) {
            handleError(error, 'EditorHandler.syncToDatabase');
        }
    }
}

const editorHandler = new EditorHandler();
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WebSocket Server is running\n');
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', async (request, socket, head) => {
    try {
        safeLog('info', `WebSocket upgrade request from ${socket.remoteAddress} to ${request.url}`);
        
        const url = new URL(request.url, `http://${request.headers.host}`);
        let pathname = url.pathname;

        if (pathname.startsWith('/siroum-websocket')) {
            pathname = pathname.replace(/^\/siroum-websocket/, '');
        }
        if (pathname.startsWith('/websocket')) {
            pathname = pathname.replace(/^\/websocket/, '');
        }
        if (pathname.startsWith('/node')) {
            pathname = pathname.replace(/^\/node/, '');
        }
        if (pathname === '') {
            pathname = '/';
        }

        const user = await verifyAuthentication(request, pathname, 'ws', 'ws');
        if (!user) {
            safeLog('warn', `Authentication failed for path: ${pathname}`);
            socket.destroy();
            return;
        }

        safeLog('info', `Authentication successful for user: ${user.userId}`);

        wss.handleUpgrade(request, socket, head, (ws) => {
            handleConnection(ws, user, pathname, request);
        });

    } catch (error) {
        handleError(error, 'Server upgrade');
        try {
            socket.destroy();
        } catch (destroyError) {
            handleError(destroyError, 'Socket destroy');
        }
    }
});

function handleConnection(ws, user, pathname, request) {
    try {
        safeLog('info', `WebSocket connection established for ${user.userId} on ${pathname}`);
        
        const connectionId = uuidv4();
        connections.set(connectionId, { ws, user, pathname, lastActivity: Date.now(), request });

        if (pathname.startsWith('/editor/')) {
            const rppId = pathname.split('/')[2];
            if (rppId) {
                handleEditorConnection(ws, user, rppId, connectionId);
            } else {
                safeLog('warn', 'Invalid editor route - missing rpp_id');
                ws.close();
            }
        } else {
            handleOtherConnections(ws, user, pathname, connectionId, request);
        }

        ws.on('close', () => {
            handleDisconnection(connectionId);
        });

        ws.on('error', (error) => {
            handleError(error, `WebSocket error for ${connectionId}`);
            handleDisconnection(connectionId);
        });

    } catch (error) {
        handleError(error, 'handleConnection');
        try {
            ws.close();
        } catch (closeError) {
            handleError(closeError, 'WebSocket close');
        }
    }
}

async function handleEditorConnection(ws, user, rppId, connectionId) {
    try {
        const clientId = await editorHandler.handleConnection(ws, user, rppId);
        
        if (clientId) {
            ws.on('message', async (message) => {
                try {
                    const data = JSON.parse(message);
                    await editorHandler.handleMessage(ws, data, clientId);
                } catch (error) {
                    handleError(error, 'Editor message');
                }
            });
            
            const connection = connections.get(connectionId);
            if (connection) {
                connection.clientId = clientId;
            }
        }
    } catch (error) {
        handleError(error, 'handleEditorConnection');
    }
}

function handleOtherConnections(ws, user, pathname, connectionId, request) {
    try {
        switch (pathname) {
            case '/handleSystemInfo':
                handleSystemInfo(ws, user, connectionId, request);
                break;
            case '/gatherPM2Data':
                handlePM2Data(ws, user, connectionId, request);
                break;
            case '/handleChat':
                handleChatWrapper(ws, user, connectionId, request);
                break;
            case '/handleWhatsapp':
                handleWhatsappWrapper(ws, connectionId, request);
                break;
            case '/call-center/chat':
                handleCallCenterWrapper(ws, user, connectionId);
                break;
            default:
                ws.send(JSON.stringify({
                    type: 'connected',
                    message: `Connected to ${pathname}`,
                    user: user.userId
                }));
        }
    } catch (error) {
        handleError(error, 'handleOtherConnections');
    }
}

function handleSystemInfo(ws, user, connectionId, request) {
    try {
        const url = new URL(request.url, `http://${request.headers.host}`);
        const type = url.searchParams.get('type') || 'default';
        
        let intervalId;

        const getSystemInfoHandler = async () => {
            try {
                const response = await getSystemInfo(type);
                if (ws.readyState === WebSocket.OPEN) {
                    try {
                        ws.send(JSON.stringify(response));
                    } catch (sendError) {
                        // Socket closed, ignore
                    }
                }
            } catch (error) {
                handleError(error, 'getSystemInfoHandler');
            }
        };

        getSystemInfoHandler();
        intervalId = setInterval(getSystemInfoHandler, 5000);

        ws.on('close', () => {
            if (intervalId) {
                clearInterval(intervalId);
            }
            handleDisconnection(connectionId);
        });

    } catch (error) {
        handleError(error, 'handleSystemInfo');
    }
}

function handlePM2Data(ws, user, connectionId, request) {
    try {
        let intervalId;

        const sendPM2Data = async () => {
            try {
                const data = await getAllDataPM2();
                if (ws.readyState === WebSocket.OPEN) {
                    try {
                        ws.send(JSON.stringify({ type: 'pm2Data', data }));
                    } catch (sendError) {
                        // Socket closed, ignore
                    }
                }
            } catch (error) {
                handleError(error, 'sendPM2Data');
            }
        };

        const sendLogs = async (pm_id) => {
            try {
                const logs = await getLogsPM2(pm_id);
                if (ws.readyState === WebSocket.OPEN) {
                    try {
                        ws.send(JSON.stringify({ type: 'logs', pm_id, logs }));
                    } catch (sendError) {
                        // Socket closed, ignore
                    }
                }
            } catch (error) {
                handleError(error, 'sendLogs');
            }
        };

        sendPM2Data();
        intervalId = setInterval(sendPM2Data, 5000);

        ws.on('message', async (message) => {
            try {
                const data = JSON.parse(message);
                if (data.type === 'requestLogs' && data.pm_id) {
                    await sendLogs(data.pm_id);
                }
            } catch (error) {
                handleError(error, 'PM2 message handler');
            }
        });

        ws.on('close', () => {
            if (intervalId) {
                clearInterval(intervalId);
            }
            handleDisconnection(connectionId);
        });

    } catch (error) {
        handleError(error, 'handlePM2Data');
    }
}

function handleChatWrapper(ws, user, connectionId, request) {
    try {
        handleChat(ws, user, request);
        ws.on('close', () => {
            handleDisconnection(connectionId);
        });
    } catch (error) {
        handleError(error, 'handleChatWrapper');
    }
}

function handleWhatsappWrapper(ws, connectionId, request) {
    try {
        handleWhatsapp(ws, request);
        ws.on('close', () => {
            handleDisconnection(connectionId);
        });
    } catch (error) {
        handleError(error, 'handleWhatsappWrapper');
    }
}

function handleCallCenterWrapper(ws, user, connectionId) {
    try {
        handleCallCenter(ws, user);
        ws.on('close', () => {
            handleDisconnection(connectionId);
        });
    } catch (error) {
        handleError(error, 'handleCallCenterWrapper');
    }
}

async function handleDisconnection(connectionId) {
    try {
        const connection = connections.get(connectionId);
        if (connection) {
            safeLog('info', `Connection closed: ${connectionId}`);

            if (connection.clientId) {
                editorHandler.handleDisconnection(connection.clientId);
            }

            if (connection.user) {
                await removeConnection(connection.user);
            }

            connections.delete(connectionId);
        }
    } catch (error) {
        handleError(error, 'handleDisconnection');
    }
}

function gracefulShutdown() {
    safeLog('info', 'Shutting down gracefully...');
    
    try {
        if (callCenterPersistence && typeof callCenterPersistence.stop === 'function') {
            callCenterPersistence.stop();
        }

        server.close(() => {
            safeLog('info', 'HTTP server closed');
        });
        
        wss.close(() => {
            safeLog('info', 'WebSocket server closed');
        });
        
        redis.disconnect();
        safeLog('info', 'Redis disconnected');
        
        setTimeout(() => {
            process.exit(0);
        }, 5000);
    } catch (error) {
        handleError(error, 'Graceful shutdown');
        process.exit(1);
    }
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

process.on('uncaughtException', (error) => {
    handleError(error, 'Uncaught Exception');
});

process.on('unhandledRejection', (reason, promise) => {
    handleError(reason, 'Unhandled Rejection');
});

function startServer() {
    return new Promise((resolve, reject) => {
        const startTime = Date.now();
        
        server.on('error', (error) => {
            if (error.code === 'EADDRINUSE') {
                safeLog('error', `Port ${PORT} is already in use. Waiting 3 seconds before retry...`);
                setTimeout(() => {
                    if (Date.now() - startTime < 30000) {
                        startServer().then(resolve).catch(reject);
                    } else {
                        reject(new Error(`Failed to start server after 30 seconds: ${error.message}`));
                    }
                }, 3000);
            } else {
                handleError(error, 'Server error');
                reject(error);
            }
        });
        
        server.listen(PORT, '0.0.0.0', () => {
            safeLog('info', `WebSocket server running on 0.0.0.0:${PORT}`);
            resolve();
        });
    });
}

startServer().catch((error) => {
    safeLog('error', 'Failed to start server:', error);
    process.exit(1);
});
