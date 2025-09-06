const { getSystemInfo } = require('./app/Function/systemInformationMonitor');
const { handleWhatsapp } = require('./app/Function/whatsappHandler');
const { getAllDataPM2, getLogsPM2 } = require('./app/Function/pm2DataHandler');
const { handleChat } = require('./app/Function/chatHandler'); // Added chatHandler
const editorHandler = require('./app/Function/editorHandler');
const { removeConnection } = require('./app/Helper/authMiddleware');
const { logInfo, logWarning, logError, logDebug, AuthenticationError, handleError } = require('./app/Helper/errorHandler');

let wss;

function setupWebSocketServer(webSocketServer) {
    wss = webSocketServer;

    wss.on('connection', (ws, request, user, pathname) => {
        logDebug(`New client connected`, { userId: user.userId, roleId: user.roleId, path: pathname });

        ws.userId = user.userId;

        ws.on('close', () => {
            removeConnection(user.userId);
            logInfo(`Connection closed for user ${user.userId}`);
        });

        let cleanRequestedPath = pathname;

        if (cleanRequestedPath.startsWith('/websocket')) {
            cleanRequestedPath = cleanRequestedPath.replace(/^\/websocket/, '');
        }

        if (cleanRequestedPath.startsWith('/node')) {
            cleanRequestedPath = cleanRequestedPath.replace(/^\/node/, '');
        }

        if (cleanRequestedPath === '') {
            cleanRequestedPath = '/';
        }

        try {
            switch (cleanRequestedPath) {
                case '/handleSystemInfo':
                    handleSystemInfo(ws, user, request);
                    break;
                case '/handleWhatsapp':
                    handleWhatsapp(ws, request);
                    break;
                case '/gatherPM2Data':
                    gatherPM2Data(ws, user, request);
                    break;
                case '/handleChat': // Added new case for chat
                    handleChat(ws, user, request);
                    break;
                default:
                    // Check if it's an editor route
                    if (cleanRequestedPath.startsWith('/editor/')) {
                        const rppId = cleanRequestedPath.split('/')[2];
                        if (rppId) {
                            handleEditor(ws, user, request, rppId);
                        } else {
                            logWarning('Invalid editor route - missing rpp_id');
                            ws.close();
                        }
                    } else {
                        logWarning('Invalid route requested', { path: cleanRequestedPath });
                        ws.close();
                    }
            }
        } catch (error) {
            logError('WebSocket route error', { error: error.message });
            // Don't call handleError to prevent socket write issues
            if (ws && ws.readyState === ws.OPEN) {
                ws.close();
            }
        }
    });

    wss.on('error', (error) => {
        logWarning('WebSocket server error:', error);
    });
}

function gatherPM2Data(ws, user, request) {
    let intervalId;

    const sendPM2Data = async () => {
        try {
            const data = await getAllDataPM2();
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify({ type: 'pm2Data', data }));
            }
        } catch (error) {
            throw error;
        }
    };

    const sendLogs = async (pm_id) => {
        try {
            const logs = await getLogsPM2(pm_id);
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify({ type: 'logs', pm_id, logs }));
            }
        } catch (error) {
            throw error;
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
            logError('PM2 message error', { error: error.message });
            // Don't throw - just log the error
        }
    });

    ws.on('close', () => {
        removeConnection(user.userId);
        clearInterval(intervalId);
        logInfo(`Connection closed for user ${user.userId}`);
    });
}

function handleSystemInfo(ws, user, request) {
    const searchParams = new URL(request.url, `http://${request.headers.host}`).searchParams;
    const type = searchParams.get('type');

    let intervalId;

    const getSystemInfoHandler = async () => {
        try {
            const response = await getSystemInfo(type);
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify(response));
            }
        } catch (error) {
            logError('System info error', { error: error.message });
            // Don't call handleError to prevent socket write issues
        }
    };

    intervalId = setInterval(getSystemInfoHandler, 5000);

    ws.on('close', () => {
        removeConnection(user.userId);
        clearInterval(intervalId);
        logInfo(`Connection closed for user ${user.userId}`);
    });
}

async function handleEditor(ws, user, request, rppId) {
    const clientId = await editorHandler.handleConnection(ws, user, rppId);
    
    if (!clientId) {
        ws.close();
        return;
    }
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            await editorHandler.handleEditorMessage(ws, data, clientId);
        } catch (error) {
            logError('Editor message error', { error: error.message });
            // Don't call handleError to prevent socket write issues
        }
    });

    ws.on('close', () => {
        removeConnection(user.userId);
        editorHandler.handleDisconnection(clientId);
        logInfo(`Editor connection closed for user ${user.userId}, rpp_id ${rppId}`);
    });
}

module.exports = { setupWebSocketServer };
