const { getSystemInfo } = require('./app/Function/systemInformationMonitor');
const { handleWhatsapp } = require('./app/Function/whatsappHandler');
const { getAllDataPM2, getLogsPM2 } = require('./app/Function/pm2DataHandler');
const { handleChat } = require('./app/Function/chatHandler'); // Added chatHandler
const { removeConnection } = require('./app/Helper/authMiddleware');
const { logInfo, logWarning, logError, AuthenticationError, handleError } = require('./app/Helper/errorHandler');

let wss;

function setupWebSocketServer(webSocketServer) {
    wss = webSocketServer;

    wss.on('connection', (ws, request, user, pathname) => {
        logInfo(`New client connected: User ID ${user.userId}, Role ID ${user.roleId} on path ${pathname}`);

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
                    throw new AuthenticationError('Invalid route', `Invalid route requested: ${cleanRequestedPath}`);
            }
        } catch (error) {
            handleError(error, ws);
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
            throw error;
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
            handleError(error, ws);
        }
    };

    intervalId = setInterval(getSystemInfoHandler, 5000);

    ws.on('close', () => {
        removeConnection(user.userId);
        clearInterval(intervalId);
        logInfo(`Connection closed for user ${user.userId}`);
    });
}

module.exports = { setupWebSocketServer };