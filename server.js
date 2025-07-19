require('dotenv').config();
require('./app/Function/botHandler');
const http = require('http');
const WebSocket = require('ws');
const { setupWebSocketServer } = require('./websocketServer');
const { verifyAuthentication } = require('./app/Helper/authMiddleware');
const { AuthenticationError ,setupErrorHandlers , handleError } = require('./app/Helper/errorHandler');
const { handleRequestHttp } = require('./requestServer');

const PORT = parseInt(process.env.PORT) || 9950;

const server = http.createServer();
const wss = new WebSocket.Server({ noServer: true });

setupWebSocketServer(wss);

server.on('upgrade', async function upgrade(request, socket, head) {
    const { pathname } = new URL(request.url, `http://${request.headers.host}`);

    try {
        const result = await verifyAuthentication(request, pathname, 'ws', socket);
        if (!result) {
            throw new AuthenticationError('Authentication failed', `Failed to authenticate user for path ${pathname}`);
        }

        const { newToken, ...user } = result;

        wss.handleUpgrade(request, socket, head, function done(ws) {
            wss.emit('connection', ws, request, user, pathname);

            if (newToken) {
                ws.send(JSON.stringify({ type: 'token_update', token: newToken }));
            }
        });
    } catch (error) {
        handleError(error, socket);
    }
});

server.on('request', (req, res) => {
    handleRequestHttp(req, res).catch(error => {
        handleError(error, res);
    });
});

setupErrorHandlers();

console.log(`Attempting to listen on port ${PORT}...`);
server.listen(PORT, () => {
    console.log(`Successfully listening on 0.0.0.0:${PORT}`);
    console.log(`WebSocket server running on 0.0.0.0:${PORT}`);
}).on('error', (err) => {
    console.error(`ERROR BINDING TO PORT: ${err.message}`);
    console.error(err);
});