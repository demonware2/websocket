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
    console.log(`WebSocket upgrade request from ${socket.remoteAddress} to ${request.url}`);
    
    try {
        const { pathname } = new URL(request.url, `http://${request.headers.host}`);

        const result = await verifyAuthentication(request, pathname, 'ws', socket);
        if (!result) {
            console.log('Authentication failed for:', pathname);
            // Just close the socket, don't throw error
            if (socket && !socket.destroyed) {
                socket.destroy();
            }
            return;
        }

        console.log('Authentication successful for user:', result.userId);
        const { newToken, ...user } = result;

        wss.handleUpgrade(request, socket, head, function done(ws) {
            console.log('WebSocket connection established');
            wss.emit('connection', ws, request, user, pathname);

            if (newToken) {
                ws.send(JSON.stringify({ type: 'token_update', token: newToken }));
            }
        });
    } catch (error) {
        console.error('WebSocket upgrade error:', error.message);
        
        // Just close the socket cleanly - don't crash the server
        try {
            if (socket && !socket.destroyed) {
                socket.destroy();
            }
        } catch (closeError) {
            // Ignore close errors
        }
        
        // Don't throw or re-throw the error - just handle it silently
    }
});

server.on('request', (req, res) => {
    handleRequestHttp(req, res).catch(error => {
        console.error('HTTP request error:', error.message);
        
        if (!res.headersSent) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Internal Server Error' }));
        }
    });
});

// Remove early error handler setup - will be done after server starts

console.log(`Attempting to listen on port ${PORT}...`);
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Successfully listening on 0.0.0.0:${PORT}`);
    console.log(`WebSocket server running on 0.0.0.0:${PORT}`);
    
    // Set up error handlers after server starts
    setupErrorHandlers(server, wss);
    
}).on('error', (err) => {
    console.error(`ERROR BINDING TO PORT: ${err.message}`);
    console.error(err);
    process.exit(1);
});