require('dotenv').config();
require('./app/Function/botHandler');
const http = require('http');
const WebSocket = require('ws');
const { setupWebSocketServer } = require('./websocketServer');
const { verifyAuthentication } = require('./app/Helper/authMiddleware');
const { AuthenticationError ,setupErrorHandlers , handleError, logInfo, logWarning, logError, logDebug } = require('./app/Helper/errorHandler');
const { handleRequestHttp } = require('./requestServer');

const PORT = parseInt(process.env.PORT) || 9950;

const server = http.createServer();
// Harden WS server: limit payloads and disable compression to reduce memory/CPU spikes
const wss = new WebSocket.Server({
    noServer: true,
    // Allow larger frames to support editor payloads (default 5MB)
    maxPayload: parseInt(process.env.WS_MAX_PAYLOAD || String(5 * 1024 * 1024), 10),
    perMessageDeflate: false
});

setupWebSocketServer(wss);

server.on('upgrade', async function upgrade(request, socket, head) {
    logDebug(`WebSocket upgrade request from ${socket.remoteAddress} to ${request.url}`);
    
    try {
        const { pathname } = new URL(request.url, `http://${request.headers.host}`);

        const result = await verifyAuthentication(request, pathname, 'ws', socket);
        if (!result) {
            logWarning('Authentication failed', { pathname });
            // Just close the socket, don't throw error
            if (socket && !socket.destroyed) {
                socket.destroy();
            }
            return;
        }

        logDebug('Authentication successful', { userId: result.userId });
        const { newToken, ...user } = result;

        wss.handleUpgrade(request, socket, head, function done(ws) {
            logDebug('WebSocket connection established');
            wss.emit('connection', ws, request, user, pathname);

            if (newToken) {
                ws.send(JSON.stringify({ type: 'token_update', token: newToken }));
            }
        });
    } catch (error) {
        logError('WebSocket upgrade error', { error: error.message });
        
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
        logError('HTTP request error', { error: error.message });
        
        if (!res.headersSent) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Internal Server Error' }));
        }
    });
});

// Remove early error handler setup - will be done after server starts

logInfo(`Attempting to listen on port ${PORT}...`);
server.listen(PORT, '0.0.0.0', () => {
    logInfo(`Successfully listening on 0.0.0.0:${PORT}`);
    logInfo(`WebSocket server running on 0.0.0.0:${PORT}`);
    
    // Set up error handlers after server starts
    setupErrorHandlers(server, wss);
    
}).on('error', (err) => {
    logError('ERROR BINDING TO PORT', { error: err.message, stack: err.stack });
    process.exit(1);
});

// Conservative HTTP timeouts to avoid hanging resources
try {
    server.requestTimeout = parseInt(process.env.HTTP_REQUEST_TIMEOUT || '60000', 10); // 60s
    server.headersTimeout = parseInt(process.env.HTTP_HEADERS_TIMEOUT || '65000', 10); // 65s
    server.keepAliveTimeout = parseInt(process.env.HTTP_KEEPALIVE_TIMEOUT || '5000', 10); // 5s
} catch (_) {
    // Ignore if not supported on this Node version
}

// Ensure module-level cleanup hooks can run on shutdown
try {
    const editorHandler = require('./app/Function/editorHandler');
    ['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(sig => {
        process.on(sig, () => {
            try { editorHandler.cleanup(); } catch (_) {}
        });
    });
} catch (_) {
    // Optional dependency; safe to ignore if missing
}
