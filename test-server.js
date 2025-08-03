const http = require('http');
const WebSocket = require('ws');
const { setupWebSocketServer } = require('./websocketServer');

const PORT = 9951; // Use different port for testing

const server = http.createServer();
const wss = new WebSocket.Server({ 
    server: server,
    perMessageDeflate: false
});

// Simple connection handling without authentication
wss.on('connection', function connection(ws, request) {
    const url = new URL(request.url, `http://${request.headers.host}`);
    console.log('New connection:', url.pathname);
    
    ws.on('message', function message(data) {
        console.log('Received:', data.toString());
        ws.send(`Echo: ${data}`);
    });
    
    ws.on('close', function close() {
        console.log('Connection closed');
    });
    
    ws.on('error', function error(err) {
        console.log('WebSocket error:', err.message);
    });
    
    ws.send('Connected successfully');
});

wss.on('error', function(error) {
    console.error('WebSocket server error:', error);
});

server.on('error', (err) => {
    console.error(`Server error: ${err.message}`);
});

console.log(`Starting test server on port ${PORT}...`);
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Test WebSocket server running on 0.0.0.0:${PORT}`);
});