const http = require('http');
const { URL } = require('url');
const { AuthenticationError } = require('../Helper/errorHandler');

const MATRIX_SERVER_URL = 'http://localhost:8008';
const MATRIX_ROUTE = '/chatmatrix';

function matrixRequest(req, res) {
    const matrixPath = req.url.replace(MATRIX_ROUTE, '');
    const targetUrl = new URL(matrixPath, MATRIX_SERVER_URL);

    const options = {
        method: req.method,
        headers: {
            ...req.headers,
            host: targetUrl.host
        }
    };

    const proxyReq = http.request(targetUrl, options, (proxyRes) => {
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res, { end: true });
    });

    req.pipe(proxyReq, { end: true });

    proxyReq.on('error', (error) => {
        throw new AuthenticationError('Authentication failed', `Failed to authenticate user for path ${pathname} and error: ${error}`);
    });
}

module.exports = { matrixRequest };