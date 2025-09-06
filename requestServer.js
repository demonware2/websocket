const { URL } = require('url');
const { verifyAuthentication } = require('./app/Helper/authMiddleware');
const { matrixRequest } = require('./app/Function/matrixHandler');
const { AuthenticationError, handleError, logDebug } = require('./app/Helper/errorHandler');

const MATRIX_ROUTE = '/chatmatrix';
const NODE_REQUEST = '/noderequest';

async function handleRequestHttp(req, res) {
    try {
        const { pathname } = new URL(req.url, `http://${req.headers.host}`);

        if (pathname.startsWith(MATRIX_ROUTE)) {

            await matrixRequest(req, res);

        } else if (pathname.startsWith(NODE_REQUEST)) {
            const pathRequest = req.url.replace(NODE_REQUEST, '');

            const result = await verifyAuthentication(req, pathRequest, 'http');

            if (!result) {
                throw new AuthenticationError('Authentication failed', `Failed to authenticate user for path ${pathRequest}`, 401);
            }

            switch (pathname) {
                case NODE_REQUEST + '/test1':
                    logDebug('Handling system info request');
                    break;
                default:
                    throw new AuthenticationError('Invalid route', `Invalid route requested: ${pathname}`, 404);
            }

            res.writeHead(200);
            res.end('OK');
        } else {
            throw new AuthenticationError('Invalid route', `Invalid route requested: ${pathname}`, 404);
        }

    } catch (error) {
        handleError(error, res);
    }
}

module.exports = { handleRequestHttp };
