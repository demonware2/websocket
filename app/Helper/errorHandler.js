const fs = require('fs');
const path = require('path');
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, colorize, json } = format;
const DailyRotateFile = require('winston-daily-rotate-file');
const WebSocket = require('ws');

// Use current working directory instead of hardcoded path
const logsDir = path.join(process.cwd(), 'writable', 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

const consoleFormat = printf(({ level, message, timestamp, ...metadata }) => {
    let msg = `${timestamp} [${level}]: ${message}`;
    if (Object.keys(metadata).length > 0) {
        msg += '\n' + logStringify(metadata);
    }
    return msg;
});

function logStringify(obj, replacer = null, space = 2) {
    const seen = new WeakSet();
    return JSON.stringify(obj, function (key, value) {
        if (typeof value === 'object' && value !== null) {
            if (seen.has(value)) {
                return '[Circular]';
            }
            seen.add(value);
        }
        return replacer ? replacer.call(this, key, value) : value;
    }, space);
}

const logger = createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: combine(
        timestamp(),
        json()
    ),
    transports: [
        ...(process.env.NODE_ENV !== 'production' ? [
            new transports.Console({
                format: combine(colorize(), consoleFormat)
            })
        ] : []),
        new DailyRotateFile({
            filename: path.join(logsDir, 'error-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            level: 'error',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'error.log'
        }),
        new DailyRotateFile({
            filename: path.join(logsDir, 'warning-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            level: 'warn',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'warning.log'
        }),
        new DailyRotateFile({
            filename: path.join(logsDir, 'info-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            level: 'info',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'info.log'
        }),
        new DailyRotateFile({
            filename: path.join(logsDir, 'debug-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            level: 'debug',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'debug.log'
        }),
        new DailyRotateFile({
            filename: path.join(logsDir, 'combined-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'combined.log'
        }),
        new DailyRotateFile({
            filename: path.join(logsDir, 'requests-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '14d',
            createSymlink: true,
            symlinkName: 'requests.log'
        })
    ]
});

const requestMetrics = {
    totalRequests: 0,
    requestsPerMinute: 0,
    lastMinuteRequests: []
};

const MAX_ERROR_LOGS = 100;
const errorLogs = [];

class AuthenticationError extends Error {
    constructor(message, errorMessage = 'Internal Server Error', code = 401) {
        super(message);
        this.name = 'AuthenticationError';
        this.errorMessage = errorMessage;
        this.code = code;
    }
}

function logWebRequest(req) {
    requestMetrics.totalRequests++;
    requestMetrics.lastMinuteRequests.push(Date.now());
    updateRequestMetrics();

    logger.info('Web request', {
        method: req.method,
        url: req.url,
        ip: req.ip,
        userAgent: req.headers['user-agent']
    });
}

function updateRequestMetrics() {
    const now = Date.now();
    const oneMinuteAgo = now - 60000;
    requestMetrics.lastMinuteRequests = requestMetrics.lastMinuteRequests.filter(time => time > oneMinuteAgo);
    requestMetrics.requestsPerMinute = requestMetrics.lastMinuteRequests.length;
}

function logServerError(error, req = null) {
    const errorLog = {
        timestamp: new Date().toISOString(),
        message: error.message,
        stack: error.stack,
        request: req ? {
            method: req.method,
            url: req.url,
            ip: req.ip,
            userAgent: req.headers['user-agent']
        } : null
    };

    errorLogs.push(errorLog);
    if (errorLogs.length > MAX_ERROR_LOGS) {
        errorLogs.shift();
    }

    logger.error('Server error', errorLog);
}

function setupErrorHandlers(server = null, wss = null) {
    process.on('uncaughtException', (error) => {
        logServerError(error);
        logger.error('Uncaught Exception', { error: error.message, stack: error.stack });
    });

    process.on('unhandledRejection', (reason, promise) => {
        logServerError(new Error(`Unhandled Rejection: ${reason}`));
        logger.error('Unhandled Rejection', { reason: reason.toString(), stack: reason.stack });
    });

    if (wss) {
        wss.on('error', (error) => {
            logServerError(error);
            logger.error('WebSocket Server Error', { error: error.message, stack: error.stack });
        });
    }

    ['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(signal => {
        process.on(signal, () => {
            logger.info(`${signal} received. Shutting down.`);

            const closeHttpServer = server ? new Promise((resolve) => {
                server.close(() => {
                    logger.info('HTTP server closed.');
                    resolve();
                });
            }) : Promise.resolve();

            const closeWsServer = wss ? new Promise((resolve) => {
                wss.close(() => {
                    logger.info('WebSocket server closed.');
                    resolve();
                });
            }) : Promise.resolve();

            Promise.all([closeHttpServer, closeWsServer]).then(() => {
                logger.info('All servers closed. Exiting process.');
                process.exit(0);
            });
        });
    });
}

function getErrorLogs(limit = 10) {
    return errorLogs.slice(-limit);
}

function getRequestMetrics() {
    return { ...requestMetrics };
}

function logInfo(message, metadata = {}) {
    logger.info(message, metadata);
}

function logWarning(message, metadata = {}) {
    logger.warn(message, metadata);
}

function logDebug(message, metadata = {}) {
    logger.debug(message, metadata);
}

function logError(message, metadata = {}) {
    logger.error(message, metadata);
}

function errorMiddleware(err, req, res, next) {
    logServerError(err, req);

    res.status(500).json({
        error: 'Something went wrong. Please try again later.'
    });
}

function asyncErrorHandler(fn) {
    return (req, res, next) => {
        Promise.resolve(fn(req, res, next)).catch(next);
    };
}

function handleWebSocketError(error, ws, message = 'Something went wrong. Please try again later.') {
    logServerError(error);
    ws.send(JSON.stringify({ error: message }));
}

function handleHttpError(error, res, code = 500, message = 'Something went wrong. Please try again later.') {
    logServerError(error);
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: message }));
}

function handleError(error, connection = null, code = 500, message = 'Internal Server Error') {
    let errorMessage = 'An unexpected error occurred';

    if (error instanceof AuthenticationError) {
        code = error.code;
        message = error.message;
        errorMessage = error.errorMessage;
    } else if (error instanceof Error) {
        errorMessage = error.message;
    }

    logServerError(error);

    const errorResponse = {
        error: message,
        message: errorMessage,
        code: code,
        status: getStatusText(code)
    };

    if (connection instanceof WebSocket) {
        if (connection.readyState === WebSocket.OPEN) {
            connection.send(JSON.stringify(errorResponse));
            connection.close();
        } else {
            console.warn('WebSocket is not in OPEN state. Cannot send error message.');
        }
    } else if (connection && typeof connection.write === 'function' && typeof connection.end === 'function') {
        if (connection.writable) {
            connection.write(`HTTP/1.1 ${code} ${getStatusText(code)}\r\n`);
            connection.write('Content-Type: application/json\r\n');
            connection.write(`Content-Length: ${Buffer.byteLength(JSON.stringify(errorResponse))}\r\n`);
            connection.write('\r\n');
            connection.end(JSON.stringify(errorResponse));
        } else {
            console.warn('Socket is not writable. Cannot send error message.');
        }
    } else if (connection && typeof connection.writeHead === 'function' && typeof connection.end === 'function') {
        connection.writeHead(code, { 'Content-Type': 'application/json' });
        connection.end(JSON.stringify(errorResponse));
    } else {
        console.error('Cannot send error response. Invalid connection object.');
    }
    console.log('errorResponse', errorResponse);
    return errorResponse;
}

function getStatusText(code) {
    const statusTexts = {
        200: 'OK',
        400: 'Bad Request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not Found',
        500: 'Internal Server Error'
    };
    return statusTexts[code] || 'Unknown Status';
}

module.exports = {
    logWebRequest,
    logServerError,
    setupErrorHandlers,
    getErrorLogs,
    getRequestMetrics,
    logInfo,
    logWarning,
    logDebug,
    logError,
    errorMiddleware,
    asyncErrorHandler,
    handleError,
    handleWebSocketError,
    handleHttpError,
    AuthenticationError
};