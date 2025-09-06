const jwt = require('jsonwebtoken');
const mysql = require('mysql2/promise');
const NodeCache = require('node-cache');
const roleConfig = require('../Config/roleConfig');
const Redis = require('ioredis');
const RateLimit = require('rate-limiter-flexible');
const net = require('net');
const { AuthenticationError, handleError, logWarning, logInfo, logDebug } = require('../Helper/errorHandler');
const crypto = require('crypto');
const { log } = require('console');

const JWT_SECRET = process.env.JWT_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || 'http://localhost:3000';
const RESTRICTED_PATHS = ['/handleWhatsapp', '/anotherRestrictedPath'];
const RESTRICTED_PATHS_HTTP = ['/handleWhatsapp', '/anotherRestrictedPath'];
const MAX_CONNECTIONS_PER_USER = 30;
const MAX_TOTAL_CONNECTIONS = 1000;
const BLOCK_DURATION = 3600;
const MAX_FAILED_ATTEMPTS = 100;
const FAILED_ATTEMPTS_EXPIRY = 300;

const BLOCKED_IPS = [];

const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true,
    connectTimeout: 10000,
    commandTimeout: 5000
});

const userRoleCache = new NodeCache({ stdTTL: 3600, checkperiod: 120 });

const rateLimiter = new RateLimit.RateLimiterRedis({
    storeClient: redis,
    keyPrefix: 'ratelimit_ws',
    points: 100,
    duration: 60,
});

function isValidIP(ip) {
    return net.isIP(ip) !== 0;
}

function isIPv4(ip) {
    return net.isIPv4(ip);
}

function isIPv6(ip) {
    return net.isIPv6(ip);
}

function extractIPv4FromMapped(ip) {
    const ipv4Regex = /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/;
    const match = ip.match(ipv4Regex);
    return match ? match[1] : ip;
}

function normalizeIP(ip) {
    if (!isValidIP(ip)) {
        throw new Error('Invalid IP address');
    }

    if (isIPv6(ip)) {
        return extractIPv4FromMapped(ip);
    }

    return ip;
}

function constantTimeCompare(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') {
        return false;
    }

    const bufA = Buffer.from(a);
    const bufB = Buffer.from(b);

    if (bufA.length !== bufB.length) {
        return crypto.timingSafeEqual(bufA, bufA);
    }

    return crypto.timingSafeEqual(bufA, bufB);
}

async function isIPBlocked(normalizedIP) {

    if (BLOCKED_IPS.includes(normalizedIP)) {
        return true;
    }

    const blocked = await redis.get(`blocked_ip_ws:${normalizedIP}`);
    return blocked !== null;
}

async function blockIP(normalizedIP) {
    await redis.setex(`blocked_ip_ws:${normalizedIP}`, BLOCK_DURATION, '1');
    logWarning(`IP ${normalizedIP} has been dynamically blocked for ${BLOCK_DURATION} seconds`);
}

function isLocalNetwork(ip) {
    return ip === '127.0.0.1' ||
        ip === '::1' ||
        ip === 'localhost' ||
        ip.startsWith('172.') ||
        ip.startsWith('192.168.') ||
        ip.startsWith('10.');
}

async function verifyAuthentication(request, pathname, typeRequest, protocol) {
    try {
        const ip = request.connection.remoteAddress;
        const normalizedIP = normalizeIP(ip);

        // Skip Redis-dependent checks if Redis is not available
        let skipRedisChecks = false;
        try {
            await redis.ping();
        } catch (redisError) {
            logWarning('Redis not available, skipping Redis-dependent checks');
            skipRedisChecks = true;
        }

        if (!skipRedisChecks) {
            if (await isIPBlocked(normalizedIP)) {
                throw new AuthenticationError('IP blocked', `IP ${normalizedIP} is blocked`);
            }

            try {
                await rateLimiter.consume(normalizedIP);
            } catch (rejRes) {
                await blockIP(normalizedIP);
                throw new AuthenticationError('Rate limit exceeded', `Rate limit exceeded for IP: ${normalizedIP}`, { pathname, typeRequest });
            }
        }

        const searchParams = new URL(request.url, `http://${request.headers.host}`).searchParams;
        const token = searchParams.get('token');

        // const cleanRequestedPath = pathname.replace(/^\/socket/, '');

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

        if (typeRequest === 'ws') {
            const whatsappSecret = searchParams.get('whatsapp_secret');
            // const validatedSecret = validateWhatsAppSecret(whatsappSecret);

            if (RESTRICTED_PATHS.includes(cleanRequestedPath)) {
                if (!isLocalNetwork(normalizedIP)) {
                    throw new AuthenticationError('Unauthorized access', `Unauthorized access attempt to ${cleanRequestedPath} from IP: ${normalizedIP}`);
                } else {
                    const whatsappToken = await getWhatsappToken(whatsappSecret);

                    if (whatsappToken) {
                        logInfo('WhatsApp connection authenticated');
                        return { userId: 'whatsapp', roleId: 'whatsapp' };
                    } else {
                        throw new AuthenticationError('Invalid WhatsApp secret key', 'Invalid WhatsApp secret key');
                    }
                }
            }

            const origin = request.headers.origin;
            // Allow multiple origins for development/production
            const allowedOrigins = [
                ALLOWED_ORIGIN,
                'https://172.18.177.22',
                'https://172.29.3.178',
                'http://localhost',
                'http://127.0.0.1'
            ];
            
            if (origin && !allowedOrigins.some(allowed => origin.startsWith(allowed))) {
                throw new AuthenticationError('Invalid origin', `Invalid origin: ${origin}`);
            }
        } else {
            if (RESTRICTED_PATHS_HTTP.includes(pathname)) {
                if (!isLocalNetwork(normalizedIP)) {
                    throw new AuthenticationError('Unauthorized access', `Unauthorized access attempt to ${pathname} from IP: ${normalizedIP}`);
                }
            }
        }

        if (!token) {
            throw new AuthenticationError('No token provided', 'No token provided');
        }

        const authResult = await verifyToken(token, cleanRequestedPath, typeRequest, skipRedisChecks);

        if (typeRequest === 'ws' && !skipRedisChecks) {
            if (authResult) {
                const canConnect = await checkTotalConnectionLimit();
                if (!canConnect) {
                    throw new AuthenticationError('Total connection limit reached', `Total connection limit reached, max: ${MAX_TOTAL_CONNECTIONS}`);
                }

                const userCanConnect = await checkUserConnectionLimit(authResult.userId);
                if (!userCanConnect) {
                    throw new AuthenticationError('Connection limit reached', `User ${authResult.userId} has reached the maximum number of connections`);
                }

                await redis.del(`failed_auth:${normalizedIP}`);
            } else {
                const failedAttempts = await redis.incr(`failed_auth:${normalizedIP}`);
                await redis.expire(`failed_auth:${normalizedIP}`, FAILED_ATTEMPTS_EXPIRY);

                if (failedAttempts >= MAX_FAILED_ATTEMPTS) {
                    await blockIP(normalizedIP);
                    await redis.del(`failed_auth:${normalizedIP}`);
                    throw new AuthenticationError('Max failed attempts reached', `Max failed attempts reached for IP: ${normalizedIP}`);
                }
            }
        }

        return authResult;
    } catch (error) {
        // Don't call handleError and don't throw - just return null
        logWarning('Authentication error', { error: error.message });
        return null;
    }
}

async function verifyToken(token, cleanRequestedPath, typeRequest, skipRedisChecks = false) {
    try {
        if (!skipRedisChecks) {
            const isRevoked = await isTokenRevoked(token);
            if (isRevoked) {
                throw new AuthenticationError('Token revoked', 'Token revoked');
            }
        }

        const decoded = jwt.verify(token, JWT_SECRET);

        if (!skipRedisChecks) {
            const storedToken = await redis.get(`jwt_token:${decoded.userId}`);
            if (!storedToken || !constantTimeCompare(storedToken, token)) {
                throw new AuthenticationError('Invalid token', `Invalid token for user ${decoded.userId}`);
            }
        }

        if (typeRequest != decoded.type) {
            throw new AuthenticationError('Invalid request type', `Invalid request type for user ${decoded.userId}`);
        }

        let userRoles = getUserRolesFromCache(decoded.userId);

        if (!userRoles) {
            try {
                userRoles = await getUserRolesFromDatabase(decoded.userId);
                if (userRoles) {
                    setUserRolesToCache(decoded.userId, userRoles);
                } else {
                    // Default role if database is not available
                    userRoles = [1];
                    logDebug(`Using default role`, { userId: decoded.userId });
                }
            } catch (dbError) {
                logWarning('Database not available, using default role');
                userRoles = [1];
            }
        }

        let requiredRoles = roleConfig.routes[cleanRequestedPath];

        // Handle dynamic routes like /editor/{rpp_id}
        if (!requiredRoles && cleanRequestedPath.startsWith('/editor/')) {
            requiredRoles = roleConfig.routes['/editor'];
        }

        if (!requiredRoles) {
            // Default to allowing access if route not configured (for system routes)
            const allowedPaths = ['/handleSystemInfo', '/gatherPM2Data', '/handleChat', '/handleWhatsapp'];
            if (!allowedPaths.includes(cleanRequestedPath)) {
                throw new AuthenticationError('Route not configured', `Route not configured: ${cleanRequestedPath}`);
            }
            requiredRoles = [1]; // Default role
        }

        if (!userRoles.some(role => requiredRoles.includes(parseInt(role)))) {
            throw new AuthenticationError('Insufficient permissions', `Insufficient permissions for user ${decoded.userId} on path ${cleanRequestedPath}`);
        }

        const currentTime = Math.floor(Date.now() / 1000);
        if (!skipRedisChecks && decoded.exp - currentTime < 300) {
            return await updateToken(decoded, userRoles[0]);
        }

        logInfo(`Token verified for user ${decoded.userId} on path ${cleanRequestedPath}`);
        return { ...decoded, roleId: userRoles[0] };
    } catch (error) {
        throw error;
    }
}

async function checkTotalConnectionLimit() {
    const totalConnections = await redis.incr('ws_total_connections');
    if (totalConnections > MAX_TOTAL_CONNECTIONS) {
        await redis.decr('ws_total_connections');
        return false;
    }
    return true;
}

async function checkUserConnectionLimit(userId) {
    const connectionCount = await redis.incr(`ws_user_connections:${userId}`);
    if (connectionCount > MAX_CONNECTIONS_PER_USER) {
        await redis.decr(`ws_user_connections:${userId}`);
        return false;
    }
    await redis.expire(`ws_user_connections:${userId}`, 3600);
    return true;
}

async function removeConnection(userId) {
    await redis.decr(`ws_user_connections:${userId}`);
    await redis.decr('ws_total_connections');
}

function getUserRolesFromCache(userId) {
    const cachedRoles = userRoleCache.get(userId);

    return cachedRoles;
}

async function getWhatsappToken(key) {
    const cachedToken = await redis.get(`whatsapp_secret_key:${key}`);
    if (cachedToken) {
        return JSON.parse(cachedToken);
    }

    const [tokens] = await pool.execute('SELECT secret_key FROM whatsapp_bot WHERE secret_key = ?', [key]);
    if (tokens.length === 0) {
        throw new AuthenticationError('Invalid WhatsApp secret key', 'Invalid WhatsApp secret key');
    }

    const token = tokens[0];

    await redis.setex(`whatsapp_secret_key:${key}`, 3600, JSON.stringify(token));

    return token;
}

async function getUserRolesFromDatabase(userId) {
    const [roles] = await pool.execute('SELECT role_id FROM user_roles WHERE user_id = ?', [userId]);
    const userRoles = roles.map(role => role.role_id);

    return userRoles;
}

function setUserRolesToCache(userId, roles) {
    userRoleCache.set(userId, roles);
}

async function updateToken(decodedToken, roleId) {
    try {
        const currentTime = Math.floor(Date.now() / 1000);
        const updatedPayload = {
            ...decodedToken,
            iat: currentTime,
            exp: currentTime + 3600,
            roleId: roleId
        };

        const oldToken = await redis.get(`jwt_token:${updatedPayload.userId}`);
        const newToken = jwt.sign(updatedPayload, JWT_SECRET);

        if (oldToken) {
            await redis.sadd('revoked_tokens', oldToken);
            await redis.expire('revoked_tokens', 3600);
        }

        await redis.setex(`jwt_token:${updatedPayload.userId}`, 3600, newToken);

        return {
            ...updatedPayload,
            newToken
        };
    } catch (error) {
        handleError(error);
    }
}

function validateWhatsAppSecret(secret) {
    if (typeof secret !== 'string' || secret.length === 0) {
        throw new AuthenticationError('Invalid WhatsApp secret', 'Invalid WhatsApp secret');
    }

    // Fixed regex - was missing + for multiple characters
    const secretRegex = /^[A-Za-z0-9]+$/;
    if (!secretRegex.test(secret)) {
        throw new AuthenticationError('Invalid WhatsApp secret format', 'Invalid WhatsApp secret format');
    }

    return secret;
}

async function isTokenRevoked(token) {
    return await redis.sismember('revoked_tokens', token);
}

function invalidateUserRoleCache(userId) {
    userRoleCache.del(userId);
    logInfo(`Cache invalidated for user ${userId}`);
}

module.exports = {
    verifyAuthentication,
    verifyToken,
    updateToken,
    invalidateUserRoleCache,
    isTokenRevoked,
    removeConnection,
    blockIP,
    isIPBlocked
};
