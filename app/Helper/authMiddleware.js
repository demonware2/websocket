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
const DEFAULT_WS_ROUTE_PREFIXES = ['/siroum-websocket', '/websocket', '/node'];
const MAX_CONNECTIONS_PER_USER = parseInt(process.env.WS_MAX_CONNECTIONS_PER_USER || '30', 10);
const MAX_CUSTOMER_CONNECTIONS_PER_USER = parseInt(process.env.WS_MAX_CUSTOMER_CONNECTIONS || String(Math.min(MAX_CONNECTIONS_PER_USER, 5)), 10);
const MAX_MONITOR_CONNECTIONS_PER_ADMIN = parseInt(process.env.WS_MAX_MONITOR_CONNECTIONS || '3', 10);
const MAX_TOTAL_CONNECTIONS = parseInt(process.env.WS_MAX_TOTAL_CONNECTIONS || '1000', 10);
const BLOCK_DURATION = 3600;
const MAX_FAILED_ATTEMPTS = 100;
const FAILED_ATTEMPTS_EXPIRY = 300;
const CONNECTION_COUNTER_TTL = parseInt(process.env.WS_CONNECTION_TTL || '900', 10);

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

function sanitizeRoutePrefix(prefix) {
    if (typeof prefix !== 'string') {
        return null;
    }

    let trimmed = prefix.trim();
    if (trimmed === '') {
        return null;
    }

    if (!trimmed.startsWith('/')) {
        trimmed = `/${trimmed}`;
    }

    if (trimmed.endsWith('/') && trimmed.length > 1) {
        trimmed = trimmed.slice(0, -1);
    }

    if (trimmed === '/') {
        return null;
    }

    return trimmed;
}

const wsRoutePrefixes = (() => {
    const rawPrefixes = process.env.WS_ROUTE_PREFIXES;
    const basePrefixes = rawPrefixes ? rawPrefixes.split(',') : DEFAULT_WS_ROUTE_PREFIXES;
    const normalized = basePrefixes
        .map(sanitizeRoutePrefix)
        .filter((prefix, index, array) => prefix && array.indexOf(prefix) === index);

    return normalized;
})();

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

function matchesRoutePattern(pattern, path) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
        return false;
    }

    if (pattern.endsWith('*')) {
        const prefix = pattern.slice(0, -1);
        return path.startsWith(prefix);
    }

    return pattern === path;
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

function normalizeRequestedPath(pathname) {
    if (typeof pathname !== 'string' || pathname.length === 0) {
        return '/';
    }

    let normalized = pathname.startsWith('/') ? pathname : `/${pathname}`;

    let updated = true;
    while (updated) {
        updated = false;
        for (const prefix of wsRoutePrefixes) {
            if (!prefix) {
                continue;
            }

            if (normalized === prefix) {
                normalized = '/';
                updated = true;
                continue;
            }

            if (normalized.startsWith(`${prefix}/`)) {
                normalized = normalized.slice(prefix.length);
                if (normalized === '') {
                    normalized = '/';
                } else if (!normalized.startsWith('/')) {
                    normalized = `/${normalized}`;
                }
                updated = true;
            }
        }
    }

    if (normalized.length > 1 && normalized.endsWith('/')) {
        normalized = normalized.slice(0, -1);
    }

    return normalized;
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

        const cleanRequestedPath = normalizeRequestedPath(pathname);

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

                const scope = resolveConnectionScope(authResult);
                if (scope) {
                    const allowed = await registerConnection(scope.redisKey, scope.limit, scope.ttl);
                    if (!allowed) {
                        await decrementCounter('ws_total_connections');
                        throw new AuthenticationError('Connection limit reached', `Connection limit reached for ${scope.redisKey} (max ${scope.limit})`);
                    }
                    authResult.connectionKey = scope.redisKey;
                    authResult.connectionLimit = scope.limit;
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
        logWarning('Authentication error', {
            error: error.message,
            stack: error.stack,
            pathname,
            typeRequest,
        });
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

        const isCallCenterCustomer = cleanRequestedPath === '/call-center/chat'
            && decoded.callCenter
            && decoded.callCenter.role === 'customer';

        if (isCallCenterCustomer) {
            const sessionId = Number(decoded.callCenter.sessionId || 0);
            const authResult = {
                ...decoded,
                callCenterInternal: {
                    sessionUuid: decoded.callCenter.sessionUuid,
                    sessionId,
                    role: 'customer',
                },
                roleId: null,
                roles: [],
            };

            logInfo('Call center customer token accepted', {
                userId: decoded.userId,
                sessionId,
            });

            return authResult;
        }

        const isCallCenterMonitor = cleanRequestedPath === '/call-center/admin/broadcast'
            && decoded.callCenter
            && decoded.callCenter.role === 'admin_monitor';

        if (isCallCenterMonitor) {
            const authResult = {
                ...decoded,
                callCenterInternal: {
                    role: 'admin_monitor',
                },
                roleId: null,
                roles: Array.isArray(decoded.roles) ? decoded.roles : [],
            };

            logInfo('Call center admin monitor token accepted', {
                userId: decoded.userId,
            });

            return authResult;
        }

        const publicRoutes = Array.isArray(roleConfig.publicRoutes) ? roleConfig.publicRoutes : [];
        const isPublicRoute = publicRoutes.some((pattern) => matchesRoutePattern(pattern, cleanRequestedPath));

        let requiredRoles = roleConfig.routes[cleanRequestedPath];

        if (!requiredRoles && cleanRequestedPath.startsWith('/editor/')) {
            requiredRoles = roleConfig.routes['/editor'];
        }

        if (!requiredRoles && !isPublicRoute) {
            const allowedPaths = ['/handleSystemInfo', '/gatherPM2Data', '/handleChat', '/handleWhatsapp', '/call-center/chat', '/call-center/admin/broadcast'];
            if (!allowedPaths.includes(cleanRequestedPath)) {
                throw new AuthenticationError('Route not configured', `Route not configured: ${cleanRequestedPath}`);
            }
            requiredRoles = [1];
        }

        const requiresRoleCheck = !isPublicRoute && Array.isArray(requiredRoles) && requiredRoles.length > 0;

        const tokenRolesRaw = Array.isArray(decoded.roles) ? decoded.roles : [];
        const tokenRoles = tokenRolesRaw
            .map((role) => {
                if (typeof role === 'object' && role !== null) {
                    if (Object.prototype.hasOwnProperty.call(role, 'role_id') && isFinite(role.role_id)) {
                        return parseInt(role.role_id, 10);
                    }
                    if (Object.prototype.hasOwnProperty.call(role, 'id') && isFinite(role.id)) {
                        return parseInt(role.id, 10);
                    }
                }
                if (typeof role === 'string' && role.trim() === '') {
                    return null;
                }
                const parsed = parseInt(role, 10);
                return Number.isNaN(parsed) ? null : parsed;
            })
            .filter((value) => Number.isInteger(value));

        let userRoles = tokenRoles.length > 0 ? [...new Set(tokenRoles)] : null;

        if (requiresRoleCheck) {
            if (userRoles) {
                logDebug('Roles provided by token', {
                    userId: decoded.userId,
                    roleCount: userRoles.length,
                });
            } else {
                userRoles = getUserRolesFromCache(decoded.userId);

                logDebug('Role cache lookup', {
                    userId: decoded.userId,
                    cacheHit: !!userRoles,
                });

                if (!userRoles) {
                    try {
                        userRoles = await getUserRolesFromDatabase(decoded.userId);
                        if (userRoles && userRoles.length > 0) {
                            userRoles = userRoles.map((role) => parseInt(role, 10)).filter((value) => Number.isInteger(value));
                            setUserRolesToCache(decoded.userId, userRoles);
                        } else {
                            userRoles = [1];
                            logDebug('Using default role', { userId: decoded.userId });
                        }
                    } catch (dbError) {
                        logWarning('Database not available, using default role');
                        userRoles = [1];
                    }
                }
            }

            if (!userRoles.some((role) => requiredRoles.includes(parseInt(role, 10)))) {
                throw new AuthenticationError('Insufficient permissions', `Insufficient permissions for user ${decoded.userId} on path ${cleanRequestedPath}`);
            }
        } else {
            userRoles = userRoles ? userRoles : [];
            if (userRoles.length > 0) {
                logDebug('Public route roles preserved from token', {
                    userId: decoded.userId,
                    roleCount: userRoles.length,
                });
            }
        }

        const currentTime = Math.floor(Date.now() / 1000);
        if (requiresRoleCheck && userRoles.length > 0 && !skipRedisChecks && decoded.exp - currentTime < 300) {
            return await updateToken(decoded, userRoles[0]);
        }

        const primaryRoleId = userRoles.length > 0 ? userRoles[0] : null;
        const authResult = { ...decoded, roleId: primaryRoleId, roles: userRoles };

        if (cleanRequestedPath === '/call-center/chat') {
            authResult.callCenterInternal = { role: 'agent' };
            logInfo('Call center agent token accepted', {
                userId: decoded.userId,
                roles: userRoles,
            });
            return authResult;
        }

        logInfo(`Token verified for user ${decoded.userId} on path ${cleanRequestedPath}`);

        return authResult;
    } catch (error) {
        logWarning('verifyToken error', {
            path: cleanRequestedPath,
            message: error.message,
            stack: error.stack,
        });
        throw error;
    }
}

function resolveConnectionScope(authResult) {
    if (!authResult || typeof authResult !== 'object') {
        return null;
    }

    const ttl = Math.max(CONNECTION_COUNTER_TTL, 60);

    if (authResult.callCenterInternal && authResult.callCenterInternal.role === 'admin_monitor') {
        const adminId = authResult.actorId || authResult.userId || 'unknown';
        return {
            redisKey: `ws_connections:monitor:${adminId}`,
            limit: Math.max(1, MAX_MONITOR_CONNECTIONS_PER_ADMIN || 1),
            ttl,
        };
    }

    if (authResult.callCenterInternal && authResult.callCenterInternal.role === 'customer') {
        const customerId = authResult.userId || `session:${authResult.callCenterInternal.sessionId || 'unknown'}`;
        return {
            redisKey: `ws_connections:customer:${customerId}`,
            limit: Math.max(1, MAX_CUSTOMER_CONNECTIONS_PER_USER || 1),
            ttl,
        };
    }

    const genericId = authResult.userId || 'anonymous';
    return {
        redisKey: `ws_connections:user:${genericId}`,
        limit: Math.max(1, MAX_CONNECTIONS_PER_USER || 1),
        ttl,
    };
}

async function registerConnection(redisKey, limit, ttlSeconds) {
    if (!redisKey || limit <= 0) {
        return true;
    }

    const current = Number(await redis.incr(redisKey));
    if (current > limit) {
        await redis.decr(redisKey);
        return false;
    }

    if (ttlSeconds > 0) {
        await redis.expire(redisKey, ttlSeconds);
    }

    return true;
}

async function decrementCounter(redisKey) {
    if (!redisKey) {
        return;
    }

    try {
        const value = Number(await redis.decr(redisKey));
        if (value <= 0) {
            await redis.del(redisKey);
        } else {
            await redis.expire(redisKey, Math.max(CONNECTION_COUNTER_TTL, 60));
        }
    } catch (error) {
        logWarning('Failed to decrement connection counter', { key: redisKey, error: error.message });
    }
}

async function checkTotalConnectionLimit() {
    const totalKey = 'ws_total_connections';
    const totalConnections = Number(await redis.incr(totalKey));
    if (totalConnections > MAX_TOTAL_CONNECTIONS) {
        await redis.decr(totalKey);
        return false;
    }
    await redis.expire(totalKey, Math.max(CONNECTION_COUNTER_TTL, 60));
    return true;
}

async function removeConnection(user) {
    if (!user) {
        return;
    }

    let connectionKey = null;
    if (typeof user === 'string') {
        connectionKey = `ws_connections:user:${user}`;
    } else if (typeof user === 'object') {
        connectionKey = user.connectionKey || (user.userId ? `ws_connections:user:${user.userId}` : null);
    }

    await decrementCounter(connectionKey);
    await decrementCounter('ws_total_connections');
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
    isIPBlocked,
    normalizeRequestedPath
};
