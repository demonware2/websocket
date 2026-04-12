const { getSystemInfo } = require('./app/Function/systemInformationMonitor');
const { handleWhatsapp } = require('./app/Function/whatsappHandler');
const { getAllDataPM2, getLogsPM2 } = require('./app/Function/pm2DataHandler');
const { handleChat } = require('./app/Function/chatHandler'); // Added chatHandler
const { handleCallCenter, handleCallCenterAdminBroadcast } = require('./app/Function/callCenterHandler');
const editorHandler = require('./app/Function/editorHandler');
const { removeConnection, normalizeRequestedPath } = require('./app/Helper/authMiddleware');
const { logInfo, logWarning, logError, logDebug } = require('./app/Helper/errorHandler');
const Redis = require('ioredis');

let wss;

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_WS_DB || process.env.REDIS_DB || 0),
};

const redis = new Redis(redisConfig);

redis.on('connect', () => {
    logInfo('Presence Redis connected', {
        host: redisConfig.host,
        port: redisConfig.port,
        db: redisConfig.db,
    });
});

redis.on('ready', () => {
    logDebug('Presence Redis ready', {
        host: redisConfig.host,
        db: redisConfig.db,
    });
});

redis.on('error', (error) => {
    logWarning('Presence Redis error', {
        message: error.message,
        code: error.code || null,
    });
});

redis.on('end', () => {
    logWarning('Presence Redis connection closed');
});

redis.on('reconnecting', (delay) => {
    logInfo('Presence Redis reconnecting', {
        delay,
    });
});

// Global guard thresholds
const DEFAULT_MAX_MSG_BYTES = parseInt(process.env.WS_MAX_MSG_BYTES || String(256 * 1024), 10); // default 256KB
const EDITOR_MAX_MSG_BYTES = parseInt(process.env.WS_EDITOR_MAX_MSG_BYTES || String(5 * 1024 * 1024), 10); // 5MB for editor
const BUFFERED_AMOUNT_LIMIT = parseInt(process.env.WS_BUFFERED_LIMIT || String(1024 * 1024), 10); // 1MB default

const kajianPresenceRooms = new Map();
const penetapanPresenceRooms = new Map();
const PRESENCE_QUEUE_KEY = 'kajian_activity_queue';

function presenceRandomId() {
    return `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function getPresenceRoom(store, rppId) {
    if (!store.has(rppId)) {
        store.set(rppId, {
            clients: new Set(),
            users: new Map(),
        });
    }
    return store.get(rppId);
}

function getKajianPresenceRoom(rppId) {
    return getPresenceRoom(kajianPresenceRooms, rppId);
}

function getPenetapanPresenceRoom(rppId) {
    return getPresenceRoom(penetapanPresenceRooms, rppId);
}

function presenceSafeSend(ws, payload) {
    try {
        if (!ws || ws.readyState !== ws.OPEN) {
            return false;
        }
        const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
        ws.send(data);
        return true;
    } catch (error) {
        logWarning('Presence send failed', { message: error.message });
        return false;
    }
}

function serializePresenceUsers(room, excludeUserId = null) {
    const users = [];
    room.users.forEach((info, userId) => {
        if (excludeUserId && userId === excludeUserId) {
            return;
        }
        users.push({ userId, userName: info.userName });
    });
    return users;
}

function broadcastPresence(store, rppId, payload, excludeWs = null) {
    const room = store.get(rppId);
    if (!room) {
        return;
    }
    room.clients.forEach((clientWs) => {
        if (clientWs !== excludeWs) {
            presenceSafeSend(clientWs, payload);
        }
    });
}

function broadcastKajianPresence(rppId, payload, excludeWs = null) {
    broadcastPresence(kajianPresenceRooms, rppId, payload, excludeWs);
}

function broadcastPenetapanPresence(rppId, payload, excludeWs = null) {
    broadcastPresence(penetapanPresenceRooms, rppId, payload, excludeWs);
}

function truncateMeta(meta) {
    if (!meta) {
        return '';
    }
    if (typeof meta === 'string') {
        return meta.length > 512 ? meta.slice(0, 512) : meta;
    }
    try {
        const serialized = JSON.stringify(meta);
        return serialized.length > 512 ? serialized.slice(0, 512) : serialized;
    } catch (error) {
        return '';
    }
}

function enqueuePresenceActivity(event) {
    if (!redis) {
        logWarning('Presence activity skipped because Redis client is unavailable');
        return Promise.resolve();
    }

    const payload = {
        rppId: event.rppId || '',
        userId: event.userId || '',
        userName: event.userName || '',
        action: event.action || '',
        target: event.target || '',
        meta: truncateMeta(event.meta),
        createdAt: new Date().toISOString(),
    };

    const metaPreview = typeof payload.meta === 'string' ? payload.meta : JSON.stringify(payload.meta || '');
    const preview = metaPreview && metaPreview.length > 120
        ? `${metaPreview.slice(0, 120)}…`
        : metaPreview;

    logDebug('Queueing presence activity', {
        queue: PRESENCE_QUEUE_KEY,
        action: payload.action,
        target: payload.target,
        rppId: payload.rppId,
        userId: payload.userId,
        metaPreview: preview,
    });

    return redis.rpush(PRESENCE_QUEUE_KEY, JSON.stringify(payload))
        .then((queueLength) => {
            logInfo('Presence activity enqueued', {
                queue: PRESENCE_QUEUE_KEY,
                action: payload.action,
                target: payload.target,
                queueLength,
            });
            return queueLength;
        })
        .catch((error) => {
            logWarning('Presence activity enqueue failed', {
                message: error.message,
                action: payload.action,
                target: payload.target,
            });
        });
}

function detachPresence(ws, metaKey, store, broadcastFn, logPrefix) {
    const meta = ws[metaKey];
    if (!meta || meta.removed) {
        return;
    }

    meta.removed = true;
    const { rppId, userId } = meta;
    const room = store.get(rppId);
    if (!room) {
        return;
    }

    room.clients.delete(ws);

    const info = room.users.get(userId);
    if (info) {
        info.count -= 1;
        if (info.count <= 0) {
            room.users.delete(userId);
            broadcastFn(rppId, {
                type: 'presence:left',
                user: { userId, userName: info.userName },
            }, ws);
        } else {
            room.users.set(userId, info);
        }
    }

    if (room.clients.size === 0) {
        store.delete(rppId);
    }

    const leaveMeta = {
        durationMs: meta.connectedAt ? Date.now() - meta.connectedAt : null,
        connectionId: meta.connectionId,
    };

    logDebug(`${logPrefix} presence leave prepared`, {
        rppId,
        userId,
        durationMs: leaveMeta.durationMs,
        connectionId: leaveMeta.connectionId,
    });

    enqueuePresenceActivity({
        rppId,
        userId,
        userName: meta.userName || (info && info.userName) || '',
        action: `${logPrefix.toLowerCase()}_presence_leave`,
        target: `${logPrefix.toLowerCase()}_presence`,
        meta: leaveMeta,
    });

    logInfo(`${logPrefix} presence leave`, {
        rppId,
        userId,
        durationMs: leaveMeta.durationMs,
        connectionId: leaveMeta.connectionId,
    });
}

function detachKajianPresence(ws) {
    detachPresence(ws, '_kajianPresence', kajianPresenceRooms, broadcastKajianPresence, 'Kajian');
}

function detachPenetapanPresence(ws) {
    detachPresence(ws, '_penetapanPresence', penetapanPresenceRooms, broadcastPenetapanPresence, 'Penetapan');
}

function handlePresenceChannel({
    ws,
    user,
    rppId,
    roomGetter,
    store,
    metaKey,
    broadcastFn,
    detachFn,
    logPrefix,
}) {
    if (!rppId) {
        logWarning(`${logPrefix} presence refused: missing rpp_id`);
        try { ws.close(1008, 'Missing rpp_id'); } catch (_) {}
        return;
    }

    const userId = user.uuid || user.userId || user.id;
    if (!userId) {
        logWarning(`${logPrefix} presence refused: missing user identifier`);
        try { ws.close(1008, 'Missing user identifier'); } catch (_) {}
        return;
    }

    const userName = user.name || user.username || 'Pengguna';
    const room = roomGetter(rppId);
    const connectionId = presenceRandomId();

    ws[metaKey] = {
        rppId,
        userId,
        userName,
        connectionId,
        removed: false,
        connectedAt: Date.now(),
    };

    room.clients.add(ws);

    const info = room.users.get(userId) || { userName, count: 0, lastSeen: 0 };
    info.count += 1;
    info.userName = userName;
    info.lastSeen = Date.now();
    room.users.set(userId, info);

    logDebug(`${logPrefix} presence join`, { rppId, userId, connectionId });

    presenceSafeSend(ws, {
        type: 'presence:init',
        users: serializePresenceUsers(room, userId),
    });

    broadcastFn(rppId, {
        type: 'presence:join',
        user: { userId, userName },
    }, ws);

    logDebug(`${logPrefix} presence join prepared`, {
        rppId,
        userId,
        connectionId,
    });

    enqueuePresenceActivity({
        rppId,
        userId,
        userName,
        action: `${logPrefix.toLowerCase()}_presence_join`,
        target: `${logPrefix.toLowerCase()}_presence`,
        meta: { connectionId },
    });

    ws.on('message', (raw) => {
        try {
            const payload = typeof raw === 'string' ? JSON.parse(raw) : JSON.parse(raw.toString());
            if (!payload || !payload.type) return;

            const state = ws[metaKey];
            if (!state || !state.rppId) return;

            if (payload.type === 'presence:ping') {
                const currentRoom = store.get(state.rppId);
                if (!currentRoom) {
                    return;
                }
                const currentInfo = currentRoom.users.get(state.userId);
                if (currentInfo) {
                    currentInfo.lastSeen = Date.now();
                    currentRoom.users.set(state.userId, currentInfo);
                }
            } else if (payload.type.startsWith('presence:')) {
                broadcastFn(state.rppId, {
                    ...payload,
                    userId: state.userId,
                    userName: state.userName
                }, ws);
            }
        } catch (_) {
            // ignore invalid payloads for presence channel
        }
    });

    const cleanup = () => detachFn(ws);
    ws.once('close', cleanup);
    ws.once('error', cleanup);
}

function handleKajianPresence(ws, user, rppId) {
    handlePresenceChannel({
        ws,
        user,
        rppId,
        roomGetter: getKajianPresenceRoom,
        store: kajianPresenceRooms,
        metaKey: '_kajianPresence',
        broadcastFn: broadcastKajianPresence,
        detachFn: detachKajianPresence,
        logPrefix: 'Kajian',
    });
}

function handlePenetapanPresence(ws, user, rppId) {
    handlePresenceChannel({
        ws,
        user,
        rppId,
        roomGetter: getPenetapanPresenceRoom,
        store: penetapanPresenceRooms,
        metaKey: '_penetapanPresence',
        broadcastFn: broadcastPenetapanPresence,
        detachFn: detachPenetapanPresence,
        logPrefix: 'Penetapan',
    });
}

function createRateLimiter(capacity, intervalMs) {
    return {
        capacity,
        tokens: capacity,
        last: Date.now(),
        intervalMs,
        take() {
            const now = Date.now();
            const elapsed = now - this.last;
            if (elapsed >= this.intervalMs) {
                const refill = Math.floor(elapsed / this.intervalMs) * this.capacity;
                this.tokens = Math.min(this.capacity, this.tokens + refill);
                this.last = now;
            }
            if (this.tokens > 0) { this.tokens--; return true; }
            return false;
        }
    };
}

function safeSend(ws, payload) {
    try {
        if (!ws || ws.readyState !== ws.OPEN) return false;
        if (typeof ws.bufferedAmount === 'number' && ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
            try { ws.close(1011, 'Backpressure exceeded'); } catch (_) {}
            return false;
        }
        const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
        ws.send(data);
        return true;
    } catch (e) {
        return false;
    }
}

function attachGuards(ws, pathForLimits) {
    // Heartbeat state
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    // Per-connection rate limiter (tuned per route)
    let capacity = 50; // default 50 msgs per window
    let intervalMs = 10_000; // per 10 seconds
    if (pathForLimits && pathForLimits.startsWith('/editor/')) {
        capacity = 200; // editors can be chatty
    } else if (pathForLimits === '/handleChat' || pathForLimits === '/call-center/chat') {
        capacity = 100; // chats can be active
    }
    ws._rate = createRateLimiter(capacity, intervalMs);

    // Universal message guard on size and rate
    ws.on('message', (data) => {
        try {
            const size = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(String(data));
            const limit = (pathForLimits && pathForLimits.startsWith('/editor/')) ? EDITOR_MAX_MSG_BYTES : DEFAULT_MAX_MSG_BYTES;
            if (size > limit) {
                logWarning('WS message too large; closing', { size, limit });
                // Notify client with an error frame before closing
                safeSend(ws, { type: 'error', code: 'message_too_large', message: 'Message too large' });
                try { ws.close(1009, 'Message too large'); } catch (_) {}
                return;
            }
            if (!ws._rate || !ws._rate.take()) {
                logWarning('WS rate limit exceeded; closing');
                safeSend(ws, { type: 'error', code: 'rate_limit_exceeded', message: 'Rate limit exceeded' });
                try { ws.close(1008, 'Rate limit exceeded'); } catch (_) {}
                return;
            }
            if (typeof ws.bufferedAmount === 'number' && ws.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
                logWarning('WS backpressure exceeded; closing', { bufferedAmount: ws.bufferedAmount });
                safeSend(ws, { type: 'error', code: 'backpressure_exceeded', message: 'Backpressure exceeded' });
                try { ws.close(1011, 'Backpressure exceeded'); } catch (_) {}
                return;
            }
        } catch (_) { /* no-op guard */ }
    });
}

function setupWebSocketServer(webSocketServer) {
    wss = webSocketServer;

    wss.on('connection', (ws, request, user, pathname) => {
        logDebug(`New client connected`, { userId: user.userId, roleId: user.roleId, path: pathname });

        ws.userId = user.userId;

        // Attach guards (heartbeat, size/rate/backpressure)
        attachGuards(ws, pathname);

        ws.on('close', async () => {
            await removeConnection(user);
            logInfo('Connection closed', {
                userId: user.userId,
                connectionKey: user.connectionKey || null,
            });
        });

        const cleanRequestedPath = normalizeRequestedPath(pathname);

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
                case '/call-center/chat':
                    handleCallCenter(ws, user, request);
                    break;
                case '/call-center/admin/broadcast':
                    handleCallCenterAdminBroadcast(ws, user, request);
                    break;
                default:
                    if (cleanRequestedPath === '/kajian-presence' || cleanRequestedPath.startsWith('/kajian-presence/')) {
                        let rppId = null;
                        if (cleanRequestedPath.startsWith('/kajian-presence/')) {
                            rppId = cleanRequestedPath.split('/')[2] || null;
                        }
                        if (!rppId) {
                            try {
                                const searchParams = new URL(request.url, 'http://localhost').searchParams;
                                rppId = searchParams.get('rpp_id');
                            } catch (_) {
                                rppId = null;
                            }
                        }

                        if (rppId) {
                            handleKajianPresence(ws, user, rppId);
                        } else {
                            logWarning('Invalid kajian presence route - missing rpp_id');
                            ws.close();
                        }
                    } else if (cleanRequestedPath === '/penetapan-presence' || cleanRequestedPath.startsWith('/penetapan-presence/')) {
                        let rppId = null;
                        if (cleanRequestedPath.startsWith('/penetapan-presence/')) {
                            rppId = cleanRequestedPath.split('/')[2] || null;
                        }
                        if (!rppId) {
                            try {
                                const searchParams = new URL(request.url, 'http://localhost').searchParams;
                                rppId = searchParams.get('rpp_id');
                            } catch (_) {
                                rppId = null;
                            }
                        }

                        if (rppId) {
                            handlePenetapanPresence(ws, user, rppId);
                        } else {
                            logWarning('Invalid penetapan presence route - missing rpp_id');
                            ws.close();
                        }
                    } else if (cleanRequestedPath.startsWith('/editor/')) {
                        const rppId = cleanRequestedPath.split('/')[2];
                        if (rppId) {
                            handleEditor(ws, user, request, rppId);
                        } else {
                            logWarning('Invalid editor route - missing rpp_id');
                            ws.close();
                        }
                    } else {
                        logWarning('Invalid route requested', { path: cleanRequestedPath });
                        ws.close();
                    }
            }
        } catch (error) {
            logError('WebSocket route error', { error: error.message });
            // Don't call handleError to prevent socket write issues
            if (ws && ws.readyState === ws.OPEN) {
                ws.close();
            }
        }
    });

    wss.on('error', (error) => {
        logWarning('WebSocket server error:', error);
    });

    // Heartbeat: clean up dead connections
    if (!wss._heartbeatInterval) {
        wss._heartbeatInterval = setInterval(() => {
            wss.clients.forEach((client) => {
                if (client.isAlive === false) {
                    // Let client know why before hard terminate
                    try { client.close(1001, 'Ping timeout'); } catch (_) {}
                    try { client.terminate(); } catch (_) {}
                    return;
                }
                client.isAlive = false;
                try { client.ping(); } catch (_) {}
            });
        }, parseInt(process.env.WS_HEARTBEAT_INTERVAL || '30000', 10)); // 30s

        wss.on('close', () => {
            clearInterval(wss._heartbeatInterval);
            wss._heartbeatInterval = null;
        });
    }
}

function gatherPM2Data(ws, user, request) {
    let intervalId;

    const sendPM2Data = async () => {
        try {
            const data = await getAllDataPM2();
            safeSend(ws, { type: 'pm2Data', data });
        } catch (error) {
            throw error;
        }
    };

    const sendLogs = async (pm_id) => {
        try {
            const logs = await getLogsPM2(pm_id);
            safeSend(ws, { type: 'logs', pm_id, logs });
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
            logError('PM2 message error', { error: error.message });
            // Don't throw - just log the error
        }
    });

    ws.on('close', () => {
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
            safeSend(ws, response);
        } catch (error) {
            logError('System info error', { error: error.message });
            // Don't call handleError to prevent socket write issues
        }
    };

    intervalId = setInterval(getSystemInfoHandler, 5000);

    ws.on('close', () => {
        clearInterval(intervalId);
        logInfo(`Connection closed for user ${user.userId}`);
    });
}

async function handleEditor(ws, user, request, rppId) {
    const clientId = await editorHandler.handleConnection(ws, user, rppId);
    
    if (!clientId) {
        ws.close();
        return;
    }
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            await editorHandler.handleEditorMessage(ws, data, clientId);
        } catch (error) {
            logError('Editor message error', { error: error.message });
            // Don't call handleError to prevent socket write issues
        }
    });

    ws.on('close', () => {
        editorHandler.handleDisconnection(clientId);
        logInfo(`Editor connection closed for user ${user.userId}, rpp_id ${rppId}`);
    });
}

module.exports = { setupWebSocketServer };
