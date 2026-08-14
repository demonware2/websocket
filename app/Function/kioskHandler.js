const Redis = require('ioredis');
const crypto = require('crypto');
const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || undefined,
};

const subscriber = new Redis(redisConfig);
const redisPublisher = new Redis(redisConfig);
const kioskClients = new Map(); // kioskToken -> Set of ws connections

function updateKioskHeartbeat(token) {
    if (!token) return;
    try {
        const rawToken = String(token);
        const cleanToken = rawToken.startsWith('kiosk_') ? rawToken.substring(6) : rawToken;
        const prefixedToken = 'kiosk_' + cleanToken;

        const md5Raw = crypto.createHash('md5').update(rawToken).digest('hex');
        const md5Clean = crypto.createHash('md5').update(cleanToken).digest('hex');
        const md5Prefixed = crypto.createHash('md5').update(prefixedToken).digest('hex');
        const val = new Date().toISOString().replace('T', ' ').substring(0, 19);

        redisPublisher.setex(`kiosk:heartbeat:${md5Raw}`, 150, val).catch(() => { });
        redisPublisher.setex(`kiosk:heartbeat:${md5Clean}`, 150, val).catch(() => { });
        redisPublisher.setex(`kiosk:heartbeat:${md5Prefixed}`, 150, val).catch(() => { });
        redisPublisher.setex(`kiosk_heartbeat_${md5Raw}`, 150, val).catch(() => { });
        redisPublisher.setex(`kiosk_heartbeat_${md5Clean}`, 150, val).catch(() => { });
    } catch (e) {
        logWarning('[Kiosk WS] Heartbeat update error', { error: e.message });
    }
}

function removeKioskHeartbeat(token) {
    if (!token) return;
    try {
        const rawToken = String(token);
        const cleanToken = rawToken.startsWith('kiosk_') ? rawToken.substring(6) : rawToken;
        const prefixedToken = 'kiosk_' + cleanToken;

        const md5Raw = crypto.createHash('md5').update(rawToken).digest('hex');
        const md5Clean = crypto.createHash('md5').update(cleanToken).digest('hex');
        const md5Prefixed = crypto.createHash('md5').update(prefixedToken).digest('hex');

        redisPublisher.del(`kiosk:heartbeat:${md5Raw}`).catch(() => { });
        redisPublisher.del(`kiosk:heartbeat:${md5Clean}`).catch(() => { });
        redisPublisher.del(`kiosk:heartbeat:${md5Prefixed}`).catch(() => { });
        redisPublisher.del(`kiosk_heartbeat_${md5Raw}`).catch(() => { });
        redisPublisher.del(`kiosk_heartbeat_${md5Clean}`).catch(() => { });
    } catch (e) {
        logWarning('[Kiosk WS] Heartbeat remove error', { error: e.message });
    }
}

function broadcastKioskStatusChange(token, isOnline) {
    if (!token) return;
    try {
        const payload = {
            type: 'kiosk_status_changed',
            kiosk_token: token,
            is_online: isOnline,
            timestamp: new Date().toISOString()
        };

        kioskClients.forEach((clientSet) => {
            clientSet.forEach(ws => {
                if (ws.readyState === 1) {
                    ws.send(JSON.stringify(payload));
                }
            });
        });
    } catch (_) { }
}

const statusDebounceTimers = new Map();

function debouncedBroadcastStatus(token, isOnline) {
    if (!token) return;
    if (statusDebounceTimers.has(token)) {
        clearTimeout(statusDebounceTimers.get(token));
    }
    statusDebounceTimers.set(token, setTimeout(() => {
        statusDebounceTimers.delete(token);
        broadcastKioskStatusChange(token, isOnline);
    }, 5000));
}

subscriber.subscribe('kiosk:events', (err, count) => {
    if (err) {
        logError('[Kiosk WS] Redis subscribe error', { error: err.message });
    } else {
        logInfo('[Kiosk WS] Redis subscriber ready for kiosk:events channel');
    }
});

subscriber.on('message', (channel, message) => {
    if (channel === 'kiosk:events') {
        try {
            const data = JSON.parse(message);
            const rawTarget = data.kiosk_token || data.token;

            if (rawTarget) {
                const cleanTarget = rawTarget.startsWith('kiosk_') ? rawTarget.substring(6) : rawTarget;
                const prefixedTarget = 'kiosk_' + cleanTarget;

                const clientSet = kioskClients.get(cleanTarget) || kioskClients.get(prefixedTarget) || kioskClients.get(rawTarget);

                if (clientSet && clientSet.size > 0) {
                    clientSet.forEach(ws => {
                        if (ws.readyState === 1) { // 1 = OPEN
                            ws.send(JSON.stringify(data));
                        }
                    });
                    logInfo(`[Kiosk WS] Relayed event '${data.type || data.command}' to TV display ${rawTarget}`);
                } else {
                    logWarning(`[Kiosk WS] No active WebSocket connection found for target token: ${rawTarget}`);
                }
            } else {
                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(ws => {
                        if (ws.readyState === 1) {
                            ws.send(JSON.stringify(data));
                        }
                    });
                });
            }
        } catch (e) {
            logError('[Kiosk WS] Redis message parse error', { error: e.message });
        }
    }
});

const kioskStates = new Map();

function handleKiosk(ws, user, request) {
    const searchParams = new URL(request.url, `http://${request.headers.host}`).searchParams;
    let kioskToken = searchParams.get('kiosk_token') || (user && user.kiosk_token);

    if (!kioskToken && user && user.userId) {
        kioskToken = user.userId.startsWith('kiosk_') ? user.userId.substring(6) : user.userId;
    }
    if (!kioskToken && searchParams.get('token')) {
        const queryTok = searchParams.get('token');
        if (!queryTok.startsWith('eyJ')) {
            kioskToken = queryTok;
        }
    }

    if (!kioskToken) {
        logWarning('[Kiosk WS] Connection rejected: Missing kioskToken parameter');
        try { ws.close(4001, 'Missing kioskToken'); } catch (_) { }
        return;
    }

    if (!kioskClients.has(kioskToken)) {
        kioskClients.set(kioskToken, new Set());
    }
    kioskClients.get(kioskToken).add(ws);

    const isAdmin = String(kioskToken).startsWith('admin_');
    if (!isAdmin) {
        updateKioskHeartbeat(kioskToken);
    }

    logInfo(`[Kiosk WS] Client TV display connected for token: ${kioskToken}`);

    ws.send(JSON.stringify({
        type: 'connection_established',
        status: true,
        message: 'Terhubung ke SIROUM Kiosk WebSocket Server',
        timestamp: new Date().toISOString()
    }));

    ws.on('message', (rawMsg) => {
        try {
            const data = JSON.parse(rawMsg);
            if (data.type === 'ping') {
                if (!isAdmin) updateKioskHeartbeat(kioskToken);
                ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
            } else if (data.type === 'kiosk_display_state_changed') {
                if (!isAdmin) updateKioskHeartbeat(kioskToken);
                const cleanTok = String(data.kiosk_token || kioskToken).replace(/^kiosk_|^ksk_/, '');
                kioskStates.set(cleanTok, data);
                redisPublisher.setex('kiosk:state:' + cleanTok, 86400, JSON.stringify(data)).catch(() => { });

                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(clientWs => {
                        if (clientWs.readyState === 1 && clientWs !== ws) {
                            clientWs.send(JSON.stringify(data));
                        }
                    });
                });
            } else if (data.type === 'get_kiosk_state') {
                const reqTok = String(data.kiosk_token || data.token || kioskToken).replace(/^kiosk_|^ksk_/, '');
                const cachedState = kioskStates.get(reqTok);
                if (cachedState) {
                    ws.send(JSON.stringify(cachedState));
                } else {
                    redisPublisher.get('kiosk:state:' + reqTok).then(val => {
                        if (val) {
                            try { ws.send(val); } catch (_) { }
                        }
                    }).catch(() => { });
                }
            } else if (data.type === 'kiosk_screenshot_captured') {
                logInfo(`[Kiosk WS] Screenshot captured from TV display: ${kioskToken}`);
                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(clientWs => {
                        if (clientWs.readyState === 1 && clientWs !== ws) {
                            clientWs.send(JSON.stringify(data));
                        }
                    });
                });
            }
        } catch (_) { }
    });

    ws.on('close', () => {
        if (kioskClients.has(kioskToken)) {
            const clientSet = kioskClients.get(kioskToken);
            clientSet.delete(ws);
            if (clientSet.size === 0) {
                kioskClients.delete(kioskToken);
                if (!isAdmin) {
                    removeKioskHeartbeat(kioskToken);
                }
            }
        }
        logInfo(`[Kiosk WS] Client TV display disconnected for token: ${kioskToken}`);
    });
}

module.exports = {
    handleKiosk
};