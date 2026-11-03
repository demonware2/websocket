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
const kioskClients = new Map(); // tokenVariant -> Set of ws connections
const kioskStates = new Map();

function getTokenVariants(token) {
    if (!token) return [];
    const raw = String(token).trim();
    const cleanKiosk = raw.replace(/^kiosk_/, '');
    const cleanKsk = raw.replace(/^ksk_/, '');
    const bareHex = raw.replace(/^kiosk_|^ksk_/, '');
    const prefixedKsk = 'ksk_' + bareHex;
    const prefixedKiosk = 'kiosk_' + bareHex;
    const prefixedBoth = 'kiosk_ksk_' + bareHex;

    return [...new Set([raw, cleanKiosk, cleanKsk, bareHex, prefixedKsk, prefixedKiosk, prefixedBoth])];
}

function updateKioskHeartbeat(token) {
    if (!token) return;
    try {
        const variants = getTokenVariants(token);
        const val = new Date().toISOString().replace('T', ' ').substring(0, 19);

        variants.forEach(t => {
            const md5 = crypto.createHash('md5').update(t).digest('hex');
            redisPublisher.setex(`kiosk:heartbeat:${md5}`, 150, val).catch(() => { });
            redisPublisher.setex(`kiosk_heartbeat_${md5}`, 150, val).catch(() => { });
        });
    } catch (e) {
        logWarning('[Kiosk WS] Heartbeat update error', { error: e.message });
    }
}

function removeKioskHeartbeat(token) {
    if (!token) return;
    try {
        const variants = getTokenVariants(token);
        variants.forEach(t => {
            const md5 = crypto.createHash('md5').update(t).digest('hex');
            redisPublisher.del(`kiosk:heartbeat:${md5}`).catch(() => { });
            redisPublisher.del(`kiosk_heartbeat_${md5}`).catch(() => { });
        });
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

        const sentSockets = new Set();
        kioskClients.forEach((clientSet) => {
            clientSet.forEach(ws => {
                if (ws.readyState === 1 && !sentSockets.has(ws)) {
                    sentSockets.add(ws);
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
    }, 1000));
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
                const variants = getTokenVariants(rawTarget);
                let clientSet = null;

                for (const v of variants) {
                    const set = kioskClients.get(v);
                    if (set && set.size > 0) {
                        clientSet = set;
                        break;
                    }
                }

                if (!clientSet || clientSet.size === 0) {
                    const bareTarget = String(rawTarget).replace(/^kiosk_|^ksk_/, '');
                    for (const [key, set] of kioskClients.entries()) {
                        const bareKey = String(key).replace(/^kiosk_|^ksk_/, '');
                        if (bareKey === bareTarget && set && set.size > 0) {
                            clientSet = set;
                            break;
                        }
                    }
                }

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

                // Also notify connected admin management clients of dispatched command
                const sentSockets = new Set();
                kioskClients.forEach((set, key) => {
                    if (String(key).startsWith('admin_')) {
                        set.forEach(ws => {
                            if (ws.readyState === 1 && !sentSockets.has(ws)) {
                                sentSockets.add(ws);
                                ws.send(JSON.stringify(data));
                            }
                        });
                    }
                });
            } else {
                const sentSockets = new Set();
                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(ws => {
                        if (ws.readyState === 1 && !sentSockets.has(ws)) {
                            sentSockets.add(ws);
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

function handleKiosk(ws, user, request) {
    const searchParams = new URL(request.url, `http://${request.headers.host}`).searchParams;

    const roleParam = searchParams.get('role');
    const isAdmin = roleParam === 'admin' 
        || (user && user.role === 'admin')
        || (user && user.md && user.md.role === 'admin')
        || (user && user.identity && user.identity.startsWith('admin_'))
        || (user && user.userId && String(user.userId).startsWith('admin_'));

    let kioskToken = searchParams.get('kiosk_token')
        || (user && user.kiosk_token)
        || (user && user.md && user.md.kiosk_token)
        || (user && user.metadata && user.metadata.kiosk_token);

    if (!kioskToken && user && user.identity) {
        if (user.identity.startsWith('kiosk_')) {
            kioskToken = user.identity.substring(6);
        } else if (user.identity.startsWith('admin_')) {
            kioskToken = user.identity;
        }
    }

    if (!kioskToken && user && user.userId) {
        if (String(user.userId).startsWith('kiosk_')) {
            kioskToken = String(user.userId).substring(6);
        } else if (String(user.userId).startsWith('admin_')) {
            kioskToken = String(user.userId);
        }
    }

    if (!kioskToken && searchParams.get('token')) {
        const queryTok = searchParams.get('token');
        if (!queryTok.startsWith('eyJ')) {
            kioskToken = queryTok;
        }
    }

    if (!kioskToken && isAdmin) {
        kioskToken = (user && user.identity) || ('admin_' + ((user && user.userId) || 'session'));
    }

    if (!kioskToken) {
        logWarning('[Kiosk WS] Connection rejected: Missing kioskToken parameter');
        try { ws.close(4001, 'Missing kioskToken'); } catch (_) { }
        return;
    }

    // Register ws connection in kioskClients
    if (!kioskClients.has(kioskToken)) {
        kioskClients.set(kioskToken, new Set());
    }
    kioskClients.get(kioskToken).add(ws);

    if (!isAdmin) {
        // Register under all token variants so any command lookup succeeds
        const variants = getTokenVariants(kioskToken);
        variants.forEach(v => {
            if (!kioskClients.has(v)) {
                kioskClients.set(v, new Set());
            }
            kioskClients.get(v).add(ws);
        });

        updateKioskHeartbeat(kioskToken);
        debouncedBroadcastStatus(kioskToken, true);
        logInfo(`[Kiosk WS] Client TV display connected for token: ${kioskToken}`);
    } else {
        logInfo(`[Kiosk WS] Admin management client connected: ${kioskToken}`);
    }

    ws.send(JSON.stringify({
        type: 'connection_established',
        status: true,
        is_admin: isAdmin,
        kiosk_token: kioskToken,
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

                const sentSockets = new Set();
                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(clientWs => {
                        if (clientWs.readyState === 1 && clientWs !== ws && !sentSockets.has(clientWs)) {
                            sentSockets.add(clientWs);
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
                const sentSockets = new Set();
                kioskClients.forEach((clientSet) => {
                    clientSet.forEach(clientWs => {
                        if (clientWs.readyState === 1 && clientWs !== ws && !sentSockets.has(clientWs)) {
                            sentSockets.add(clientWs);
                            clientWs.send(JSON.stringify(data));
                        }
                    });
                });
            }
        } catch (_) { }
    });

    ws.on('close', () => {
        if (!isAdmin) {
            const variants = getTokenVariants(kioskToken);
            variants.forEach(v => {
                if (kioskClients.has(v)) {
                    const clientSet = kioskClients.get(v);
                    clientSet.delete(ws);
                    if (clientSet.size === 0) {
                        kioskClients.delete(v);
                    }
                }
            });

            const remaining = kioskClients.get(kioskToken);
            if (!remaining || remaining.size === 0) {
                removeKioskHeartbeat(kioskToken);
                debouncedBroadcastStatus(kioskToken, false);
            }
            logInfo(`[Kiosk WS] Client TV display disconnected for token: ${kioskToken}`);
        } else {
            if (kioskClients.has(kioskToken)) {
                const clientSet = kioskClients.get(kioskToken);
                clientSet.delete(ws);
                if (clientSet.size === 0) {
                    kioskClients.delete(kioskToken);
                }
            }
            logInfo(`[Kiosk WS] Admin management client disconnected: ${kioskToken}`);
        }
    });
}

module.exports = {
    handleKiosk
};
