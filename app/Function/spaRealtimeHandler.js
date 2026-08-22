const Redis = require('ioredis');
const { logInfo, logWarning, logError, logDebug } = require('../Helper/errorHandler');

/**
 * Universal Realtime Channel Manager for Siroum Template Engine & SPA
 */
const channels = new Map(); // Map<channelName, Set<WebSocket>>

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_WS_DB || process.env.REDIS_DB || 0),
};

// Dedicated Redis Subscriber client
const redisSub = new Redis(redisConfig);
const REDIS_CHANNEL = 'siroum:spa:realtime';

redisSub.subscribe(REDIS_CHANNEL, (err, count) => {
    if (err) {
        logError('[SpaRealtime] Redis subscribe error:', { error: err.message });
    } else {
        logInfo(`[SpaRealtime] Subscribed to Redis channel: ${REDIS_CHANNEL} (count: ${count})`);
    }
});

redisSub.on('message', (channel, message) => {
    if (channel !== REDIS_CHANNEL) return;

    try {
        const payload = JSON.parse(message);
        if (!payload || !payload.channel) return;

        broadcastToChannel(payload.channel, payload);
    } catch (e) {
        logWarning('[SpaRealtime] Invalid JSON message from Redis:', { message, error: e.message });
    }
});

/**
 * Broadcast message to all verified subscribers of a channel
 */
function broadcastToChannel(channelName, payload) {
    const subscribers = channels.get(channelName);
    if (!subscribers || subscribers.size === 0) return;

    const dataString = JSON.stringify({
        type: 'realtime_event',
        channel: channelName,
        ...payload
    });

    subscribers.forEach(ws => {
        if (ws.readyState === 1) { // WebSocket.OPEN
            ws.send(dataString);
        }
    });

    logDebug(`[SpaRealtime] Broadcasted to channel ${channelName} (${subscribers.size} subscribers)`);
}

/**
 * Authorize client subscription based on channel prefix & user JWT identity/roles
 */
function isAuthorizedForChannel(channelName, user) {
    if (!channelName || typeof channelName !== 'string') return false;

    // 1. Public channels: open to everyone (guests and logged-in users)
    if (channelName.startsWith('public:') || channelName.startsWith('portal:') || channelName === 'global') {
        return true;
    }

    // From here, user MUST be authenticated (not empty/unknown)
    if (!user || !user.userId || user.userId === 'unknown' || user.is_guest) {
        return false;
    }

    // 2. User-specific private channel (e.g. 'user:uuid-123')
    if (channelName.startsWith('user:')) {
        const targetUserId = channelName.replace(/^user:/, '').trim();
        return String(user.userId) === targetUserId;
    }

    // 3. Multi-Role protected channel (e.g. 'role:1,2,64' or 'admin:...')
    if (channelName.startsWith('role:')) {
        const allowedRoles = channelName.replace(/^role:/, '').split(',').map(r => parseInt(r.trim(), 10)).filter(r => !isNaN(r));
        const userRoles = Array.isArray(user.roles) ? user.roles.map(r => parseInt(r, 10)) : [];
        if (user.roleId) userRoles.push(parseInt(user.roleId, 10));

        return userRoles.some(r => allowedRoles.includes(r));
    }

    if (channelName.startsWith('admin:')) {
        const userRoles = Array.isArray(user.roles) ? user.roles.map(r => parseInt(r, 10)) : [];
        if (user.roleId) userRoles.push(parseInt(user.roleId, 10));

        return userRoles.some(r => r === 1);
    }

    return true;
}

/**
 * Handle new WebSocket connection to /realtime
 */
function handleSpaRealtime(ws, user, request) {
    ws.subscribedChannels = new Set();

    logInfo('[SpaRealtime] Client connected to /realtime', {
        userId: user?.userId || 'guest',
        roles: user?.roles || [],
    });

    ws.send(JSON.stringify({
        type: 'realtime_connected',
        message: 'Connected to Siroum Universal Realtime Hub',
        userId: user?.userId || 'guest',
        timestamp: Date.now()
    }));

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());

            if (msg.action === 'subscribe' && msg.channel) {
                const channel = String(msg.channel).trim();

                if (!isAuthorizedForChannel(channel, user)) {
                    ws.send(JSON.stringify({
                        type: 'realtime_error',
                        action: 'subscribe',
                        channel: channel,
                        error: 'Unauthorized: You do not have permission to join this channel'
                    }));
                    return;
                }

                if (!channels.has(channel)) {
                    channels.set(channel, new Set());
                }
                channels.get(channel).add(ws);
                ws.subscribedChannels.add(channel);

                ws.send(JSON.stringify({
                    type: 'realtime_subscribed',
                    channel: channel,
                    status: 'success'
                }));
                logDebug(`[SpaRealtime] User ${user?.userId} subscribed to ${channel}`);
            }

            if (msg.action === 'unsubscribe' && msg.channel) {
                const channel = String(msg.channel).trim();
                if (channels.has(channel)) {
                    channels.get(channel).delete(ws);
                    if (channels.get(channel).size === 0) {
                        channels.delete(channel);
                    }
                }
                ws.subscribedChannels.delete(channel);

                ws.send(JSON.stringify({
                    type: 'realtime_unsubscribed',
                    channel: channel
                }));
            }

            if (msg.action === 'ping') {
                ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
            }
        } catch (e) {
            logWarning('[SpaRealtime] Error processing client message:', { error: e.message });
        }
    });

    ws.on('close', () => {
        if (ws.subscribedChannels) {
            ws.subscribedChannels.forEach(channel => {
                if (channels.has(channel)) {
                    channels.get(channel).delete(ws);
                    if (channels.get(channel).size === 0) {
                        channels.delete(channel);
                    }
                }
            });
        }
        logDebug(`[SpaRealtime] Client disconnected, cleaned up subscriptions for user ${user?.userId}`);
    });
}

module.exports = {
    handleSpaRealtime,
    broadcastToChannel
};