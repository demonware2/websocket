const Redis = require('ioredis');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const os = require('os');

const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
};

const realtimePublisher = new Redis(redisConfig);
const realtimeSubscriber = new Redis(redisConfig);
const persistenceRedis = new Redis({ ...redisConfig, db: 2 });

const NODE_ID = `${os.hostname()}-${process.pid}`;

const sessionConnections = new Map();
const connectionMeta = new Map();

function ensureSessionEntry(sessionId) {
    if (!sessionConnections.has(sessionId)) {
        sessionConnections.set(sessionId, {
            agents: new Set(),
            customers: new Set(),
        });
    }
    return sessionConnections.get(sessionId);
}

function broadcastToSession(sessionId, payload, excludeWs = null) {
    const entry = sessionConnections.get(sessionId);
    if (!entry) {
        return;
    }

    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    [...entry.agents, ...entry.customers].forEach((client) => {
        if (!client || client === excludeWs) {
            return;
        }
        if (client.readyState !== WebSocket.OPEN) {
            return;
        }
        try {
            client.send(data);
        } catch (error) {
            logWarning('Failed sending to call center client', { error: error.message });
        }
    });
}

async function queueForPersistence(message) {
    try {
        await persistenceRedis.rpush('callcenter:persistence', JSON.stringify(message));
    } catch (error) {
        logError('Failed to push message to persistence queue', { error: error.message });
    }
}

function publishRealtime(sessionId, payload) {
    try {
        realtimePublisher.publish(
            `callcenter:session:${sessionId}`,
            JSON.stringify({
                ...payload,
                sessionId,
                sourceNode: NODE_ID,
            }),
        );
    } catch (error) {
        logError('Failed publishing realtime payload', { error: error.message });
    }
}

function handleInboundMessage(ws, meta, raw) {
    let data;
    try {
        data = typeof raw === 'string' ? JSON.parse(raw) : JSON.parse(raw.toString());
    } catch (error) {
        logWarning('Invalid call center payload', { error: error.message });
        return;
    }

    const { sessionId, role, sessionUuid, actorId } = meta;

    switch (data.type) {
        case 'ping':
            ws.send(JSON.stringify({ type: 'pong', at: Date.now() }));
            break;
        case 'typing':
            publishRealtime(sessionId, {
                kind: 'typing',
                actor: role,
                sessionUuid,
            });
            break;
        case 'message': {
            const content = typeof data.content === 'string' ? data.content.trim() : '';
            if (!content) {
                return;
            }

            const timestamp = new Date().toISOString();
            const message = {
                id: uuidv4(),
                sessionId,
                sessionUuid,
                senderType: role === 'agent' ? 'admin' : 'customer',
                senderId: role === 'agent' ? actorId : null,
                messageType: 'text',
                content,
                createdAt: timestamp,
            };

            queueForPersistence({
                action: 'message',
                payload: message,
            });

            const outbound = {
                type: 'callcenter:event',
                payload: {
                    kind: 'message',
                    message,
                },
            };

            broadcastToSession(sessionId, outbound);
            publishRealtime(sessionId, outbound.payload);
            break;
        }
        default:
            break;
    }
}

function cleanupConnection(ws) {
    const meta = connectionMeta.get(ws);
    if (!meta) {
        return;
    }

    const entry = sessionConnections.get(meta.sessionId);
    if (entry) {
        entry.agents.delete(ws);
        entry.customers.delete(ws);
        if (entry.agents.size === 0 && entry.customers.size === 0) {
            sessionConnections.delete(meta.sessionId);
        }
    }

    connectionMeta.delete(ws);
}

function subscribeRealtimeChannel() {
    realtimeSubscriber.psubscribe('callcenter:session:*', (err) => {
        if (err) {
            logError('Failed to subscribe call center channel', { error: err.message });
        }
    });

    realtimeSubscriber.on('pmessage', (_pattern, _channel, message) => {
        let payload;
        try {
            payload = JSON.parse(message);
        } catch (error) {
            logWarning('Invalid realtime message payload', { error: error.message });
            return;
        }

        if (payload.sourceNode && payload.sourceNode === NODE_ID) {
            return;
        }

        const sessionId = payload.sessionId;
        if (!sessionId) {
            return;
        }

        const entry = sessionConnections.get(sessionId);
        if (!entry) {
            return;
        }

        const outbound = {
            type: 'callcenter:event',
            payload,
        };

        broadcastToSession(sessionId, outbound);

        if (payload.kind === 'session_closed') {
            [...entry.agents, ...entry.customers].forEach((client) => {
                try { client.close(1000, 'Session closed'); } catch (_) {}
            });
            sessionConnections.delete(sessionId);
        }
    });
}

subscribeRealtimeChannel();

function handleCallCenter(ws, user) {
    const callCenterData = user.callCenter;
    if (!callCenterData || !callCenterData.sessionId) {
        logWarning('Missing call center data in websocket token');
        try { ws.close(4403, 'Unauthorized'); } catch (_) {}
        return;
    }

    const sessionId = callCenterData.sessionId;
    const role = callCenterData.role || 'customer';
    const sessionUuid = callCenterData.sessionUuid;
    const userId = user.userId;
    const actorId = user.actorId || null;

    const entry = ensureSessionEntry(sessionId);
    if (role === 'agent') {
        entry.agents.add(ws);
    } else {
        entry.customers.add(ws);
    }

    connectionMeta.set(ws, {
        sessionId,
        role,
        sessionUuid,
        userId,
        actorId,
    });

    ws.on('message', (raw) => handleInboundMessage(ws, connectionMeta.get(ws), raw));
    ws.on('close', () => cleanupConnection(ws));
    ws.on('error', (error) => logWarning('Call center websocket error', { error: error.message }));

    logInfo('Call center connection established', {
        sessionId,
        role,
        userId,
        actorId,
    });

    ws.send(JSON.stringify({
        type: 'callcenter:event',
        payload: {
            kind: 'connected',
            sessionId,
            role,
        },
    }));
}

module.exports = {
    handleCallCenter,
};
