const Redis = require('ioredis');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const os = require('os');
const crypto = require('crypto');

const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
};

const realtimePublisher = new Redis(redisConfig);
const realtimeSubscriber = new Redis(redisConfig);
const persistenceRedis = new Redis({ ...redisConfig, db: 2 });

const EXPIRY_DB = Number(process.env.CALL_CENTER_EXPIRY_DB || 3);
const SESSION_INACTIVITY_TTL = Number(process.env.CALL_CENTER_INACTIVE_TIMEOUT_SECONDS || 900);
const EXPIRY_KEY_PREFIX = process.env.CALL_CENTER_EXPIRY_KEY_PREFIX || 'callcenter:session:expiry:';

const expiryRedis = SESSION_INACTIVITY_TTL > 0 ? new Redis({ ...redisConfig, db: EXPIRY_DB }) : null;
const expirySubscriber = SESSION_INACTIVITY_TTL > 0 ? new Redis({ ...redisConfig, db: EXPIRY_DB }) : null;

const PRIMARY_ATTACHMENT_SECRET = process.env.LIVECHAT_ATTACHMENT_SECRET
    || process.env.ENCRYPTION_KEY
    || 'livechat-secret';

const ATTACHMENT_SECRETS = [
    PRIMARY_ATTACHMENT_SECRET,
    process.env.ENCRYPTION_KEY,
].filter((value, index, array) => typeof value === 'string' && value.length > 0 && array.indexOf(value) === index);

function buildAttachmentTokenWithSecret(secret, sessionId, filePath) {
    const effectiveSecret = secret || 'livechat-secret';

    return crypto
        .createHmac('sha256', effectiveSecret)
        .update(`${sessionId}|${filePath}`)
        .digest('hex');
}

function verifyAttachmentToken(sessionId, filePath, token) {
    if (!token || !filePath) {
        return false;
    }

    return ATTACHMENT_SECRETS.some((secret) => {
        try {
            const expected = buildAttachmentTokenWithSecret(secret, sessionId, filePath);
            return expected === token;
        } catch (error) {
            logWarning('Failed to compute attachment token', {
                error: error.message,
                sessionId,
                filePath,
            });
            return false;
        }
    });
}

if (expiryRedis) {
    expiryRedis.on('error', (error) => {
        logWarning('Call center expiry redis error', { error: error.message });
    });
}

if (expirySubscriber) {
    expirySubscriber.on('error', (error) => {
        logWarning('Call center expiry subscriber error', { error: error.message });
    });
}

const NODE_ID = `${os.hostname()}-${process.pid}`;

const sessionConnections = new Map();
const connectionMeta = new Map();
const monitorConnections = new Set();

const INACTIVITY_TTL_SECONDS = SESSION_INACTIVITY_TTL > 0 ? Math.max(SESSION_INACTIVITY_TTL, 30) : 0;

function refreshSessionExpiry(sessionId) {
    if (!expiryRedis || !sessionId || INACTIVITY_TTL_SECONDS <= 0) {
        return;
    }

    expiryRedis
        .set(`${EXPIRY_KEY_PREFIX}${sessionId}`, Date.now().toString(), 'EX', INACTIVITY_TTL_SECONDS)
        .catch((error) => {
            logWarning('Failed to refresh session inactivity TTL', { error: error.message, sessionId });
        });
}

function clearSessionExpiry(sessionId) {
    if (!expiryRedis || !sessionId) {
        return;
    }

    expiryRedis
        .del(`${EXPIRY_KEY_PREFIX}${sessionId}`)
        .catch((error) => {
            logWarning('Failed to clear session inactivity TTL', { error: error.message, sessionId });
        });
}

async function ensureExpiryNotificationSupport() {
    if (!expiryRedis || INACTIVITY_TTL_SECONDS <= 0) {
        return;
    }

    try {
        const configResult = await expiryRedis.config('GET', 'notify-keyspace-events');
        const currentValue = Array.isArray(configResult) ? configResult[1] || '' : '';
        let desiredValue = typeof currentValue === 'string' ? currentValue : '';

        if (!desiredValue.includes('E')) {
            desiredValue += 'E';
        }
        if (!desiredValue.includes('x')) {
            desiredValue += 'x';
        }

        if (desiredValue !== currentValue) {
            await expiryRedis.config('SET', 'notify-keyspace-events', desiredValue);
        }
    } catch (error) {
        logWarning('Unable to configure Redis keyspace notifications', { error: error.message });
        throw error;
    }
}

function subscribeExpiryEvents() {
    if (!expirySubscriber || INACTIVITY_TTL_SECONDS <= 0) {
        return;
    }

    const channel = `__keyevent@${EXPIRY_DB}__:expired`;

    expirySubscriber.subscribe(channel, (error) => {
        if (error) {
            logWarning('Failed subscribing to inactivity expiry channel', { error: error.message });
        }
    });

    expirySubscriber.on('message', (_channel, key) => {
        if (typeof key !== 'string' || !key.startsWith(EXPIRY_KEY_PREFIX)) {
            return;
        }

        const sessionId = Number(key.slice(EXPIRY_KEY_PREFIX.length));
        if (!Number.isFinite(sessionId) || sessionId <= 0) {
            return;
        }

        queueForPersistence({
            action: 'expire',
            payload: {
                sessionId,
                reason: 'inactivity',
                emittedAt: new Date().toISOString(),
            },
        }).catch((error) => {
            logError('Failed to enqueue inactivity expiration', { error: error.message, sessionId });
        });
    });
}

function initExpiryMonitor() {
    if (!expiryRedis || !expirySubscriber || INACTIVITY_TTL_SECONDS <= 0) {
        return;
    }

    ensureExpiryNotificationSupport()
        .catch(() => {})
        .finally(() => {
            subscribeExpiryEvents();
        });
}

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

function broadcastToMonitors(payload) {
    if (monitorConnections.size === 0) {
        return;
    }

    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    monitorConnections.forEach((client) => {
        if (!client || client.readyState !== WebSocket.OPEN) {
            return;
        }

        try {
            client.send(data);
        } catch (error) {
            logWarning('Failed sending to call center monitor', { error: error.message });
        }
    });
}

function emitMonitorEvent(event, details = {}) {
    broadcastToMonitors({
        type: 'callcenter:dashboard',
        payload: {
            event,
            emittedAt: new Date().toISOString(),
            ...details,
        },
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
            refreshSessionExpiry(sessionId);
            break;
        case 'typing':
            publishRealtime(sessionId, {
                kind: 'typing',
                actor: role,
                sessionUuid,
            });
            break;
        case 'message': {
            const rawContent = typeof data.content === 'string' ? data.content : '';
            const attachmentsInput = Array.isArray(data.attachments) ? data.attachments : [];

            const attachments = attachmentsInput.reduce((list, attachment) => {
                if (!attachment || typeof attachment !== 'object') {
                    return list;
                }

                const attachmentId = Number(attachment.attachmentId || attachment.id || 0);
                const fileName = typeof attachment.fileName === 'string' ? attachment.fileName : null;
                const downloadToken = typeof attachment.downloadToken === 'string' ? attachment.downloadToken : (typeof attachment.token === 'string' ? attachment.token : null);
                const filePath = typeof attachment.filePath === 'string' ? attachment.filePath : null;

                if (!attachmentId || !fileName || !downloadToken || !filePath) {
                    logWarning('Attachment payload missing required fields for message', { attachment });
                    return list;
                }

                const isTokenValid = verifyAttachmentToken(sessionId, filePath, downloadToken);
                const normalizedToken = buildAttachmentTokenWithSecret(PRIMARY_ATTACHMENT_SECRET, sessionId, filePath);

                if (!isTokenValid) {
                    logWarning('Attachment download token mismatch (message)', {
                        sessionId,
                        attachmentId,
                    });
                }

                list.push({
                    id: attachmentId,
                    attachmentId,
                    fileName,
                    fileSize: Number(attachment.fileSize || attachment.file_size || 0),
                    mimeType: attachment.mimeType || attachment.mime_type || 'application/octet-stream',
                    downloadToken: normalizedToken,
                    token: normalizedToken,
                    filePath,
                    metadata: attachment.metadata && typeof attachment.metadata === 'object' ? attachment.metadata : {},
                });

                return list;
            }, []);

            const hasText = rawContent.trim().length > 0;

            if (!hasText && attachments.length === 0) {
                return;
            }

            const timestamp = new Date().toISOString();
            const clientMessageId = uuidv4();

            const fallbackContent = hasText ? rawContent : (attachments[0]?.fileName || '(Lampiran)');

            const message = {
                id: clientMessageId,
                clientMessageId,
                messageUuid: clientMessageId,
                messageId: clientMessageId,
                sessionId,
                sessionUuid,
                senderType: role === 'agent' ? 'admin' : 'customer',
                senderId: role === 'agent' ? actorId : null,
                messageType: hasText ? 'text' : 'attachment',
                content: fallbackContent,
                createdAt: timestamp,
            };

            message.attachments = attachments;

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
            refreshSessionExpiry(sessionId);
            break;
        }
        case 'attachment': {
            const attachment = data.attachment;
            if (!attachment || typeof attachment !== 'object') {
                return;
            }

            const attachmentId = Number(attachment.attachmentId || attachment.id || 0);
            const fileName = typeof attachment.fileName === 'string' ? attachment.fileName : null;
            const fileSize = Number(attachment.fileSize || 0);
            const mimeType = typeof attachment.mimeType === 'string' ? attachment.mimeType : 'application/octet-stream';
            const downloadToken = typeof attachment.downloadToken === 'string' ? attachment.downloadToken : null;
            const filePath = typeof attachment.filePath === 'string' ? attachment.filePath : null;
            const metadata = attachment.metadata && typeof attachment.metadata === 'object' ? attachment.metadata : {};

            if (!attachmentId || !fileName || !downloadToken || !filePath) {
                logWarning('Attachment payload missing required fields', { payload: attachment });
                return;
            }

            const isTokenValid = verifyAttachmentToken(sessionId, filePath, downloadToken);
            const normalizedToken = buildAttachmentTokenWithSecret(PRIMARY_ATTACHMENT_SECRET, sessionId, filePath);

            if (!isTokenValid) {
                logWarning('Attachment download token mismatch', {
                    sessionId,
                    attachmentId,
                });
            }

            const timestamp = new Date().toISOString();
            const clientMessageId = uuidv4();
            const message = {
                id: clientMessageId,
                clientMessageId,
                messageUuid: clientMessageId,
                messageId: clientMessageId,
                sessionId,
                sessionUuid,
                senderType: role === 'agent' ? 'admin' : 'customer',
                senderId: role === 'agent' ? actorId : null,
                messageType: 'attachment',
                content: fileName,
                createdAt: timestamp,
                attachment: {
                    id: attachmentId,
                    fileName,
                    fileSize,
                    mimeType,
                    downloadToken: normalizedToken,
                    token: normalizedToken,
                    filePath,
                    metadata,
                },
            };

            message.attachments = [message.attachment];

            queueForPersistence({
                action: 'message',
                payload: message,
            });

            const outbound = {
                type: 'callcenter:event',
                payload: {
                    kind: 'attachment',
                    message,
                },
            };

            broadcastToSession(sessionId, outbound);
            publishRealtime(sessionId, outbound.payload);
            refreshSessionExpiry(sessionId);
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

    if (meta.monitor) {
        monitorConnections.delete(ws);
        connectionMeta.delete(ws);
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

        if (payload.kind === 'message_persisted') {
            if (entry) {
                broadcastToSession(sessionId, {
                    type: 'callcenter:event',
                    payload,
                });
            }

            emitMonitorEvent('message_persisted', {
                sessionId,
                messageId: payload.messageId || null,
            });

            return;
        }

        if (payload.kind === 'session_created') {
            emitMonitorEvent('session_created', {
                sessionId,
                status: payload.status || 'pending',
            });
        } else if (payload.kind === 'session_claimed') {
            emitMonitorEvent('session_claimed', {
                sessionId,
                actor: 'agent',
                userId: payload.actorId || null,
                assignedAdminId: payload.assignedAdminId || null,
            });
        } else if (payload.kind === 'session_closed') {
            emitMonitorEvent('session_closed', {
                sessionId,
                reason: payload.reason || null,
                source: payload.sourceNode || null,
            });
        } else if (payload.kind === 'upload_permission') {
            emitMonitorEvent('upload_permission', {
                sessionId,
                allow: !!payload.allow,
            });
        }

        if (!entry) {
            return;
        }

        const outbound = {
            type: 'callcenter:event',
            payload,
        };

        broadcastToSession(sessionId, outbound);

        if (payload.kind === 'session_closed') {
            clearSessionExpiry(sessionId);
            [...entry.agents, ...entry.customers].forEach((client) => {
                try { client.close(1000, 'Session closed'); } catch (_) {}
            });
            sessionConnections.delete(sessionId);
        } else if (payload.kind === 'upload_permission') {
            broadcastToSession(sessionId, {
                type: 'callcenter:event',
                payload,
            });
        }
    });
}

subscribeRealtimeChannel();
initExpiryMonitor();

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

    refreshSessionExpiry(sessionId);

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

    if (role === 'customer') {
        emitMonitorEvent('session_connected', {
            sessionId,
            sessionUuid,
            actor: 'customer',
            userId,
        });
    } else if (role === 'agent') {
        emitMonitorEvent('session_claimed', {
            sessionId,
            actor: 'agent',
            userId,
        });
    }
}

function handleCallCenterAdminBroadcast(ws, user) {
    const callCenterData = user.callCenter;
    if (!callCenterData || callCenterData.role !== 'admin_monitor') {
        logWarning('Unauthorized monitor connection attempt', { userId: user.userId });
        try { ws.close(4403, 'Unauthorized'); } catch (_) {}
        return;
    }

    monitorConnections.add(ws);
    connectionMeta.set(ws, { monitor: true, userId: user.userId });

    ws.on('close', () => cleanupConnection(ws));
    ws.on('error', (error) => logWarning('Call center monitor websocket error', { error: error.message }));

    logInfo('Call center monitor connection established', {
        userId: user.userId,
    });

    try {
        ws.send(JSON.stringify({
            type: 'callcenter:dashboard',
            payload: {
                event: 'monitor_connected',
                emittedAt: new Date().toISOString(),
                userId: user.userId,
            },
        }));
    } catch (error) {
        logWarning('Failed sending monitor acknowledgement', { error: error.message });
    }
}

module.exports = {
    handleCallCenter,
    handleCallCenterAdminBroadcast,
};
