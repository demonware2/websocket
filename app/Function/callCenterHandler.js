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
const historyRedis = new Redis({ ...redisConfig, db: 4 });

const HISTORY_CACHE_TTL = 10800; // 3 jam

async function appendHistoryCache(sessionId, message) {
    try {
        const key = `chat:history:${sessionId}`;
        const bufferKey = `chat:buffer:${sessionId}`;
        await historyRedis.rpush(key, JSON.stringify(message));
        await historyRedis.expire(key, HISTORY_CACHE_TTL);
        await historyRedis.rpush(bufferKey, JSON.stringify(message));
        await historyRedis.expire(bufferKey, 300);
    } catch (e) {
        logError('Failed to append history cache', { sessionId, error: e.message });
    }
}

async function markHistoryAsReadCache(sessionId, readerType, messageId) {
    try {
        const key = `chat:history:${sessionId}`;
        const bufferKey = `chat:buffer:${sessionId}`;
        
        const cacheKeys = [key, bufferKey];
        for (const k of cacheKeys) {
            const items = await historyRedis.lrange(k, 0, -1);
            if (!items.length) continue;

            let found = false;
            const updated = items.map(raw => {
                const m = JSON.parse(raw);
                const senderType = m.senderType || m.sender_type;
                const isMatch = (m.id == messageId || m.clientMessageId == messageId);
                const isPrev = (!isNaN(m.id) && !isNaN(messageId) && Number(m.id) <= Number(messageId));

                if (senderType !== readerType && (isMatch || isPrev || messageId === 'all')) {
                    m.status = 'read'; found = true;
                }
                return JSON.stringify(m);
            });

            if (found) {
                await historyRedis.del(k);
                await historyRedis.rpush(k, ...updated);
                await historyRedis.expire(k, k === bufferKey ? 300 : HISTORY_CACHE_TTL);
            }
        }
    } catch (e) {
        logError('Failed to update history read cache', { sessionId, error: e.message });
    }
}

const EXPIRY_DB = Number(process.env.CALL_CENTER_EXPIRY_DB || 3);
const SESSION_INACTIVITY_TTL = Number(process.env.CALL_CENTER_INACTIVE_TIMEOUT_SECONDS || 900);
const EXPIRY_KEY_PREFIX = process.env.CALL_CENTER_EXPIRY_KEY_PREFIX || 'callcenter:session:expiry:';

const expiryRedis = SESSION_INACTIVITY_TTL > 0 ? new Redis({ ...redisConfig, db: EXPIRY_DB }) : null;
const expirySubscriber = SESSION_INACTIVITY_TTL > 0 ? new Redis({ ...redisConfig, db: EXPIRY_DB }) : null;

const PRIMARY_ATTACHMENT_SECRET = process.env.LIVECHAT_ATTACHMENT_SECRET || process.env.ENCRYPTION_KEY || 'livechat-secret';
const ATTACHMENT_SECRETS = [PRIMARY_ATTACHMENT_SECRET, process.env.ENCRYPTION_KEY].filter((v, i, a) => v && a.indexOf(v) === i);

function buildAttachmentTokenWithSecret(secret, sessionId, filePath) {
    return crypto.createHmac('sha256', secret || 'livechat-secret').update(`${sessionId}|${filePath}`).digest('hex');
}

function verifyAttachmentToken(sessionId, filePath, token) {
    if (!token || !filePath) return false;
    return ATTACHMENT_SECRETS.some(s => {
        try { return buildAttachmentTokenWithSecret(s, sessionId, filePath) === token; } catch (e) { return false; }
    });
}

if (expiryRedis) expiryRedis.on('error', e => logWarning('Expiry redis error', { error: e.message }));
if (expirySubscriber) expirySubscriber.on('error', e => logWarning('Expiry sub error', { error: e.message }));

const NODE_ID = `${os.hostname()}-${process.pid}`;
const sessionConnections = new Map();
const connectionMeta = new Map();
const monitorConnections = new Set();
const INACTIVITY_TTL_SECONDS = SESSION_INACTIVITY_TTL > 0 ? Math.max(SESSION_INACTIVITY_TTL, 30) : 0;

function refreshSessionExpiry(sessionId, isPersistent = false) {
    if (!expiryRedis || !sessionId || INACTIVITY_TTL_SECONDS <= 0 || isPersistent) return;
    expiryRedis.set(`${EXPIRY_KEY_PREFIX}${sessionId}`, Date.now().toString(), 'EX', INACTIVITY_TTL_SECONDS).catch(e => {});
}

function clearSessionExpiry(sessionId) {
    if (!expiryRedis || !sessionId) return;
    expiryRedis.del(`${EXPIRY_KEY_PREFIX}${sessionId}`).catch(e => {});
}

async function ensureExpiryNotificationSupport() {
    if (!expiryRedis || INACTIVITY_TTL_SECONDS <= 0) return;
    try {
        const res = await expiryRedis.config('GET', 'notify-keyspace-events');
        let val = (Array.isArray(res) ? res[1] : '') || '';
        if (!val.includes('E')) val += 'E';
        if (!val.includes('x')) val += 'x';
        await expiryRedis.config('SET', 'notify-keyspace-events', val);
    } catch (e) {}
}

function subscribeExpiryEvents() {
    if (!expirySubscriber || INACTIVITY_TTL_SECONDS <= 0) return;
    const channel = `__keyevent@${EXPIRY_DB}__:expired`;
    expirySubscriber.subscribe(channel);
    expirySubscriber.on('message', (ch, key) => {
        if (!key.startsWith(EXPIRY_KEY_PREFIX)) return;
        const sid = Number(key.slice(EXPIRY_KEY_PREFIX.length));
        if (sid > 0) queueForPersistence({ action: 'expire', payload: { sessionId: sid, reason: 'inactivity' } });
    });
}

initExpiryMonitor();
function initExpiryMonitor() { if (expiryRedis && expirySubscriber) ensureExpiryNotificationSupport().finally(() => subscribeExpiryEvents()); }

function ensureSessionEntry(sid) { if (!sessionConnections.has(sid)) sessionConnections.set(sid, { agents: new Set(), customers: new Set() }); return sessionConnections.get(sid); }

function broadcastToSession(sid, payload, skip = null) {
    const entry = sessionConnections.get(sid);
    if (!entry) return;
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    [...entry.agents, ...entry.customers].forEach(c => { if (c && c !== skip && c.readyState === WebSocket.OPEN) c.send(data); });
}

function broadcastToMonitors(payload) {
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    monitorConnections.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
}

function emitMonitorEvent(event, details = {}) { broadcastToMonitors({ type: 'callcenter:dashboard', payload: { event, emittedAt: new Date().toISOString(), ...details } }); }

async function queueForPersistence(msg) { try { await persistenceRedis.rpush('callcenter:persistence', JSON.stringify(msg)); } catch (e) {} }

function publishRealtime(sid, payload) { try { realtimePublisher.publish(`callcenter:session:${sid}`, JSON.stringify({ ...payload, sessionId: sid, sourceNode: NODE_ID })); } catch (e) {} }

function handleInboundMessage(ws, meta, raw) {
    let data; try { data = JSON.parse(raw); } catch (e) { return; }
    const { sessionId, role, sessionUuid, actorId, senderName, isPersistent } = meta;

    switch (data.type) {
        case 'ping': ws.send(JSON.stringify({ type: 'pong', at: Date.now() })); refreshSessionExpiry(sessionId, isPersistent); break;
        case 'typing':
            publishRealtime(sessionId, { kind: 'typing', actor: role, sessionUuid });
            broadcastToSession(sessionId, { type: 'typing', senderType: role === 'agent' ? 'admin' : 'customer' }, ws);
            break;
        case 'read_receipt':
            broadcastToSession(sessionId, { type: 'read_receipt', messageId: data.messageId, senderType: role === 'agent' ? 'admin' : 'customer' }, ws);
            markHistoryAsReadCache(sessionId, role === 'agent' ? 'admin' : 'customer', data.messageId);
            queueForPersistence({ action: 'read', payload: { sessionId, messageId: data.messageId, readerType: role === 'agent' ? 'admin' : 'customer', readAt: new Date().toISOString() } });
            break;
        case 'message': {
            const hasText = (data.content || '').trim().length > 0;
            const atts = Array.isArray(data.attachments) ? data.attachments : [];
            if (!hasText && atts.length === 0) return;
            const message = {
                id: uuidv4(), clientMessageId: uuidv4(), sessionId, sessionUuid, senderType: role === 'agent' ? 'admin' : 'customer',
                senderId: role === "agent" ? actorId : null, senderName: role === "agent" ? senderName : null,
                messageType: hasText ? 'text' : 'attachment', content: data.content || atts[0]?.fileName,
                createdAt: new Date().toISOString(), attachments: atts, status: 'sent'
            };
            appendHistoryCache(sessionId, message);
            queueForPersistence({ action: 'message', payload: message });
            const out = { type: 'callcenter:event', payload: { kind: 'message', message } };
            broadcastToSession(sessionId, out);
            publishRealtime(sessionId, out.payload);
            refreshSessionExpiry(sessionId, isPersistent);
            break;
        }
        case 'attachment': {
            const a = data.attachment; if (!a) return;
            const message = {
                id: uuidv4(), clientMessageId: uuidv4(), sessionId, sessionUuid, senderType: role === 'agent' ? 'admin' : 'customer',
                senderId: role === "agent" ? actorId : null, senderName: role === "agent" ? senderName : null,
                messageType: 'attachment', content: a.fileName, createdAt: new Date().toISOString(), attachments: [a], status: 'sent'
            };
            appendHistoryCache(sessionId, message);
            queueForPersistence({ action: 'message', payload: message });
            const out = { type: 'callcenter:event', payload: { kind: 'attachment', message } };
            broadcastToSession(sessionId, out);
            publishRealtime(sessionId, out.payload);
            refreshSessionExpiry(sessionId, isPersistent);
            break;
        }
        case "transfer":
            if (role !== "agent") return;
            queueForPersistence({ action: "transfer", payload: { sessionId, targetAdminId: data.targetAdminId } });
            const tOut = { type: "callcenter:event", payload: { kind: "session_transferred", sessionId, newAdminId: data.targetAdminId } };
            broadcastToSession(sessionId, tOut); publishRealtime(sessionId, tOut.payload);
            break;
        case "hold":
            if (role !== "agent") return;
            queueForPersistence({ action: "hold", payload: { sessionId } });
            const hOut = { type: "callcenter:event", payload: { kind: "session_on_hold", sessionId, status: "on_hold" } };
            broadcastToSession(sessionId, hOut); publishRealtime(sessionId, hOut.payload);
            break;
        case "resume":
            if (role !== "agent") return;
            queueForPersistence({ action: "resume", payload: { sessionId } });
            const rOut = { type: "callcenter:event", payload: { kind: "session_resumed", sessionId, status: "open" } };
            broadcastToSession(sessionId, rOut); publishRealtime(sessionId, rOut.payload);
            break;
    }
}

function cleanupConnection(ws) {
    const meta = connectionMeta.get(ws); if (!meta) return;
    if (meta.monitor) monitorConnections.delete(ws);
    else {
        const entry = sessionConnections.get(meta.sessionId);
        if (entry) { entry.agents.delete(ws); entry.customers.delete(ws); if (entry.agents.size === 0 && entry.customers.size === 0) sessionConnections.delete(meta.sessionId); }
    }
    connectionMeta.delete(ws);
}

function subscribeRealtimeChannel() {
    realtimeSubscriber.psubscribe('callcenter:session:*');
    realtimeSubscriber.on('pmessage', (_p, _ch, msg) => {
        let payload; try { payload = JSON.parse(msg); } catch (e) { return; }
        if (payload.sourceNode === NODE_ID) return;
        const sid = payload.sessionId; if (!sid) return;
        const entry = sessionConnections.get(sid);
        
        if (payload.kind === 'messages_read' && entry) { 
            broadcastToSession(sid, { type: 'read_receipt', readerType: payload.readerType, all: !!payload.all }); 
            return; 
        }

        if (payload.kind === 'message_persisted') { if (entry) broadcastToSession(sid, { type: 'callcenter:event', payload }); emitMonitorEvent('message_persisted', { sessionId: sid }); return; }
        if (payload.kind === 'session_created') emitMonitorEvent('session_created', { sessionId: sid });
        else if (payload.kind === 'session_claimed') emitMonitorEvent('session_claimed', { sessionId: sid });
        else if (payload.kind === 'session_closed') {
            emitMonitorEvent('session_closed', { sessionId: sid });
            if (entry) { [...entry.agents, ...entry.customers].forEach(c => { try { c.close(1000); } catch (e) {} }); sessionConnections.delete(sid); }
        }
        if (entry) broadcastToSession(sid, { type: 'callcenter:event', payload });
    });
}

subscribeRealtimeChannel();
function handleCallCenter(ws, user) {
    const cd = user.callCenter; if (!cd || !cd.sessionId) return;
    const entry = ensureSessionEntry(cd.sessionId);
    if (cd.role === 'agent') entry.agents.add(ws); else entry.customers.add(ws);
    connectionMeta.set(ws, { sessionId: cd.sessionId, role: cd.role, sessionUuid: cd.sessionUuid, actorId: user.actorId, senderName: cd.senderName, isPersistent: !!cd.isPersistent });
    refreshSessionExpiry(cd.sessionId, !!cd.isPersistent);
    ws.on('message', r => handleInboundMessage(ws, connectionMeta.get(ws), r));
    ws.on('close', () => cleanupConnection(ws));
    ws.send(JSON.stringify({ type: 'callcenter:event', payload: { kind: 'connected', sessionId: cd.sessionId, role: cd.role } }));
    if (cd.role === 'customer') emitMonitorEvent('session_connected', { sessionId: cd.sessionId });
}

function handleCallCenterAdminBroadcast(ws, user) {
    if (user.callCenter?.role !== 'admin_monitor') return;
    monitorConnections.add(ws); connectionMeta.set(ws, { monitor: true });
    ws.on('close', () => cleanupConnection(ws));
}

module.exports = { handleCallCenter, handleCallCenterAdminBroadcast };
