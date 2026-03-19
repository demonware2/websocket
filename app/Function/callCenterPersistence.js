const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const CALL_CENTER_TIMEZONE = process.env.CALL_CENTER_TIMEZONE || process.env.TZ || 'Asia/Jakarta';

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
};

const queueRedis = new Redis({ ...redisConfig, db: 2 });
const historyRedis = new Redis({ ...redisConfig, db: 4 }); // DB 4 untuk cache history & buffer
const settingsRedis = new Redis(redisConfig);

const DEFAULT_SETTINGS = {
    enabled: true,
    timezone: CALL_CENTER_TIMEZONE,
    schedule: [],
};

const EXPIRY_KEY_PREFIX = process.env.CALL_CENTER_EXPIRY_KEY_PREFIX || 'callcenter:session:expiry:';

function normalizeTimestamp(value) {
    if (!value) return new Date().toISOString().slice(0, 19).replace('T', ' ');
    try {
        const d = new Date(value);
        return d.toISOString().slice(0, 19).replace('T', ' ');
    } catch (e) {
        return new Date().toISOString().slice(0, 19).replace('T', ' ');
    }
}

async function persistMessage(pool, publisher, message) {
    const createdAt = normalizeTimestamp(message.createdAt);

    let currentStatus = message.status || 'sent';
    try {
        const cachedHistory = await historyRedis.lrange(`chat:history:${message.sessionId}`, -10, -1);
        for (const raw of cachedHistory) {
            const m = JSON.parse(raw);
            if (m.clientMessageId === message.clientMessageId && m.status === 'read') {
                currentStatus = 'read';
                break;
            }
        }
    } catch (e) {}

    const attachmentsInput = Array.isArray(message.attachments) ? message.attachments : [];
    let primaryAttachmentId = attachmentsInput.length > 0 ? Number(attachmentsInput[0].id || attachmentsInput[0].attachmentId || 0) : null;

    const connection = await pool.getConnection();
    try {
        const [insertResult] = await connection.execute(
            `INSERT INTO call_center_messages (session_id, sender_type, sender_id, message_type, status, content, metadata, created_at, updated_at, attachment_id)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM call_center_sessions WHERE id = ?`,
            [
                message.sessionId, message.senderType, message.senderId, message.messageType,
                currentStatus, message.content, message.metadata ? JSON.stringify(message.metadata) : null,
                createdAt, createdAt, primaryAttachmentId, message.sessionId,
            ],
        );

        if (insertResult.affectedRows === 0) return;

        await publisher.publish(`callcenter:session:${message.sessionId}`, JSON.stringify({
            kind: 'message_persisted', sessionId: message.sessionId, messageUuid: message.clientMessageId,
            messageId: insertResult.insertId, id: insertResult.insertId, sourceNode: 'callcenter:persistence'
        }));

        if (attachmentsInput.length) {
            for (const att of attachmentsInput) {
                const aid = Number(att.id || att.attachmentId || 0);
                if (aid > 0) {
                    await connection.execute('UPDATE call_center_attachments SET message_id = ?, updated_at = ? WHERE id = ?', [insertResult.insertId, createdAt, aid]);
                }
            }
        }
    } finally {
        connection.release();
    }
}

async function markMessagesAsRead(pool, payload) {
    const connection = await pool.getConnection();
    try {
        const readAt = normalizeTimestamp(payload.readAt);
        const msgId = Number(payload.messageId);
        if (!isNaN(msgId)) {
            await connection.execute(
                "UPDATE call_center_messages SET status = 'read', updated_at = ? WHERE session_id = ? AND sender_type != ? AND id <= ? AND status != 'read'",
                [readAt, payload.sessionId, payload.readerType, msgId]
            );
        }
    } finally {
        connection.release();
    }
}

async function closeSessionDueToInactivity(pool, sessionId) {
    const connection = await pool.getConnection();
    try {
        const [rows] = await connection.execute('SELECT id, status, is_persistent FROM call_center_sessions WHERE id = ?', [sessionId]);
        if (!rows.length || rows[0].status === 'closed' || rows[0].is_persistent) return { status: 'ignored' };
        const now = normalizeTimestamp(new Date());
        await connection.execute('UPDATE call_center_sessions SET status = "closed", closed_at = ?, updated_at = ? WHERE id = ?', [now, now, sessionId]);
        return { status: 'closed' };
    } finally {
        connection.release();
    }
}

function startCallCenterPersistence() {
    const mysqlPool = mysql.createPool({
        host: process.env.DATABASE_HOST, user: process.env.DATABASE_USER, password: process.env.DATABASE_PASSWORD,
        database: 'siroum_call', waitForConnections: true, connectionLimit: 10,
    });

    const eventPublisher = new Redis(redisConfig);
    let running = true;

    async function processSync() {
        const allItems = []; let raw;
        while (raw = await queueRedis.lpop('callcenter:persistence')) { try { allItems.push(JSON.parse(raw)); } catch (e) {} }
        if (!allItems.length) return;

        const messages = allItems.filter(i => i.action === 'message').map(i => i.payload);
        const readReceipts = allItems.filter(i => i.action === 'read').map(i => i.payload);
        const expirations = allItems.filter(i => i.action === 'expire');
        const others = allItems.filter(i => ['transfer', 'hold', 'resume'].includes(i.action));

        const sessionIdsToClear = new Set();
        allItems.forEach(i => { if (i.payload?.sessionId) sessionIdsToClear.add(Number(i.payload.sessionId)); });

        if (messages.length) { for (const m of messages) await persistMessage(mysqlPool, eventPublisher, m); }
        if (readReceipts.length) { for (const r of readReceipts) await markMessagesAsRead(mysqlPool, r); }
        
        for (const exp of expirations) {
            const sid = Number(exp.payload?.sessionId); if (!sid) continue;
            const res = await closeSessionDueToInactivity(mysqlPool, sid);
            if (res.status === 'closed') await eventPublisher.publish(`callcenter:session:${sid}`, JSON.stringify({ kind: 'session_closed', sessionId: sid, reason: 'inactivity' }));
        }

        for (const act of others) {
            const sid = Number(act.payload?.sessionId); if (!sid) continue;
            const connection = await mysqlPool.getConnection();
            try {
                if (act.action === 'transfer') await connection.execute('UPDATE call_center_sessions SET assigned_admin_id = ?, updated_at = ? WHERE id = ?', [act.payload.targetAdminId, normalizeTimestamp(new Date()), sid]);
                else if (act.action === 'hold') await connection.execute('UPDATE call_center_sessions SET status = "on_hold", updated_at = ? WHERE id = ?', [normalizeTimestamp(new Date()), sid]);
                else if (act.action === 'resume') await connection.execute('UPDATE call_center_sessions SET status = "open", updated_at = ? WHERE id = ?', [normalizeTimestamp(new Date()), sid]);
            } finally { connection.release(); }
        }

        for (const sid of sessionIdsToClear) {
            await historyRedis.del(`chat:buffer:${sid}`);
            logInfo(`Buffer cleared for session #${sid} after bulk sync`);
        }
    }

    async function processLoop() {
        logInfo('Call center bulk worker started');
        while (running) {
            try {
                const trigger = await queueRedis.blpop('callcenter:persistence', 0);
                if (trigger) {
                    await queueRedis.lpush('callcenter:persistence', trigger[1]);
                    await new Promise(r => setTimeout(r, 30000));
                    await processSync();
                }
            } catch (e) { await new Promise(r => setTimeout(r, 5000)); }
        }
    }
    processLoop().catch(e => logError('Fatal worker error', { error: e.message }));
    return { stop() { running = false; } };
}

module.exports = { startCallCenterPersistence };
