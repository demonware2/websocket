const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
};

const queueRedis = new Redis({ ...redisConfig, db: 2 });
const historyRedis = new Redis({ ...redisConfig, db: 4 });

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

        if (insertResult.affectedRows === 0) {
            return { success: false, error: 'Session not found in DB' };
        }

        console.log(`>>> [WORKER] SUCCESS: Message saved (ID: ${insertResult.insertId}) for Session #${message.sessionId}`);

        await publisher.publish(`callcenter:session:${message.sessionId}`, JSON.stringify({
            kind: 'message_persisted', 
            sessionId: message.sessionId, 
            messageUuid: message.clientMessageId,
            messageId: insertResult.insertId, 
            id: insertResult.insertId, 
            sourceNode: 'callcenter:persistence'
        }));

        if (attachmentsInput.length) {
            for (const att of attachmentsInput) {
                const aid = Number(att.id || att.attachmentId || 0);
                if (aid > 0) {
                    await connection.execute('UPDATE call_center_attachments SET message_id = ?, updated_at = ? WHERE id = ?', [insertResult.insertId, createdAt, aid]);
                }
            }
        }
        return { success: true };
    } catch (err) {
        return { success: false, error: err.message };
    } finally {
        connection.release();
    }
}

function startCallCenterPersistence() {
    const mysqlPool = mysql.createPool({
        host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME_CALL || 'siroum_call', waitForConnections: true, connectionLimit: 10,
    });

    const eventPublisher = new Redis(redisConfig);
    let running = true;

    async function processSync() {
        const allItems = []; let raw;
        while (raw = await queueRedis.lpop('callcenter:persistence')) { try { allItems.push(JSON.parse(raw)); } catch (e) {} }
        if (!allItems.length) return;

        const sessionIdsToClear = new Set();

        for (const item of allItems) {
            const { action, payload } = item;
            if (!payload) continue;
            if (payload.sessionId) sessionIdsToClear.add(Number(payload.sessionId));

            try {
                if (action === 'message') {
                    const result = await persistMessage(mysqlPool, eventPublisher, payload);
                    if (!result.success) {
                        const retryCount = (item.retryCount || 0) + 1;
                        if (retryCount <= 3) {
                            console.log(`>>> [WORKER] BOUNCE: Retrying in 10s (${retryCount}/3) for Session #${payload.sessionId}`);
                            setTimeout(async () => {
                                await queueRedis.rpush('callcenter:persistence', JSON.stringify({ ...item, retryCount }));
                            }, 10000);
                        } else {
                            console.error(`>>> [WORKER] FATAL: Max retries reached. Moving to permanent error queue.`);
                            await queueRedis.rpush('callcenter:persistence:error', JSON.stringify({ ...item, lastError: result.error, failedAt: new Date().toISOString() }));
                        }
                    }
                } else {
                    const connection = await mysqlPool.getConnection();
                    try {
                        if (action === 'read') {
                            const readAt = normalizeTimestamp(payload.readAt);
                            if (payload.readerType === 'customer') {
                                await connection.execute("UPDATE call_center_messages SET status = 'read', updated_at = ? WHERE session_id = ? AND sender_type = 'admin' AND id <= ? AND status != 'read'", [readAt, payload.sessionId, Number(payload.messageId)]);
                            } else if (payload.readerType === 'admin') {
                                await connection.execute("UPDATE call_center_messages SET status = 'read', updated_at = ? WHERE session_id = ? AND sender_type = 'customer' AND id <= ? AND status != 'read'", [readAt, payload.sessionId, Number(payload.messageId)]);
                                if (payload.readerId) {
                                    await connection.execute(`
                                        UPDATE call_center_messages 
                                        SET metadata = JSON_ARRAY_APPEND(IFNULL(metadata, '{"read_by":[]}'), '$.read_by', ?) 
                                        WHERE session_id = ? AND sender_type = 'admin' AND sender_id != ? AND id <= ? 
                                          AND (metadata IS NULL OR JSON_SEARCH(IFNULL(metadata, '{"read_by":[]}'), 'one', ?, null, '$.read_by') IS NULL)
                                    `, [payload.readerId, payload.sessionId, payload.readerId, Number(payload.messageId), payload.readerId]);
                                }
                            }
                        } else if (action === 'transfer') {
                            await connection.execute('UPDATE call_center_sessions SET assigned_admin_id = ?, updated_at = ? WHERE id = ?', [payload.targetAdminId, normalizeTimestamp(new Date()), payload.sessionId]);
                        } else if (action === 'hold') {
                            await connection.execute('UPDATE call_center_sessions SET status = "on_hold", updated_at = ? WHERE id = ?', [normalizeTimestamp(new Date()), payload.sessionId]);
                        } else if (action === 'resume') {
                            await connection.execute('UPDATE call_center_sessions SET status = "open", updated_at = ? WHERE id = ?', [normalizeTimestamp(new Date()), payload.sessionId]);
                        }
                    } finally { connection.release(); }
                }
            } catch (err) {
                console.error(`>>> [WORKER] Action ${action} failed: ${err.message}`);
            }
        }

        for (const sid of sessionIdsToClear) {
            await historyRedis.del(`chat:buffer:${sid}`);
        }
    }

    async function processLoop() {
        console.log('>>> [WORKER] Call center persistence worker STARTED');
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