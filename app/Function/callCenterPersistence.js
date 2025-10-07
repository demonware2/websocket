const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const { logInfo, logError, logWarning } = require('../Helper/errorHandler');

const DEFAULT_SETTINGS = {
    enabled: true,
    timezone: process.env.CALL_CENTER_TIMEZONE || 'Asia/Jakarta',
    schedule: [],
};

const QUEUE_LENGTH_SAMPLE_INTERVAL = Math.max(Number(process.env.CALL_CENTER_QUEUE_SAMPLE_INTERVAL || 20), 1);

const EXPIRY_KEY_PREFIX = process.env.CALL_CENTER_EXPIRY_KEY_PREFIX || 'callcenter:session:expiry:';
const EXPIRY_DB = Number(process.env.CALL_CENTER_EXPIRY_DB || 3);
const SESSION_INACTIVITY_TTL = Number(process.env.CALL_CENTER_INACTIVE_TIMEOUT_SECONDS || 900);

const DAY_INDEX = {
    sun: 0,
    mon: 1,
    tue: 2,
    wed: 3,
    thu: 4,
    fri: 5,
    sat: 6,
};

function timeToMinutes(value) {
    if (!value || typeof value !== 'string') {
        return 0;
    }
    const [hour, minute] = value.split(':').map((part) => parseInt(part, 10));
    if (Number.isNaN(hour) || Number.isNaN(minute)) {
        return 0;
    }
    return hour * 60 + minute;
}

function normaliseSchedule(schedule = []) {
    return schedule
        .map((entry) => {
            const days = Array.isArray(entry.days)
                ? entry.days.map((day) => (typeof day === 'string' ? DAY_INDEX[day.slice(0, 3).toLowerCase()] : Number(day))).filter((day) => day >= 0 && day <= 6)
                : [];

            const start = timeToMinutes(entry.start || '00:00');
            const end = timeToMinutes(entry.end || '23:59');

            if (days.length === 0) {
                return null;
            }

            return {
                days,
                start,
                end,
            };
        })
        .filter(Boolean);
}

function isWithinSchedule(settings) {
    if (!settings.enabled) {
        return false;
    }

    if (!settings.schedule || settings.schedule.length === 0) {
        return true;
    }

    const timezone = settings.timezone || 'UTC';

    const formatter = new Intl.DateTimeFormat('en-GB', {
        timeZone: timezone,
        weekday: 'short',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });

    const now = new Date();
    const parts = formatter.formatToParts(now);

    const weekdayPart = parts.find((part) => part.type === 'weekday');
    const hourPart = parts.find((part) => part.type === 'hour');
    const minutePart = parts.find((part) => part.type === 'minute');

    if (!weekdayPart || !hourPart || !minutePart) {
        return true;
    }

    const day = DAY_INDEX[weekdayPart.value.slice(0, 3).toLowerCase()];
    const minutesOfDay = parseInt(hourPart.value, 10) * 60 + parseInt(minutePart.value, 10);

    return settings.schedule.some((slot) => {
        if (!slot.days.includes(day)) {
            return false;
        }

        if (slot.start <= slot.end) {
            return minutesOfDay >= slot.start && minutesOfDay < slot.end;
        }

        // Overnight schedule
        return minutesOfDay >= slot.start || minutesOfDay < slot.end;
    });
}

function createMysqlPool() {
    return mysql.createPool({
        host: process.env.DB_HOST || 'localhost',
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD || '',
        database: process.env.DB_NAME_CALL || 'siroum_call',
        charset: 'utf8mb4',
        waitForConnections: true,
        connectionLimit: Number(process.env.CALL_CENTER_DB_POOL || 4),
        queueLimit: 0,
    });
}

function normalizeTimestamp(value) {
    const raw = value && String(value).trim() !== '' ? value : null;
    const date = raw ? new Date(raw) : new Date();

    if (Number.isNaN(date.getTime())) {
        return new Date().toISOString().slice(0, 19).replace('T', ' ');
    }

    return date.toISOString().slice(0, 19).replace('T', ' ');
}

async function persistMessage(pool, message) {
    const createdAt = normalizeTimestamp(message.createdAt);

    const connection = await pool.getConnection();
    try {
        const [insertResult] = await connection.execute(
            `INSERT INTO call_center_messages (session_id, sender_type, sender_id, message_type, content, metadata, created_at, updated_at, attachment_id)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? FROM call_center_sessions WHERE id = ?`,
            [
                message.sessionId,
                message.senderType,
                message.senderId,
                message.messageType || 'text',
                message.content,
                message.metadata ? JSON.stringify(message.metadata) : null,
                createdAt,
                createdAt,
                message.attachment ? message.attachment.id : null,
                message.sessionId,
            ],
        );

        if (insertResult.affectedRows === 0) {
            logWarning('Call center persistence: unknown session id', { sessionId: message.sessionId });
            return;
        }

        if (message.attachment) {
            await connection.execute(
                'INSERT INTO call_center_attachments (session_id, message_id, original_name, stored_name, mime_type, file_path, file_size, metadata, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [
                    message.sessionId,
                    insertResult.insertId,
                    message.attachment.original_name,
                    message.attachment.stored_name,
                    message.attachment.mime_type,
                    message.attachment.file_path,
                    message.attachment.file_size,
                    message.attachment.metadata ? JSON.stringify(message.attachment.metadata) : null,
                    normalizeTimestamp(message.attachment.created_at || createdAt),
                    normalizeTimestamp(message.attachment.updated_at || createdAt),
                ],
            );
        }

        await connection.execute(
            'UPDATE call_center_sessions SET last_message_at = ?, status = IF(status = "pending", "open", status), updated_at = ? WHERE id = ?',
            [createdAt, createdAt, message.sessionId],
        );
    } finally {
        connection.release();
    }
}

async function closeSessionDueToInactivity(pool, sessionId) {
    const connection = await pool.getConnection();
    const queueSessionIds = [];

    try {
        await connection.beginTransaction();

        const [sessions] = await connection.execute(
            'SELECT id, status FROM call_center_sessions WHERE id = ? FOR UPDATE',
            [sessionId],
        );

        if (!sessions || sessions.length === 0) {
            await connection.rollback();
            return { status: 'not_found' };
        }

        const session = sessions[0];
        if (session.status === 'closed') {
            await connection.commit();
            return { status: 'already_closed' };
        }

        const timestamp = normalizeTimestamp(new Date());

        await connection.execute(
            'UPDATE call_center_sessions SET status = ?, closed_at = ?, updated_at = ? WHERE id = ?',
            ['closed', timestamp, timestamp, sessionId],
        );

        const [queueRows] = await connection.execute(
            'SELECT id FROM live_chat_sessions WHERE call_center_session_id = ?',
            [sessionId],
        );

        if (queueRows && queueRows.length > 0) {
            for (const row of queueRows) {
                queueSessionIds.push(row.id);
            }

            await connection.execute(
                'UPDATE live_chat_sessions SET status = ?, queue_position = NULL, closed_at = ?, updated_at = ? WHERE call_center_session_id = ?',
                ['closed', timestamp, timestamp, sessionId],
            );
        }

        const [queuedRows] = await connection.execute(
            'SELECT id FROM live_chat_sessions WHERE status = ? ORDER BY queue_position ASC, created_at ASC',
            ['queued'],
        );

        if (queuedRows && queuedRows.length > 0) {
            let position = 1;
            for (const row of queuedRows) {
                await connection.execute(
                    'UPDATE live_chat_sessions SET queue_position = ? WHERE id = ?',
                    [position, row.id],
                );
                position += 1;
            }
        }

        await connection.commit();

        return {
            status: 'closed',
            queueSessionIds,
            timestamp,
        };
    } catch (error) {
        try {
            await connection.rollback();
        } catch (_) {
            // ignore rollback issues
        }
        throw error;
    } finally {
        connection.release();
    }
}

function parseSettings(raw) {
    try {
        if (!raw) {
            return { ...DEFAULT_SETTINGS, schedule: [] };
        }

        const parsed = JSON.parse(raw);
        const merged = {
            ...DEFAULT_SETTINGS,
            ...parsed,
        };

        merged.schedule = normaliseSchedule(parsed.schedule || []);

        return merged;
    } catch (error) {
        logWarning('Call center settings parse error', { error: error.message });
        return { ...DEFAULT_SETTINGS, schedule: [] };
    }
}

function startCallCenterPersistence() {
    const redisHost = process.env.REDIS_HOST || '127.0.0.1';
    const redisPort = Number(process.env.REDIS_PORT || 6379);

    const queueRedis = new Redis({
        host: redisHost,
        port: redisPort,
        db: 2,
    });

    const controlRedis = new Redis({
        host: redisHost,
        port: redisPort,
    });

    const settingsRedis = new Redis({
        host: redisHost,
        port: redisPort,
    });

    const eventPublisher = new Redis({
        host: redisHost,
        port: redisPort,
    });

    eventPublisher.on('error', (error) => {
        logWarning('Call center event publisher redis error', { error: error.message });
    });

    const expiryRedis = SESSION_INACTIVITY_TTL > 0 ? new Redis({
        host: redisHost,
        port: redisPort,
        db: EXPIRY_DB,
    }) : null;

    if (expiryRedis) {
        expiryRedis.on('error', (error) => {
            logWarning('Call center expiry redis error (persistence worker)', { error: error.message });
        });
    }

    const mysqlPool = createMysqlPool();

    let running = true;
    let settings = DEFAULT_SETTINGS;
    let queueLengthSampleIndex = 0;
    let lastQueueLength = null;

    async function loadSettings() {
        try {
            const raw = await settingsRedis.get('callcenter:settings');
            settings = parseSettings(raw);
            logInfo('Call center settings updated', settings);
        } catch (error) {
            logWarning('Failed loading call center settings', { error: error.message });
            settings = DEFAULT_SETTINGS;
        }
    }

    controlRedis.subscribe('callcenter:settings:reload', (err) => {
        if (err) {
            logWarning('Call center settings subscription failed', { error: err.message });
        }
    });

    controlRedis.on('message', (channel) => {
        if (channel === 'callcenter:settings:reload') {
            loadSettings().catch((error) => {
                logWarning('Call center settings reload failed', { error: error.message });
            });
        }
    });

    async function processLoop() {
        await loadSettings();
        logInfo('Call center persistence worker embedded loop started');

        while (running) {
            try {
                const result = await queueRedis.blpop('callcenter:persistence', 1);
                if (!result) {
                    continue;
                }

                const [, raw] = result;
                let payload;
                try {
                    payload = JSON.parse(raw);
                } catch (error) {
                    logError('Call center persistence: invalid JSON payload', { error: error.message });
                    continue;
                }

                if (!payload || !payload.action) {
                    continue;
                }

                if (payload.action === 'message') {
                    if (!payload.payload) {
                        continue;
                    }

                    if (!isWithinSchedule(settings)) {
                        // push back and pause briefly until schedule window is open
                        await queueRedis.rpush('callcenter:persistence', raw);
                        await new Promise((resolve) => setTimeout(resolve, 1000));
                        continue;
                    }

                    if (queueLengthSampleIndex === 0) {
                        try {
                            lastQueueLength = await queueRedis.llen('callcenter:persistence');
                        } catch (error) {
                            logWarning('Call center persistence: queue length sample failed', { error: error.message });
                            lastQueueLength = null;
                        }
                    }
                    queueLengthSampleIndex = (queueLengthSampleIndex + 1) % QUEUE_LENGTH_SAMPLE_INTERVAL;

                    logInfo('Call center persistence dequeued message', {
                        queueLength: lastQueueLength,
                        sessionId: payload.payload.sessionId,
                        createdAt: payload.payload.createdAt,
                        senderType: payload.payload.senderType,
                    });

                    try {
                        await persistMessage(mysqlPool, payload.payload);
                        logInfo('Call center persistence: message persisted', {
                            messageId: payload.payload.id,
                            sessionId: payload.payload.sessionId,
                        });
                    } catch (error) {
                        logError('Call center persistence message error', {
                            error: error.message,
                            payload: payload.payload,
                        });
                    }

                    continue;
                }

                if (payload.action === 'expire') {
                    const sessionId = Number(payload.payload && payload.payload.sessionId);
                    if (!Number.isFinite(sessionId) || sessionId <= 0) {
                        logWarning('Call center persistence: invalid inactivity payload', { payload });
                        continue;
                    }

                    if (expiryRedis) {
                        try {
                            const exists = await expiryRedis.exists(`${EXPIRY_KEY_PREFIX}${sessionId}`);
                            if (exists) {
                                logInfo('Skipping inactivity close because session heartbeat refreshed', { sessionId });
                                continue;
                            }
                        } catch (error) {
                            logWarning('Failed checking inactivity heartbeat key', { error: error.message, sessionId });
                        }
                    }

                    try {
                        const resultClose = await closeSessionDueToInactivity(mysqlPool, sessionId);
                        if (resultClose.status === 'closed') {
                            const eventPayload = {
                                kind: 'session_closed',
                                sessionId,
                                reason: (payload.payload && payload.payload.reason) || 'inactivity',
                                closedAt: new Date().toISOString(),
                                sourceNode: 'callcenter:persistence',
                            };

                            await eventPublisher.publish(
                                `callcenter:session:${sessionId}`,
                                JSON.stringify(eventPayload),
                            );

                            logInfo('Call center session closed due to inactivity', {
                                sessionId,
                                queueSessionsClosed: resultClose.queueSessionIds.length,
                            });
                        } else if (resultClose.status === 'already_closed') {
                            logInfo('Call center session already closed during inactivity handling', { sessionId });
                        } else if (resultClose.status === 'not_found') {
                            logWarning('Call center session not found for inactivity handling', { sessionId });
                        }
                    } catch (error) {
                        logError('Call center inactivity close error', {
                            error: error.message,
                            sessionId,
                        });
                    }

                    continue;
                }

                logWarning('Call center persistence: unknown action received', { action: payload.action });
            } catch (error) {
                logError('Call center persistence loop error', { error: error.message });
                await new Promise((resolve) => setTimeout(resolve, 5000));
            }
        }

        logInfo('Call center persistence worker stopping');
        try {
            await Promise.allSettled([
                queueRedis.quit(),
                controlRedis.quit(),
                settingsRedis.quit(),
                eventPublisher.quit(),
                expiryRedis ? expiryRedis.quit() : Promise.resolve(),
                mysqlPool.end(),
            ]);
        } catch (_) {
            // ignore
        }
    }

    processLoop().catch((error) => {
        logError('Call center persistence fatal error', { error: error.message });
    });

    return {
        stop() {
            running = false;
        },
        reload: loadSettings,
    };
}

module.exports = {
    startCallCenterPersistence,
};
