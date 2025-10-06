const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const { logInfo, logError, logWarning } = require('../app/Helper/errorHandler');

const DEFAULT_SETTINGS = {
    enabled: true,
    timezone: process.env.CALL_CENTER_TIMEZONE || 'Asia/Jakarta',
    schedule: [],
};

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

async function persistMessage(pool, message) {
    const connection = await pool.getConnection();
    try {
        const [session] = await connection.execute('SELECT id FROM call_center_sessions WHERE id = ?', [message.sessionId]);
        if (!session.length) {
            logWarning('Call center persistence: unknown session id', { sessionId: message.sessionId });
            return;
        }

        const [insertResult] = await connection.execute(
            'INSERT INTO call_center_messages (session_id, sender_type, sender_id, message_type, content, metadata, created_at, updated_at, attachment_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                message.sessionId,
                message.senderType,
                message.senderId,
                message.messageType || 'text',
                message.content,
                message.metadata ? JSON.stringify(message.metadata) : null,
                message.createdAt,
                message.createdAt,
                message.attachment ? message.attachment.id : null,
            ],
        );

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
                    message.attachment.created_at || message.createdAt,
                    message.attachment.updated_at || message.createdAt,
                ],
            );
        }

        await connection.execute(
            'UPDATE call_center_sessions SET last_message_at = ?, status = IF(status = "pending", "open", status), updated_at = ? WHERE id = ?',
            [message.createdAt, message.createdAt, message.sessionId],
        );
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
    const queueRedis = new Redis({
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: Number(process.env.REDIS_PORT || 6379),
        db: 2,
    });

    const controlRedis = new Redis({
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: Number(process.env.REDIS_PORT || 6379),
    });

    const mysqlPool = createMysqlPool();

    let running = true;
    let settings = DEFAULT_SETTINGS;

    async function loadSettings() {
        try {
            const raw = await controlRedis.get('callcenter:settings');
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
                if (!isWithinSchedule(settings)) {
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                    continue;
                }

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

                if (!payload || payload.action !== 'message' || !payload.payload) {
                    continue;
                }

                await persistMessage(mysqlPool, payload.payload);
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
