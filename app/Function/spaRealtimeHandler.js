const Redis = require('ioredis');
const { Pool } = require('pg');
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

const pgPool = new Pool({
    host: process.env.PG_HOST || '127.0.0.1',
    port: parseInt(process.env.PG_PORT || '5432', 10),
    user: process.env.PG_USER || 'siroum_chat_user',
    password: process.env.PG_PASSWORD || '',
    database: process.env.PG_DATABASE || 'siroum_realtime',
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000
});

pgPool.on('error', (err) => {
    logError('[SpaRealtime] PostgreSQL pool error', { error: err.message });
});

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_MESSAGE_LENGTH = parseInt(process.env.LIVECHAT_MESSAGEMAXLENGTH || '5000', 10);
const WS_SENDABLE_CONTEXTS = ['direct', 'group'];

/**
 * Sengaja tanpa cache: satu pencarian berindeks itu murah, sedangkan hasil yang
 * basi berarti orang yang baru dikeluarkan masih bisa menyimak ruangnya.
 */
async function isRoomMember(roomUuid, userUuid) {
    try {
        const res = await pgPool.query(
            `SELECT r.module_context, r.entity_id,
                    EXISTS (SELECT 1 FROM chat_room_members m
                            WHERE m.room_uuid = r.uuid AND m.user_uuid = $2) AS is_member
             FROM chat_rooms r WHERE r.uuid = $1 LIMIT 1`,
            [roomUuid, userUuid]
        );

        let ok = false;
        if (res.rows.length > 0) {
            const row = res.rows[0];
            ok = row.is_member === true;

            if (!ok && row.module_context === 'direct') {
                ok = String(row.entity_id || '').split('__').includes(userUuid);
            }
        }

        return ok;
    } catch (e) {
        logError('[SpaRealtime] Membership check failed', { error: e.message, roomUuid });
        return false;
    }
}

const redisSub = new Redis(redisConfig);

const redisCmd = new Redis(redisConfig);

redisCmd.on('error', (err) => {
    logError('[SpaRealtime] Redis command client error', { error: err.message });
});

const IDEMPOTENCY_PREFIX = 'chat_idem_';
const IDEMPOTENCY_TTL = 600;

const CHANNEL_GRANT_PREFIX = 'ws_grant_';

const HELPDESK_AGENT_ROLES = (process.env.LIVECHAT_AGENT_ROLE_IDS || '1,64,65,66,67,68')
    .split(',')
    .map((r) => parseInt(r.trim(), 10))
    .filter((r) => !isNaN(r));
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
async function isAuthorizedForChannel(channelName, user) {
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

    // 4. Kanal pribadi milik satu pengguna, dipakai lencana dan hitungan belum dibaca
    if (channelName.startsWith('private:user_')) {
        return String(user.userId) === channelName.slice('private:user_'.length).trim();
    }

    // 5. Antrean tiket petugas helpdesk; pemohon tidak boleh melihat daftar ini
    if (channelName === 'chat:helpdesk_workstation') {
        return isHelpdeskAgent(user);
    }

    // 6. Kanal obrolan. Dua keluarga berbeda memakai awalan yang sama:
    if (channelName.startsWith('chat:room_')) {
        let key = channelName.slice('chat:room_'.length).trim();

        if (key.startsWith('support_')) {
            key = key.slice('support_'.length);
        } else if (key.startsWith('internal_')) {
            if (!isHelpdeskAgent(user)) {
                return false;
            }
            key = key.slice('internal_'.length);
        }

        if (UUID_PATTERN.test(key)) {
            return isRoomMember(key, String(user.userId));
        }

        if (/^[0-9]+$/.test(key)) {
            return hasChannelGrant(String(user.userId), channelName);
        }

        return false;
    }

    return false;
}

function isHelpdeskAgent(user) {
    const roles = Array.isArray(user.roles) ? user.roles.map((r) => parseInt(r, 10)) : [];
    if (user.roleId) {
        roles.push(parseInt(user.roleId, 10));
    }

    return roles.some((r) => HELPDESK_AGENT_ROLES.includes(r));
}

/**
 * Titipan ditulis sisi PHP setelah memeriksa tiket ke Chatwoot.
 */
async function hasChannelGrant(userUuid, channelName) {
    try {
        const exists = await redisCmd.exists(CHANNEL_GRANT_PREFIX + userUuid + '_' + channelName);
        return exists === 1;
    } catch (e) {
        logError('[SpaRealtime] Channel grant check failed', { error: e.message, channelName });
        return false;
    }
}

/**
 * Simpan satu pesan yang dikirim lewat WebSocket, lalu balas dengan pengakuan.
 *
 */
async function handleChatSend(ws, user, payload) {
    const clientMessageId = String(payload.client_message_id || '').trim();
    const roomUuid = String(payload.conversation_uuid || '').trim();
    const text = typeof payload.message === 'string' ? payload.message : '';

    const reject = (reason) => {
        ws.send(JSON.stringify({
            type: 'chat_send_error',
            client_message_id: clientMessageId,
            error: reason
        }));
    };

    if (!clientMessageId || !UUID_PATTERN.test(roomUuid)) {
        return reject('Permintaan tidak lengkap');
    }

    if (text.trim() === '') {
        return reject('Pesan kosong');
    }

    if (text.length > MAX_MESSAGE_LENGTH) {
        return reject('Pesan terlalu panjang');
    }

    const senderUuid = String(user?.userId || '');
    if (!UUID_PATTERN.test(senderUuid)) {
        return reject('Identitas pengirim tidak sah');
    }

    const client = await pgPool.connect();
    try {
        const roomRes = await client.query(
            'SELECT module_context, entity_id FROM chat_rooms WHERE uuid = $1 LIMIT 1',
            [roomUuid]
        );
        if (roomRes.rows.length === 0) {
            return reject('Ruang tidak ditemukan');
        }

        const moduleContext = String(roomRes.rows[0].module_context || '');
        if (!WS_SENDABLE_CONTEXTS.includes(moduleContext)) {
            return reject('Ruang ini tidak mendukung pengiriman lewat WebSocket');
        }

        if (!await isRoomMember(roomUuid, senderUuid)) {
            return reject('Anda bukan anggota ruang ini');
        }

        const dup = await client.query(
            `SELECT uuid, created_at FROM chat_messages
             WHERE room_uuid = $1 AND metadata->>'client_message_id' = $2 LIMIT 1`,
            [roomUuid, clientMessageId]
        );
        if (dup.rows.length > 0) {
            return ws.send(JSON.stringify({
                type: 'chat_send_ack',
                client_message_id: clientMessageId,
                message: { uuid: dup.rows[0].uuid, created_at: dup.rows[0].created_at }
            }));
        }

        const mentions = Array.isArray(payload.mentions)
            ? payload.mentions.filter((m) => UUID_PATTERN.test(String(m)))
            : [];

        const inserted = await client.query(
            `INSERT INTO chat_messages
                (room_uuid, module_context, entity_id, sender_uuid, sender_name,
                 message, message_type, parent_message_uuid, read_by, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb)
             RETURNING uuid, created_at`,
            [
                roomUuid,
                moduleContext,
                roomRes.rows[0].entity_id,
                senderUuid,
                user?.name || user?.username || 'Pengguna',
                text,
                'text',
                UUID_PATTERN.test(String(payload.parent_message_uuid || '')) ? payload.parent_message_uuid : null,
                JSON.stringify([senderUuid]),
                JSON.stringify({ client_message_id: clientMessageId, mentions, via: 'websocket' })
            ]
        );

        const saved = {
            id: inserted.rows[0].uuid,
            uuid: inserted.rows[0].uuid,
            created_at: inserted.rows[0].created_at,
            room_uuid: roomUuid,
            sender_uuid: senderUuid,
            sender_name: user?.name || user?.username || 'Pengguna',
            sender_type: 'user',
            message: text,
            message_type: 'text',
            mentions
        };

        try {
            await redisCmd.set(IDEMPOTENCY_PREFIX + clientMessageId, saved.uuid, 'EX', IDEMPOTENCY_TTL);
        } catch (e) {
            logWarning('[SpaRealtime] Idempotency marker not written', { error: e.message });
        }

        ws.send(JSON.stringify({
            type: 'chat_send_ack',
            client_message_id: clientMessageId,
            message: saved
        }));

        const event = {
            action: 'custom_event',
            event: 'chat_message',
            room_uuid: roomUuid,
            module_context: moduleContext,
            message: saved
        };
        broadcastToChannel(`chat:room_${roomUuid}`, event);

        const members = await client.query(
            'SELECT user_uuid FROM chat_room_members WHERE room_uuid = $1 AND user_uuid <> $2',
            [roomUuid, senderUuid]
        );
        members.rows.forEach((m) => broadcastToChannel(`private:user_${m.user_uuid}`, event));
    } catch (e) {
        logError('[SpaRealtime] chat_send failed', { error: e.message, roomUuid });
        reject('Pesan gagal disimpan');
    } finally {
        client.release();
    }
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

    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data.toString());

            if (msg.action === 'subscribe' && msg.channel) {
                const channel = String(msg.channel).trim();

                if (!await isAuthorizedForChannel(channel, user)) {
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

            if (msg.action === 'chat_send') {
                await handleChatSend(ws, user, msg.payload || {});
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
    broadcastToChannel,
    isAuthorizedForChannel,
    handleChatSend
};