const Redis = require('ioredis');
const { handleError, logInfo, logError } = require('../Helper/errorHandler');
const { v4: uuidv4 } = require('uuid'); // For generating unique message IDs

// Assume Redis configuration is available via process.env
// For two Redis instances, you might use different DBs or prefixes, or separate server configs
const chatRedis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    db: 1, // Example: using DB 1 for active chat data
});

const persistenceRedis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    db: 2, // Example: using DB 2 for persistence queue
});

chatRedis.on('error', (err) => logError('Chat Redis Client Error', err));
persistenceRedis.on('error', (err) => logError('Persistence Redis Client Error', err));

const connectedUsers = new Map(); // userId -> ws instance

const CHAT_HISTORY_MAX_LENGTH = 100; // Max messages to keep in real-time chat history
const CHAT_HISTORY_TTL = 3600 * 24; // Keep chat history in Redis for 24 hours

function getPrivateChatId(userId1, userId2) {
    return [userId1, userId2].sort().join(':');
}

async function handleChat(ws, user, request) {
    const userId = user.userId;
    connectedUsers.set(userId, ws);
    logInfo(`User ${userId} connected to chat.`);

    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            logInfo(`Received chat message from ${userId}:`, data);

            switch (data.type) {
                case 'private_message':
                    await handlePrivateMessage(userId, data.to, data.message);
                    break;
                case 'group_message':
                    await handleGroupMessage(userId, data.groupId, data.message);
                    break;
                case 'join_group':
                    await handleJoinGroup(userId, data.groupId);
                    break;
                case 'leave_group':
                    await handleLeaveGroup(userId, data.groupId);
                    break;
                case 'create_group':
                    await handleCreateGroup(userId, data.groupName);
                    break;
                case 'get_chat_history':
                    await handleGetChatHistory(userId, data.chatId, data.isGroup);
                    break;
                case 'typing':
                    await handleTyping(userId, data.to, data.groupId, data.isTyping);
                    break;
                case 'read_receipt':
                    await handleReadReceipt(userId, data.to, data.groupId, data.messageId);
                    break;
                default:
                    ws.send(JSON.stringify({ error: 'Unknown message type' }));
            }
        } catch (error) {
            logError(`Error processing chat message from ${userId}:`, error);
            ws.send(JSON.stringify({ error: 'Failed to process message', details: error.message }));
        }
    });

    ws.on('close', () => {
        connectedUsers.delete(userId);
        logInfo(`User ${userId} disconnected from chat.`);
        // Further cleanup for "chat redis will be wiped out when its closed" can be added here.
        // For now, this means the user's active WebSocket connection is removed.
        // Specific chat session data in chatRedis could be cleaned up based on active participants.
    });
}

async function storeMessageForPersistence(messageObject) {
    try {
        await persistenceRedis.rpush('chat_persistence_queue', JSON.stringify(messageObject));
        logInfo('Message queued for persistence:', messageObject.id);
    } catch (error) {
        logError('Failed to queue message for persistence:', error);
    }
}

async function storeAndRelayMessage(chatIdKey, messageObject, recipientWsArr) {
    const messageJSON = JSON.stringify(messageObject);
    try {
        // Store in real-time chat history (capped list)
        await chatRedis.lpush(chatIdKey, messageJSON);
        await chatRedis.ltrim(chatIdKey, 0, CHAT_HISTORY_MAX_LENGTH - 1);
        await chatRedis.expire(chatIdKey, CHAT_HISTORY_TTL);

        // Relay to connected recipients
        recipientWsArr.forEach(recipientWs => {
            if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
                recipientWs.send(JSON.stringify({ type: 'new_message', data: messageObject }));
            }
        });
        logInfo(`Message ${messageObject.id} stored and relayed for chat ${chatIdKey}`);
    } catch (error) {
        logError(`Error storing/relaying message ${messageObject.id} for ${chatIdKey}:`, error);
    }
}

async function handlePrivateMessage(senderId, recipientId, content) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();
    const chatId = getPrivateChatId(senderId, recipientId);
    const chatIdKey = `chat:private:${chatId}`;

    const messageObject = {
        id: messageId,
        type: 'private',
        senderId,
        recipientId,
        chatId: chatId,
        content,
        timestamp,
    };

    await storeMessageForPersistence(messageObject);

    const senderWs = connectedUsers.get(senderId);
    const recipientWs = connectedUsers.get(recipientId);
    const recipientsToRelay = [senderWs, recipientWs].filter(ws => ws); // Relay to sender too for confirmation

    await storeAndRelayMessage(chatIdKey, messageObject, recipientsToRelay);
}

async function handleGroupMessage(senderId, groupId, content) {
    const messageId = uuidv4();
    const timestamp = new Date().toISOString();
    const chatIdKey = `chat:group:${groupId}`;

    // Verify group existence and sender membership (simplified for now)
    const members = await chatRedis.smembers(`group_members:${groupId}`);
    if (!members.includes(senderId)) {
        const senderWs = connectedUsers.get(senderId);
        if (senderWs) {
            senderWs.send(JSON.stringify({ error: 'Not a member of this group or group does not exist.' }));
        }
        return;
    }

    const messageObject = {
        id: messageId,
        type: 'group',
        senderId,
        groupId,
        content,
        timestamp,
    };

    await storeMessageForPersistence(messageObject);

    const recipientWsArr = [];
    for (const memberId of members) {
        const memberWs = connectedUsers.get(memberId);
        if (memberWs) {
            recipientWsArr.push(memberWs);
        }
    }
    await storeAndRelayMessage(chatIdKey, messageObject, recipientWsArr);
}

async function handleCreateGroup(creatorId, groupName) {
    const groupId = uuidv4();
    await chatRedis.sadd(`group_members:${groupId}`, creatorId);
    await chatRedis.hset(`group_info:${groupId}`, 'name', groupName, 'createdBy', creatorId);
    await chatRedis.sadd(`user_groups:${creatorId}`, groupId); // Keep track of user's groups

    const creatorWs = connectedUsers.get(creatorId);
    if (creatorWs) {
        creatorWs.send(JSON.stringify({ type: 'group_created', data: { groupId, groupName } }));
    }
    logInfo(`Group ${groupName} (ID: ${groupId}) created by ${creatorId}`);
}

async function handleJoinGroup(userId, groupId) {
    // Check if group exists
    const groupExists = await chatRedis.exists(`group_info:${groupId}`);
    if (!groupExists) {
        const userWs = connectedUsers.get(userId);
        if (userWs) userWs.send(JSON.stringify({ error: 'Group not found' }));
        return;
    }
    await chatRedis.sadd(`group_members:${groupId}`, userId);
    await chatRedis.sadd(`user_groups:${userId}`, groupId);

    const userWs = connectedUsers.get(userId);
    if (userWs) {
        userWs.send(JSON.stringify({ type: 'group_joined', data: { groupId } }));
    }
    logInfo(`User ${userId} joined group ${groupId}`);

    // Notify other group members (optional)
    const members = await chatRedis.smembers(`group_members:${groupId}`);
    const groupName = await chatRedis.hget(`group_info:${groupId}`, 'name');
    for (const memberId of members) {
        if (memberId !== userId) {
            const memberWs = connectedUsers.get(memberId);
            if (memberWs) {
                memberWs.send(JSON.stringify({ type: 'member_joined_group', data: { groupId, userId, groupName } }));
            }
        }
    }
}

async function handleLeaveGroup(userId, groupId) {
    await chatRedis.srem(`group_members:${groupId}`, userId);
    await chatRedis.srem(`user_groups:${userId}`, groupId);

    const userWs = connectedUsers.get(userId);
    if (userWs) {
        userWs.send(JSON.stringify({ type: 'group_left', data: { groupId } }));
    }
    logInfo(`User ${userId} left group ${groupId}`);

    // Notify other group members (optional)
    const members = await chatRedis.smembers(`group_members:${groupId}`);
    const groupName = await chatRedis.hget(`group_info:${groupId}`, 'name');
    for (const memberId of members) {
        const memberWs = connectedUsers.get(memberId);
        if (memberWs) {
            memberWs.send(JSON.stringify({ type: 'member_left_group', data: { groupId, userId, groupName } }));
        }
    }
    // If group becomes empty, consider deleting group info (optional cleanup)
    if (members.length === 0) {
        await chatRedis.del(`group_info:${groupId}`);
        await chatRedis.del(`chat:group:${groupId}`); // Clear chat history for empty group
        logInfo(`Group ${groupId} is now empty and its info/chat history cleared from chatRedis.`);
    }
}

// Updated version of handleGetChatHistory
async function handleGetChatHistory(userId, chatId, isGroup) {
    let chatIdKey;
    if (isGroup) {
        chatIdKey = `chat:group:${chatId}`;
        // Security check: ensure user is part of the group
        const isMember = await chatRedis.sismember(`group_members:${chatId}`, userId);
        if (!isMember) {
            if (connectedUsers.has(userId))
                connectedUsers.get(userId).send(JSON.stringify({ error: "Access denied to group chat history." }));
            return;
        }
    } else {
        // For private chat, chatId is expected to be the other user's ID
        // The actual key uses both user IDs, sorted.
        const privateChatFullId = getPrivateChatId(userId, chatId);
        chatIdKey = `chat:private:${privateChatFullId}`;
    }

    // Get messages from Redis (recent chat history)
    const redisMessages = await chatRedis.lrange(chatIdKey, 0, CHAT_HISTORY_MAX_LENGTH - 1);
    let messages = redisMessages.map(msgStr => JSON.parse(msgStr));

    // If needed, get older messages from database
    if (messages.length < CHAT_HISTORY_MAX_LENGTH) {
        const oldestMessageTime = messages.length > 0 ? new Date(messages[messages.length - 1].timestamp) : new Date();

        const connection = await mysql.createConnection(dbConfig);
        const [rows] = isGroup
            ? await connection.execute(
                'SELECT * FROM chat_messages WHERE group_id = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT ?',
                [chatId, oldestMessageTime.toISOString(), CHAT_HISTORY_MAX_LENGTH - messages.length]
            )
            : await connection.execute(
                'SELECT * FROM chat_messages WHERE type = "private" AND ((sender_id = ? AND recipient_id = ?) OR (sender_id = ? AND recipient_id = ?)) AND timestamp < ? ORDER BY timestamp DESC LIMIT ?',
                [userId, chatId, chatId, userId, oldestMessageTime.toISOString(), CHAT_HISTORY_MAX_LENGTH - messages.length]
            );
        await connection.end();

        messages = [...messages, ...rows].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    const userWs = connectedUsers.get(userId);
    if (userWs) {
        userWs.send(JSON.stringify({ type: 'chat_history', data: { chatId, messages: messages.reverse() } }));
    }
}

async function handleTyping(senderId, recipientId, groupId, isTyping) {
    const payload = { type: 'typing', data: { senderId, isTyping, groupId } };
    if (groupId) {
        const members = await chatRedis.smembers(`group_members:${groupId}`);
        for (const memberId of members) {
            if (memberId !== senderId && connectedUsers.has(memberId)) {
                connectedUsers.get(memberId).send(JSON.stringify(payload));
            }
        }
    } else if (recipientId && connectedUsers.has(recipientId)) {
        connectedUsers.get(recipientId).send(JSON.stringify(payload));
    }
}

async function handleReadReceipt(senderId, recipientId, groupId, messageId) {
    const payload = { type: 'read_receipt', data: { senderId, messageId, groupId } };
    if (groupId) {
        const members = await chatRedis.smembers(`group_members:${groupId}`);
        for (const memberId of members) {
            if (memberId !== senderId && connectedUsers.has(memberId)) {
                connectedUsers.get(memberId).send(JSON.stringify(payload));
            }
        }
    } else if (recipientId && connectedUsers.has(recipientId)) {
        connectedUsers.get(recipientId).send(JSON.stringify(payload));
    }
}

module.exports = { handleChat };
