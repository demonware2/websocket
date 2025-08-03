const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const winston = require('winston');
const { v4: uuidv4 } = require('uuid');

const redis = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || null,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3
});

const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_DATABASE || 'siroum',
    charset: 'utf8mb4'
};

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({ filename: './writable/logs/editor.log' })
    ]
});

class EditorHandler {
    constructor() {
        this.clients = new Map();
        this.syncTimer = null;
        this.startPeriodicSync();
    }

    async handleConnection(ws, user, rppId) {
        const clientId = uuidv4();
        
        this.clients.set(clientId, {
            ws,
            user,
            rppId,
            lastActivity: Date.now()
        });

        try {
            // Check if editor exists in database first
            const editorExists = await this.checkEditorExists(rppId);
            if (!editorExists) {
                ws.send(JSON.stringify({
                    type: 'editor_not_found',
                    message: 'Editor not found. Please create an editor first.'
                }));
                ws.close();
                return null;
            }

            const editorData = await this.getEditorData(rppId);
            
            ws.send(JSON.stringify({
                type: 'editor_init',
                data: editorData,
                clientId,
                roomInfo: {
                    rppId,
                    activeUsers: await this.getActiveUsers(rppId)
                }
            }));

            await redis.sadd(`editor:${rppId}:clients`, clientId);
            
            logger.info(`Editor client connected: ${clientId} for rpp_id: ${rppId}`);
            
            this.broadcastToEditor(rppId, {
                type: 'user_joined',
                user: user.name || user.username,
                clientId,
                timestamp: Date.now()
            }, clientId);

            return clientId;

        } catch (error) {
            logger.error('Error initializing editor connection:', error);
            ws.close();
            return null;
        }
    }

    async checkEditorExists(rppId) {
        try {
            const connection = await mysql.createConnection(dbConfig);
            const [rows] = await connection.execute(
                'SELECT COUNT(*) as count FROM rpp_hasil_kajian WHERE rpp_id = ? AND menu = "editor"',
                [rppId]
            );
            await connection.end();
            
            return rows[0].count > 0;
        } catch (error) {
            logger.error('Error checking editor exists:', error);
            return false;
        }
    }

    async getActiveUsers(rppId) {
        const clients = [];
        this.clients.forEach((client, clientId) => {
            if (client.rppId === rppId) {
                clients.push({
                    clientId,
                    userName: client.user.name || client.user.username,
                    userId: client.user.uuid
                });
            }
        });
        return clients;
    }

    async handleEditorMessage(ws, message, clientId) {
        try {
            const client = this.clients.get(clientId);
            if (!client) return;

            const { rppId } = client;
            client.lastActivity = Date.now();

            switch (message.type) {
                case 'editor_change':
                    await this.handleEditorChange(rppId, message.data, client);
                    break;
                case 'cursor_position':
                    await this.handleCursorPosition(rppId, message.data, clientId);
                    break;
                case 'editor_save':
                    await this.handleEditorSave(rppId, message.data, client);
                    break;
                case 'ping':
                    // Keep connection alive
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
            }
        } catch (error) {
            logger.error('Error handling editor message:', error);
        }
    }

    async handleEditorChange(rppId, data, client) {
        const changeKey = `editor:${rppId}:content`;
        const changeData = {
            content: data.content,
            userId: client.user.uuid,
            userName: client.user.name || client.user.username,
            timestamp: Date.now(),
            version: await redis.incr(`editor:${rppId}:version`)
        };

        await redis.setex(changeKey, 1800, JSON.stringify(changeData));
        
        this.broadcastToEditor(rppId, {
            type: 'editor_update',
            data: changeData
        }, client.clientId);

        logger.info(`Editor change for rpp_id: ${rppId} by user: ${client.user.uuid}`);
    }

    async handleCursorPosition(rppId, data, clientId) {
        const client = this.clients.get(clientId);
        if (!client) return;

        this.broadcastToEditor(rppId, {
            type: 'cursor_update',
            data: {
                clientId,
                userName: client.user.name || client.user.username,
                position: data.position,
                selection: data.selection
            }
        }, clientId);
    }

    async handleEditorSave(rppId, data, client) {
        const lockKey = `editor:${rppId}:lock`;
        const lock = await redis.set(lockKey, client.user.uuid, 'PX', 5000, 'NX');
        
        if (!lock) {
            client.ws.send(JSON.stringify({
                type: 'save_error',
                message: 'Editor is locked by another user'
            }));
            return;
        }

        try {
            const success = await this.saveToDatabase(rppId, data, client.user.uuid);
            
            if (success) {
                await redis.del(`editor:${rppId}:content`);
                await redis.incr(`editor:${rppId}:saved_version`);
                
                this.broadcastToEditor(rppId, {
                    type: 'editor_saved',
                    data: {
                        userId: client.user.uuid,
                        userName: client.user.name || client.user.username,
                        timestamp: Date.now()
                    }
                });
                
                logger.info(`Editor saved for rpp_id: ${rppId} by user: ${client.user.uuid}`);
            } else {
                client.ws.send(JSON.stringify({
                    type: 'save_error',
                    message: 'Failed to save to database'
                }));
            }
        } finally {
            await redis.del(lockKey);
        }
    }

    async handleEditorDelete(rppId, client) {
        const lockKey = `editor:${rppId}:lock`;
        const lock = await redis.set(lockKey, client.user.uuid, 'PX', 5000, 'NX');
        
        if (!lock) {
            client.ws.send(JSON.stringify({
                type: 'delete_error',
                message: 'Editor is locked by another user'
            }));
            return;
        }

        try {
            const success = await this.deleteFromDatabase(rppId, client.user.uuid);
            
            if (success) {
                await redis.del(`editor:${rppId}:content`);
                await redis.del(`editor:${rppId}:version`);
                await redis.del(`editor:${rppId}:saved_version`);
                
                this.broadcastToEditor(rppId, {
                    type: 'editor_deleted',
                    data: {
                        userId: client.user.uuid,
                        userName: client.user.name || client.user.username,
                        timestamp: Date.now()
                    }
                });
                
                logger.info(`Editor deleted for rpp_id: ${rppId} by user: ${client.user.uuid}`);
            }
        } finally {
            await redis.del(lockKey);
        }
    }

    async getEditorData(rppId) {
        const cacheKey = `editor:${rppId}:content`;
        const cachedData = await redis.get(cacheKey);
        
        if (cachedData) {
            return JSON.parse(cachedData);
        }

        const dbData = await this.loadFromDatabase(rppId);
        if (dbData) {
            const editorData = {
                content: dbData.content || '',
                userId: dbData.user_id,
                timestamp: Date.now(),
                version: 1
            };
            
            await redis.setex(cacheKey, 1800, JSON.stringify(editorData));
            return editorData;
        }

        return {
            content: '',
            userId: null,
            timestamp: Date.now(),
            version: 1
        };
    }

    async loadFromDatabase(rppId) {
        try {
            const connection = await mysql.createConnection(dbConfig);
            const [rows] = await connection.execute(
                'SELECT value as content, id_pk as user_id, update_date as updated_at FROM rpp_hasil_kajian WHERE rpp_id = ? AND menu = "editor" ORDER BY update_date DESC LIMIT 1',
                [rppId]
            );
            await connection.end();
            
            return rows[0] || null;
        } catch (error) {
            logger.error('Database load error:', error);
            return null;
        }
    }

    async saveToDatabase(rppId, data, userId) {
        try {
            const connection = await mysql.createConnection(dbConfig);
            const uuid = require('uuid').v4();
            
            // Insert or update rpp_hasil_kajian table
            await connection.execute(`
                INSERT INTO rpp_hasil_kajian (rpp_id, menu, pertanyaan, value, uuid, id_pk, is_must, create_date, update_date)
                VALUES (?, 'editor', 'Editor Content', ?, ?, ?, 0, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                id_pk = VALUES(id_pk),
                update_date = VALUES(update_date)
            `, [rppId, data.content, uuid, userId]);
            await connection.end();
            
            return true;
        } catch (error) {
            logger.error('Database save error:', error);
            return false;
        }
    }

    async deleteFromDatabase(rppId, userId) {
        try {
            const connection = await mysql.createConnection(dbConfig);
            await connection.execute(
                'DELETE FROM rpp_hasil_kajian WHERE rpp_id = ? AND menu = "editor" AND id_pk = ?',
                [rppId, userId]
            );
            await connection.end();
            
            return true;
        } catch (error) {
            logger.error('Database delete error:', error);
            return false;
        }
    }

    broadcastToEditor(rppId, message, excludeClientId = null) {
        this.clients.forEach((client, clientId) => {
            if (client.rppId === rppId && clientId !== excludeClientId) {
                if (client.ws.readyState === 1) {
                    client.ws.send(JSON.stringify(message));
                }
            }
        });
    }

    async handleDisconnection(clientId) {
        const client = this.clients.get(clientId);
        if (!client) return;

        const { rppId, user } = client;
        
        await redis.srem(`editor:${rppId}:clients`, clientId);
        this.clients.delete(clientId);
        
        this.broadcastToEditor(rppId, {
            type: 'user_left',
            user: user.name || user.username,
            clientId,
            timestamp: Date.now()
        });
        
        logger.info(`Editor client disconnected: ${clientId} for rpp_id: ${rppId}`);
    }

    startPeriodicSync() {
        this.syncTimer = setInterval(async () => {
            const activeRppIds = new Set();
            this.clients.forEach(client => activeRppIds.add(client.rppId));
            
            for (const rppId of activeRppIds) {
                await this.syncToDatabase(rppId);
            }
        }, 300000); // Sync every 5 minutes
    }

    async syncToDatabase(rppId) {
        try {
            const cacheKey = `editor:${rppId}:content`;
            const cachedData = await redis.get(cacheKey);
            
            if (cachedData) {
                const data = JSON.parse(cachedData);
                const currentVersion = await redis.get(`editor:${rppId}:version`) || 1;
                const savedVersion = await redis.get(`editor:${rppId}:saved_version`) || 0;
                
                if (parseInt(currentVersion) > parseInt(savedVersion)) {
                    const success = await this.saveToDatabase(rppId, data, data.userId);
                    if (success) {
                        await redis.set(`editor:${rppId}:saved_version`, currentVersion);
                        logger.info(`Auto-synced editor data for rpp_id: ${rppId}`);
                    }
                }
            }
        } catch (error) {
            logger.error(`Error syncing rpp_id ${rppId}:`, error);
        }
    }

    cleanup() {
        if (this.syncTimer) {
            clearInterval(this.syncTimer);
        }
        redis.disconnect();
    }
}

module.exports = new EditorHandler();