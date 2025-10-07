require('dotenv').config();
const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const { logInfo, logError } = require('../app/Helper/errorHandler');

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  db: 2
});

const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'siroum',
  charset: 'utf8mb4',
  acquireTimeout: 60000,
  timeout: 60000
};

const pool = mysql.createPool({
  ...dbConfig,
  waitForConnections: true,
  connectionLimit: Number(process.env.CHAT_DB_POOL || 4),
  queueLimit: 0
});

let stopping = false;

async function persistMessage(msg) {
  const connection = await pool.getConnection();
  try {
    await connection.execute(
      'INSERT INTO chat_messages (id, type, sender_id, recipient_id, group_id, content, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [
        msg.id,
        msg.type,
        msg.senderId,
        msg.type === 'private' ? msg.recipientId : null,
        msg.type === 'group' ? msg.groupId : null,
        msg.content,
        msg.timestamp
      ]
    );
  } finally {
    connection.release();
  }
}

async function run() {
  logInfo('Chat persistence worker started');
  while (!stopping) {
    try {
      // Block until an item is available (or 5s timeout to allow stop checks)
      const result = await redis.blpop('chat_persistence_queue', 5);
      if (!result) continue; // timeout, loop again
      const [, payload] = result;
      let messageObject;
      try {
        messageObject = JSON.parse(payload);
      } catch (e) {
        logError('Invalid JSON in chat_persistence_queue item');
        continue;
      }
      await persistMessage(messageObject);
      logInfo(`Message persisted: ${messageObject.id}`);
    } catch (err) {
      logError('Worker loop error', { error: err.message });
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  logInfo('Chat persistence worker stopping');
  try { await redis.quit(); } catch (_) {}
  try { await pool.end(); } catch (_) {}
}

['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(sig => {
  process.on(sig, () => {
    stopping = true;
  });
});

run().catch(err => {
  logError('Fatal worker error', { error: err.message });
  process.exit(1);
});
