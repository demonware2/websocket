require('dotenv').config();
const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const { v4: uuidv4 } = require('uuid');

const { logInfo, logError } = require('../app/Helper/errorHandler');

const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  db: 0
});

const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME_DPP || 'dpp_pbj',
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

redis.on('error', (err) => {
  console.error('>>> [dppChatPersistenceWorker] Redis client error:', err);
});

pool.on('error', (err) => {
  console.error('>>> [dppChatPersistenceWorker] Database pool error:', err);
});

let stopping = false;
let flushing = false;
let timerId = null;
let accumulatedJobs = [];

async function flushToDatabase() {
  if (flushing) return;
  flushing = true;

  try {
    let count = accumulatedJobs.length;
    while (count < 100) {
      const jobData = await redis.lpop('dpp_chat_persistence_queue');
      if (!jobData) break;
      try {
        accumulatedJobs.push(JSON.parse(jobData));
      } catch (e) {
        console.error('>>> [dppChatPersistenceWorker] Failed to parse job JSON:', e.message, jobData);
        logError('Failed to parse queue job, skipping invalid JSON', { data: jobData });
      }
      count++;
    }

    if (accumulatedJobs.length === 0) {
      flushing = false;
      return;
    }

    const jobs = accumulatedJobs;
    accumulatedJobs = [];



    const inserts = [];
    const deletes = [];
    const reads = new Map(); // Key: `${rppId}:${userId}:${context}`, Value: { rppId, userId, context, time }

    for (const job of jobs) {
      if (job.action === 'insert') {
        inserts.push(job.data);
      } else if (job.action === 'delete') {
        deletes.push(job.uuid);
      } else if (job.action === 'update_read') {
        const key = `${job.rpp_id}:${job.user_id}:${job.context}`;
        reads.set(key, {
          rppId: job.rpp_id,
          userId: job.user_id,
          context: job.context,
          time: job.time
        });
      }
    }

    const readList = Array.from(reads.values());

    console.log(`>>> [dppChatPersistenceWorker] Flushing: ${inserts.length} inserts, ${deletes.length} deletes, ${readList.length} read updates`);
    logInfo(`Flushing batched database sync: ${inserts.length} inserts, ${deletes.length} deletes, ${readList.length} read updates`);

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      // 1. Process batch inserts
      if (inserts.length > 0) {
        const placeholders = inserts.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ');
        const params = [];
        for (const msg of inserts) {
          params.push(
            msg.uuid,
            msg.rpp_id,
            msg.user_id,
            msg.user_name,
            msg.message || '',
            msg.attachment_path || null,
            msg.attachment_type || null,
            typeof msg.read_by === 'object' ? JSON.stringify(msg.read_by) : (msg.read_by || '[]'),
            msg.context,
            msg.created_at
          );
        }

        await connection.execute(
          `INSERT IGNORE INTO rpp_pokja_chat (uuid, rpp_id, user_id, user_name, message, attachment_path, attachment_type, read_by, context, created_at) VALUES ${placeholders}`,
          params
        );
        logInfo(`Successfully bulk-inserted ${inserts.length} messages`);
      }

      // 2. Process batch deletes
      if (deletes.length > 0) {
        const placeholders = deletes.map(() => '?').join(', ');
        await connection.execute(
          `DELETE FROM rpp_pokja_chat WHERE uuid IN (${placeholders})`,
          deletes
        );
        logInfo(`Successfully bulk-deleted ${deletes.length} messages`);
      }

      // 3. Process read status updates
      if (readList.length > 0) {
        for (const read of readList) {
          const { rppId, userId, context, time } = read;

          const [existing] = await connection.execute(
            'SELECT id FROM rpp_pokja_chat_read_status WHERE rpp_id = ? AND user_id = ? AND context = ?',
            [rppId, userId, context]
          );

          if (existing.length > 0) {
            await connection.execute(
              'UPDATE rpp_pokja_chat_read_status SET last_read_at = ? WHERE id = ?',
              [time, existing[0].id]
            );
          } else {
            const uuid = uuidv4();
            await connection.execute(
              'INSERT INTO rpp_pokja_chat_read_status (uuid, rpp_id, user_id, context, last_read_at) VALUES (?, ?, ?, ?, ?)',
              [uuid, rppId, userId, context, time]
            );
          }

          const [messages] = await connection.execute(
            'SELECT id, read_by FROM rpp_pokja_chat WHERE rpp_id = ? AND context = ? AND user_id != ? AND created_at <= ?',
            [rppId, context, userId, time]
          );

          for (const msg of messages) {
            let readByList = [];
            try {
              readByList = JSON.parse(msg.read_by);
              if (!Array.isArray(readByList)) readByList = [];
            } catch (e) {
              if (msg.read_by) readByList = [msg.read_by];
            }

            if (!readByList.includes(userId)) {
              readByList.push(userId);
              await connection.execute(
                'UPDATE rpp_pokja_chat SET read_by = ? WHERE id = ?',
                [JSON.stringify(readByList), msg.id]
              );
            }
          }
        }
        logInfo(`Successfully updated read status for ${readList.length} users`);
      }

      await connection.commit();
      console.log(`>>> [dppChatPersistenceWorker] Sync complete (${inserts.length} inserts, ${deletes.length} deletes, ${readList.length} read updates)`);
    } catch (err) {
      await connection.rollback();
      console.error('>>> [dppChatPersistenceWorker] Database transaction failed:', err);
      logError('Database transaction failed, processing retries for jobs', { error: err.message, stack: err.stack });

      for (let i = jobs.length - 1; i >= 0; i--) {
        const job = jobs[i];
        job.retries = (job.retries || 0) + 1;
        if (job.retries > 3) {
          job.error = err.message;
          job.failed_at = new Date().toISOString();
          await redis.rpush('dpp_chat_failed_jobs', JSON.stringify(job));
          console.error(`>>> [dppChatPersistenceWorker] Job reached max retries. Moved to failed queue:`, job);
          logError(`Job reached max retries, moved to failed queue: ${job.action}`, { uuid: job.data?.uuid || job.uuid, error: err.message });
        } else {
          await redis.lpush('dpp_chat_persistence_queue', JSON.stringify(job));
        }
      }
    } finally {
      connection.release();
    }
  } catch (err) {
    console.error('>>> [dppChatPersistenceWorker] Outer connection error:', err);
    logError('Error executing flushToDatabase batch', { error: err.message, stack: err.stack });
  }

  flushing = false;
}

async function startWorkerLoop() {
  while (!stopping) {
    try {
      const result = await redis.blpop('dpp_chat_persistence_queue', 5);
      if (result && !stopping) {
        const jobData = result[1];
        try {
          accumulatedJobs.push(JSON.parse(jobData));
        } catch (e) {
          console.error('>>> [dppChatPersistenceWorker] Failed to parse job JSON:', e.message, jobData);
        }

        if (!timerId) {
          timerId = setTimeout(async () => {
            timerId = null;
            await flushToDatabase();
          }, 10000);
        }
      }
    } catch (err) {
      if (!stopping) {
        console.error('>>> [dppChatPersistenceWorker] Error in worker loop:', err);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  }
}

function startDppChatPersistence() {
  stopping = false;

  startWorkerLoop().catch((err) => {
    console.error('>>> [dppChatPersistenceWorker] Critical loop error:', err);
  });

  logInfo('DPP chat persistence worker started (Redis-side buffering mode)');

  return {
    stop: async () => {
      stopping = true;
      if (timerId) {
        clearTimeout(timerId);
        timerId = null;
      }
      try {
        await flushToDatabase();
      } catch (_) {}
      try { await redis.quit(); } catch (_) {}
      try { await pool.end(); } catch (_) {}
    }
  };
}

if (require.main === module) {
  startDppChatPersistence();
} else {
  module.exports = {
    startDppChatPersistence
  };
}