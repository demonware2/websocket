const pm2 = require('pm2');
const fs = require('fs').promises;
const fssync = require('fs');
const { AuthenticationError, handleError, logError, logInfo } = require('../Helper/errorHandler');
const { log } = require('console');

function connectToPM2() {
  return new Promise((resolve, reject) => {
    pm2.connect({
      socketPath: '/home/dicky/.pm2/rpc.sock'
    }, (err) => {
      if (err) {
        logError('Failed to connect to PM2:', err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

function getProcesses() {
  return new Promise((resolve, reject) => {
    pm2.list((err, list) => {
      if (err) {
        logError('Failed to get process list:', err);
        reject(err);
      } else {
        const processes = list.map(process => ({
          id: process.pm_id,
          name: process.name,
          mem: Math.round(process.monit.memory / 1024 / 1024),
          cpu: process.monit.cpu,
          status: process.pm2_env.status
        }));
        resolve(processes);
      }
    });
  });
}

// Efficiently read only the last N lines of a file without loading the whole file
async function tailFileLastLines(filePath, maxLines = 100, chunkSize = 128 * 1024) {
  try {
    const stat = await fs.stat(filePath);
    if (!stat || stat.size === 0) return [];

    const fd = await fs.open(filePath, 'r');
    try {
      let position = stat.size;
      let linesFound = 0;
      const chunks = [];

      while (position > 0 && linesFound <= maxLines) {
        const readStart = Math.max(0, position - chunkSize);
        const toRead = position - readStart;
        const buf = Buffer.allocUnsafe(toRead);
        await fd.read(buf, 0, toRead, readStart);
        chunks.push(buf);

        // Count newlines in this chunk
        for (let i = 0; i < buf.length; i++) {
          if (buf[i] === 0x0a) linesFound++; // '\n'
        }

        position = readStart;

        // Safety stop in case of extremely long lines; avoid reading entire gigantic files
        if (chunks.length > 128 && linesFound === 0) break; // ~16MB guard at 128 * 128KB
      }

      // Build string from the collected tail chunks (reverse order)
      const data = Buffer.concat(chunks.reverse()).toString('utf8');
      const allLines = data.split('\n');
      const lastLines = allLines.slice(-maxLines);
      return lastLines;
    } finally {
      await fd.close();
    }
  } catch (err) {
    // If tailing fails (e.g., file truncated/rotated), fallback to best-effort small read
    try {
      const fallbackSize = 64 * 1024;
      const stat2 = await fs.stat(filePath);
      const start = Math.max(0, stat2.size - fallbackSize);
      const stream = fssync.createReadStream(filePath, { start, end: stat2.size });
      const chunks = [];
      return await new Promise((resolve) => {
        stream.on('data', (c) => chunks.push(c));
        stream.on('error', () => resolve([]));
        stream.on('end', () => {
          const txt = Buffer.concat(chunks).toString('utf8');
          resolve(txt.split('\n').slice(-maxLines));
        });
      });
    } catch (_) {
      return [];
    }
  }
}

async function getLogsPM2(pm_id, lines = 100) {
  return new Promise((resolve, reject) => {
    pm2.describe(pm_id, async (err, processDescription) => {
      if (err) {
        logError(`Failed to get description for process ${pm_id}:`, err);
        reject(err);
      } else if (!processDescription || processDescription.length === 0) {
        logError(`No description found for process ${pm_id}`);
        reject(new Error('Process not found'));
      } else {
        const logFile = processDescription[0].pm2_env.pm_out_log_path;
        try {
          const logs = await tailFileLastLines(logFile, lines);
          resolve(logs);
        } catch (error) {
          logError(`Failed to tail log file for process ${pm_id}:`, error);
          reject(error);
        }
      }
    });
  });
}

function getCustomMetrics(pm_id) {
  return new Promise((resolve, reject) => {
    pm2.describe(pm_id, (err, processDescription) => {
      if (err) {
        logError(`Failed to get custom metrics for process ${pm_id}:`, err);
        reject(err);
      } else if (!processDescription || processDescription.length === 0) {
        logError(`No description found for process ${pm_id}`);
        reject(new Error('Process not found'));
      } else {
        const metrics = processDescription[0].pm2_env.axm_monitor || {};
        const formattedMetrics = {};
        for (const key in metrics) {
          if (metrics[key] && typeof metrics[key] === 'object') {
            formattedMetrics[key] = `${metrics[key].value || ''} ${metrics[key].unit || ''}`.trim();
          } else {
            formattedMetrics[key] = metrics[key];
          }
        }
        resolve(formattedMetrics);
      }
    });
  });
}

function getMetadata(pm_id) {
  return new Promise((resolve, reject) => {
    pm2.describe(pm_id, (err, processDescription) => {
      if (err) {
        logError(`Failed to get metadata for process ${pm_id}:`, err);
        reject(err);
      } else if (!processDescription || processDescription.length === 0) {
        logError(`No description found for process ${pm_id}`);
        reject(new Error('Process not found'));
      } else {
        const process = processDescription[0];
        const pm2_env = process.pm2_env || {};

        // Calculate uptime safely
        let uptimeDisplay = 'N/A';
        if (pm2_env.pm_uptime) {
          const uptimeMs = Date.now() - pm2_env.pm_uptime;
          const hours = Math.floor(uptimeMs / 3600000);
          const minutes = Math.floor((uptimeMs % 3600000) / 60000);
          uptimeDisplay = `${hours}h ${minutes}m`;
        }

        const metadata = {
          'App Name': process.name || 'N/A',
          'Namespace': pm2_env.namespace || 'default',
          'Version': pm2_env.version || 'N/A',
          'Restarts': pm2_env.restart_time || 0,
          'Uptime': uptimeDisplay,
          'Script path': pm2_env.pm_exec_path || 'N/A',
          'Script args': pm2_env.args ? pm2_env.args.join(' ') : 'N/A',
          'Interpreter': pm2_env.exec_interpreter || 'N/A',
          'Interpreter args': pm2_env.node_args ? pm2_env.node_args.join(' ') : 'N/A',
          'Exec mode': pm2_env.exec_mode || 'N/A',
          'Node.js version': process.version || 'N/A',
          'watch & reload': pm2_env.watch ? '✓' : '✗'
        };
        resolve(metadata);
      }
    });
  });
}

async function getAllDataPM2() {
  try {
    await connectToPM2();
    const processes = await getProcesses();
    const allData = {
      processes,
      logs: {},
      customMetrics: {},
      metadata: {}
    };

    for (const process of processes) {
      try {
        allData.logs[process.id] = await getLogsPM2(process.id);
      } catch (error) {
        logError(`Failed to get logs for process ${process.id}:`, error);
        allData.logs[process.id] = [];
      }

      try {
        allData.customMetrics[process.id] = await getCustomMetrics(process.id);
      } catch (error) {
        logError(`Failed to get custom metrics for process ${process.id}:`, error);
        allData.customMetrics[process.id] = {};
      }

      try {
        allData.metadata[process.id] = await getMetadata(process.id);
      } catch (error) {
        logError(`Failed to get metadata for process ${process.id}:`, error);
        allData.metadata[process.id] = {};
      }
    }

    return allData;
  } catch (error) {
    throw error;
  } finally {
    pm2.disconnect();
  }
}

module.exports = {
  getAllDataPM2,
  getLogsPM2
};
