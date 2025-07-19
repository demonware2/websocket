const pm2 = require('pm2');
const fs = require('fs').promises;
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
          const data = await fs.readFile(logFile, 'utf8');
          const logs = data.split('\n').slice(-lines);
          resolve(logs);
        } catch (error) {
          logError(`Failed to read log file for process ${pm_id}:`, error);
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
        const metrics = processDescription[0].pm2_env.axm_monitor;
        const formattedMetrics = {};
        for (const key in metrics) {
          formattedMetrics[key] = `${metrics[key].value} ${metrics[key].unit || ''}`;
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
        const metadata = {
          'App Name': process.name,
          'Namespace': process.pm2_env.namespace || 'default',
          'Version': process.pm2_env.version || 'N/A',
          'Restarts': process.pm2_env.restart_time,
          'Uptime': `${Math.floor(process.pm2_env.pm_uptime / 3600000)}h ${Math.floor((process.pm2_env.pm_uptime % 3600000) / 60000)}m`,
          'Script path': process.pm2_env.pm_exec_path,
          'Script args': process.pm2_env.args ? process.pm2_env.args.join(' ') : 'N/A',
          'Interpreter': process.pm2_env.exec_interpreter,
          'Interpreter args': process.pm2_env.node_args ? process.pm2_env.node_args.join(' ') : 'N/A',
          'Exec mode': process.pm2_env.exec_mode,
          'Node.js version': process.version,
          'watch & reload': process.pm2_env.watch ? '✓' : '✗'
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
      allData.logs[process.id] = await getLogsPM2(process.id);
      allData.customMetrics[process.id] = await getCustomMetrics(process.id);
      allData.metadata[process.id] = await getMetadata(process.id);
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