const os = require('os');
const { exec } = require('child_process');

// Harden exec usage: concurrency limit, timeouts, and buffer caps
const MAX_EXEC_CONCURRENCY = parseInt(process.env.SI_MAX_EXEC || '4', 10);
const EXEC_TIMEOUT_MS = parseInt(process.env.SI_EXEC_TIMEOUT || '4000', 10);
const EXEC_MAX_BUFFER = parseInt(process.env.SI_EXEC_MAX_BUFFER || String(512 * 1024), 10);
let __execActive = 0;
const __execQueue = [];
function safeExec(cmd, options = {}) {
  return new Promise((resolve) => {
    const run = () => {
      __execActive++;
      exec(cmd, { timeout: EXEC_TIMEOUT_MS, maxBuffer: EXEC_MAX_BUFFER, ...options }, (error, stdout = '', stderr = '') => {
        __execActive--;
        if (__execQueue.length) setImmediate(__execQueue.shift());
        resolve({ error, stdout, stderr });
      });
    };
    if (__execActive < MAX_EXEC_CONCURRENCY) run(); else __execQueue.push(run);
  });
}
function withTimeout(promise, ms, onTimeoutValue) {
  let t;
  return Promise.race([
    promise.finally(() => clearTimeout(t)),
    new Promise((resolve) => (t = setTimeout(() => resolve(onTimeoutValue), ms)))
  ]);
}

// Module-level sampling cache to avoid running heavy commands per-connection
let cachedSystemData = null;
let samplingInterval = null;
let inflightSample = null;
const SAMPLE_INTERVAL_MS = parseInt(process.env.SI_SAMPLE_INTERVAL_MS || '5000', 10);
const SAMPLE_TIMEOUT_MS = parseInt(process.env.SI_SAMPLE_TIMEOUT_MS || '4500', 10);

async function sampleSystemInfoOnce() {
  try {
    const data = await withTimeout(gatherSystemData(), SAMPLE_TIMEOUT_MS, null);
    if (data) cachedSystemData = data;
    return cachedSystemData;
  } catch (e) {
    return cachedSystemData || { timestamp: Date.now(), error: e.message };
  }
}

function ensureSamplerStarted() {
  if (samplingInterval) return;
  // Kick off an immediate sample, and then schedule periodic updates
  inflightSample = sampleSystemInfoOnce().finally(() => { inflightSample = null; });
  samplingInterval = setInterval(() => {
    if (!inflightSample) {
      inflightSample = sampleSystemInfoOnce().finally(() => { inflightSample = null; });
    }
  }, SAMPLE_INTERVAL_MS);
}
let previousNetworkData = {};
let previousDiskStats = null;
let previousTimeStamp = Date.now();

const maxBandwidth = 100 * 1024 * 1024


function getCpuInfo() {
  const cpus = os.cpus();
  const load = os.loadavg();
  return {
    load_1min: load[0],
    load_5min: load[1],
    load_15min: load[2],
    cores: cpus.length,
    model: cpus[0] && cpus[0].model ? cpus[0].model : 'Unknown',
    speed: cpus[0] && cpus[0].speed ? cpus[0].speed : 0,
    usage: Math.min(100, (load[0] / cpus.length) * 100),
    manufacturer: getProcessorManufacturer(cpus[0] && cpus[0].model ? cpus[0].model : ''),
    brand: cpus[0] && cpus[0].model ? cpus[0].model : 'Unknown',
    physicalCores: Math.ceil(cpus.length / 2)
  };
}


function getProcessorManufacturer(modelString) {
  if (modelString.includes('Intel')) return 'Intel';
  if (modelString.includes('AMD')) return 'AMD';
  if (modelString.includes('Apple')) return 'Apple';
  return 'Unknown';
}


async function getDetailedCpuInfo() {
  const { stdout: tOut } = await safeExec('cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null || echo "N/A"');
  const temps = (tOut || '').trim().split('\n').map(t => parseInt(t) / 1000).filter(t => !isNaN(t));
  const avgTemp = temps.length ? temps.reduce((a, b) => a + b) / temps.length : null;

  const { stdout: fOut } = await safeExec('cat /proc/cpuinfo | grep "cpu MHz"');
  const frequencies = (fOut || '').trim().split('\n').map(line => {
    const match = line.match(/([0-9.]+)$/);
    return match ? parseFloat(match[1]) : null;
  }).filter(f => f !== null);
  const avgFreq = frequencies.length ? frequencies.reduce((a, b) => a + b) / frequencies.length : null;

  const { error: coreErr, stdout: coreOut } = await safeExec('mpstat -P ALL 1 1 | grep -v CPU | grep -v Average');
  let coreLoad = [];
  if (!coreErr) {
    const lines = (coreOut || '').trim().split('\n');
    coreLoad = lines.map(line => {
      const parts = line.trim().split(/\s+/);
      return parts.length > 10 ? 100 - parseFloat(parts[parts.length - 1]) : null;
    }).filter(load => load !== null);
  }

  return {
    temperature: avgTemp,
    frequency: avgFreq ? (avgFreq / 1000).toFixed(2) : null,
    coreLoad: coreLoad.length ? coreLoad : null
  };
}


function getMemoryInfo() {
  const total = os.totalmem();
  const free = os.freemem();
  const used = total - free;

  return {
    total,
    free,
    used,
    active: used,
    available: free,
    usage: (used / total) * 100
  };
}

async function getDetailedMemoryInfo() {
  const { error, stdout } = await safeExec('cat /proc/meminfo');
  if (error) return {};
  const memInfo = {};
  const lines = (stdout || '').trim().split('\n');
  lines.forEach(line => {
    const [key, valueWithUnit] = line.split(':');
    const valueMatch = valueWithUnit && valueWithUnit.trim().match(/^(\d+)/);
    if (valueMatch) {
      memInfo[key.trim()] = parseInt(valueMatch[1]) * 1024;
    }
  });
  return {
    buffers: memInfo.Buffers || 0,
    cached: memInfo.Cached || 0,
    swap_total: memInfo.SwapTotal || 0,
    swap_free: memInfo.SwapFree || 0,
    swap_used: memInfo.SwapTotal ? (memInfo.SwapTotal - memInfo.SwapFree) : 0,
    swap_usage: memInfo.SwapTotal ? ((memInfo.SwapTotal - memInfo.SwapFree) / memInfo.SwapTotal * 100) : 0
  };
}

// Replace diskusage with df command
async function getDiskInfo() {
  const { error, stdout } = await safeExec('df / --output=size,used,avail | tail -n 1');
  if (error) return { total: 0, free: 0, used: 0, usage: 0 };
  try {
    const parts = (stdout || '').trim().split(/\s+/);
    if (parts.length >= 3) {
      const total = parseInt(parts[0]) * 1024;
      const used = parseInt(parts[1]) * 1024;
      const free = parseInt(parts[2]) * 1024;
      return { total, free, used, usage: total > 0 ? (used / total) * 100 : 0 };
    }
  } catch (_) {}
  return { total: 0, free: 0, used: 0, usage: 0 };
}

async function getDetailedDiskInfo() {
  const { error, stdout } = await safeExec('df -h');
  if (error) return [];
  try {
    const lines = (stdout || '').trim().split('\n').slice(1);
    const disks = lines.map(line => {
      const parts = line.trim().split(/\s+/);
      let filesystem, size, used, available, use, mount;
      if (parts.length >= 6) {
        filesystem = parts[0];
        size = parts[1];
        used = parts[2];
        available = parts[3];
        use = parseInt(parts[4].replace('%', ''));
        mount = parts[5];
      } else {
        filesystem = 'Unknown';
        size = '0';
        used = '0';
        available = '0';
        use = 0;
        mount = 'Unknown';
      }
      const convertToBytes = (sizeStr) => {
        const units = { 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4 };
        const match = sizeStr.match(/^([0-9.]+)([KMGT])?/i);
        if (!match) return 0;
        const num = parseFloat(match[1]);
        const unit = match[2] && match[2].toUpperCase() ? match[2].toUpperCase() : '';
        return num * (units[unit] || 1);
      };
      return { fs: filesystem, size: convertToBytes(size), used: convertToBytes(used), available: convertToBytes(available), use, mount };
    });
    return disks;
  } catch (_) { return []; }
}

async function getDiskIOStats() {
  const { error, stdout } = await safeExec('cat /proc/diskstats');
  if (error) return [];
  try {
    const lines = (stdout || '').trim().split('\n');
    const currentTime = Date.now();
    const timeDiff = (currentTime - previousTimeStamp) / 1000;
    const diskStats = [];
    lines.forEach(line => {
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 14) {
        const deviceName = parts[2];
        if (!deviceName.startsWith('loop') && !deviceName.startsWith('ram') && !deviceName.startsWith('dm-')) {
          const currentStats = {
            device: deviceName,
            reads_completed: parseInt(parts[3]),
            reads_merged: parseInt(parts[4]),
            sectors_read: parseInt(parts[5]),
            time_reading_ms: parseInt(parts[6]),
            writes_completed: parseInt(parts[7]),
            writes_merged: parseInt(parts[8]),
            sectors_written: parseInt(parts[9]),
            time_writing_ms: parseInt(parts[10]),
            io_in_progress: parseInt(parts[11]),
            time_io_ms: parseInt(parts[12]),
            weighted_time_io_ms: parseInt(parts[13])
          };
          if (previousDiskStats && timeDiff > 0) {
            const prevDeviceStats = previousDiskStats.find(d => d.device === deviceName);
            if (prevDeviceStats) {
              const sectorSize = 512;
              currentStats.read_rate_bytes_per_sec = ((currentStats.sectors_read - prevDeviceStats.sectors_read) * sectorSize) / timeDiff;
              currentStats.write_rate_bytes_per_sec = ((currentStats.sectors_written - prevDeviceStats.sectors_written) * sectorSize) / timeDiff;
              currentStats.read_rate_mb_per_sec = currentStats.read_rate_bytes_per_sec / (1024 * 1024);
              currentStats.write_rate_mb_per_sec = currentStats.write_rate_bytes_per_sec / (1024 * 1024);
              currentStats.io_rate = ((currentStats.reads_completed - prevDeviceStats.reads_completed) + (currentStats.writes_completed - prevDeviceStats.writes_completed)) / timeDiff;
            }
          }
          diskStats.push(currentStats);
        }
      }
    });
    previousDiskStats = diskStats;
    previousTimeStamp = currentTime;
    return diskStats;
  } catch (_) { return []; }
}

async function getNetworkInfo() {
  const { error, stdout } = await safeExec('cat /proc/net/dev');
  if (error) return [];
  try {
    const lines = (stdout || '').trim().split('\n').slice(2);
    const currentTime = Date.now();
    const interfaces = [];
    lines.forEach(line => {
      const parts = line.trim().split(/\s+/);
      const interfaceName = parts[0].replace(':', '');
      if (interfaceName !== 'lo') {
        const currentStats = {
          interface: interfaceName,
          rx_bytes: parseInt(parts[1]),
          rx_packets: parseInt(parts[2]),
          rx_errors: parseInt(parts[3]),
          rx_dropped: parseInt(parts[4]),
          tx_bytes: parseInt(parts[9]),
          tx_packets: parseInt(parts[10]),
          tx_errors: parseInt(parts[11]),
          tx_dropped: parseInt(parts[12])
        };
        if (previousNetworkData[interfaceName]) {
          const timeDiff = (currentTime - previousNetworkData[interfaceName].timestamp) / 1000;
          if (timeDiff > 0) {
            currentStats.rx_bytes_per_sec = (currentStats.rx_bytes - previousNetworkData[interfaceName].rx_bytes) / timeDiff;
            currentStats.tx_bytes_per_sec = (currentStats.tx_bytes - previousNetworkData[interfaceName].tx_bytes) / timeDiff;
            currentStats.rx_mbps = currentStats.rx_bytes_per_sec * 8 / 1000000;
            currentStats.tx_mbps = currentStats.tx_bytes_per_sec * 8 / 1000000;
          }
        }
        previousNetworkData[interfaceName] = { rx_bytes: currentStats.rx_bytes, tx_bytes: currentStats.tx_bytes, timestamp: currentTime };
        interfaces.push(currentStats);
      }
    });
    return interfaces;
  } catch (_) { return [];} 
}

async function getProcessInfo() {
  const { error, stdout } = await safeExec('ps -eo state --no-header | sort | uniq -c');
  let summary = { all: 0, running: 0, sleeping: 0, stopped: 0, zombie: 0, other: 0 };
  if (!error) {
    (stdout || '').trim().split('\n').forEach(line => {
      const [count, state] = line.trim().split(/\s+/);
      const countNum = parseInt(count);
      summary.all += countNum;
      switch (state) {
        case 'R': summary.running += countNum; break;
        case 'S':
        case 'I': summary.sleeping += countNum; break;
        case 'T': summary.stopped += countNum; break;
        case 'Z': summary.zombie += countNum; break;
        case 'D': summary.blocked = (summary.blocked || 0) + countNum; break;
        default: summary.other += countNum;
      }
    });
  }
  const { error: e2, stdout: s2 } = await safeExec('ps aux --sort=-%cpu | head -n 11');
  let top = [];
  if (!e2) {
    try {
      const lines = (s2 || '').trim().split('\n').slice(1);
      top = lines.map(line => {
        const parts = line.trim().split(/\s+/);
        const user = parts[0];
        const pid = parseInt(parts[1]);
        const cpu = parseFloat(parts[2]);
        const mem = parseFloat(parts[3]);
        const vsz = parseInt(parts[4]);
        const rss = parseInt(parts[5]);
        const tty = parts[6];
        const stat = parts[7];
        const start = parts[8];
        const time = parts[9];
        const command = parts.slice(10).join(' ');
        const all = summary.all;
        return { user, pid, cpu, mem, vsz, rss, tty, stat, start, time, command: command.length > 50 ? command.substring(0, 47) + '...' : command, all };
      });
    } catch (_) { top = []; }
  }
  return { ...summary, top };
}

async function getUserInfo() {
  const { error, stdout } = await safeExec('who --ips 2>/dev/null || who 2>/dev/null || echo ""');
  if (error) return [];
  try {
    const lines = (stdout || '').trim().split('\n');
    const users = lines.filter(line => line.trim() !== '').map(line => {
      const parts = line.trim().split(/\s+/);
      let user, tty, date, time, from;
      if (parts.length >= 5) {
        user = parts[0];
        tty = parts[1];
        date = parts[2];
        time = parts[3];
        from = (parts[4] || '').replace(/[()]/g, '');
      } else {
        user = parts[0] || 'unknown';
        tty = parts[1] || 'unknown';
        date = parts[2] || '';
        time = parts[3] || '';
        from = 'local';
      }
      return { user, tty, date, time, ip: from !== 'local' ? from : null };
    });
    return users;
  } catch (_) { return []; }
}

let servicesCache = { data: null, ts: 0 };
const SERVICES_TTL_MS = parseInt(process.env.SI_SERVICES_TTL_MS || '30000', 10);
async function getServicesInfo() {
  const now = Date.now();
  if (servicesCache.data && now - servicesCache.ts < SERVICES_TTL_MS) return servicesCache.data;
  const { error, stdout } = await safeExec('systemctl list-units --type=service --state=active,running,failed --no-legend 2>/dev/null || echo ""');
  if (error || !stdout.trim()) {
    const { error: e2, stdout: s2 } = await safeExec('service --status-all 2>&1');
    if (e2) return [];
    try {
      const lines = (s2 || '').trim().split('\n');
      const services = lines.map(line => {
        const match = line.trim().match(/\[\s*([+-?])\s*\]\s+(.+)$/);
        if (!match) return null;
        const status = match[1];
        const name = match[2];
        return { name, running: status === '+', cpu: null, mem: null };
      }).filter(Boolean);
      servicesCache = { data: services, ts: now };
      return services;
    } catch (_) { return []; }
  } else {
    try {
      const lines = (stdout || '').trim().split('\n');
      const services = lines.filter(line => line.trim() !== '').map(line => {
        const parts = line.trim().split(/\s+/);
        let name = '';
        let state = '';
        if (parts.length >= 3) { name = parts[0]; state = parts[2]; }
        return { name, running: state === 'running', cpu: null, mem: null };
      });
      if (services.length > 0) {
        const serviceNames = services.map(s => s.name.replace('.service', '')).join('|');
        const { error: psErr, stdout: psOut } = await safeExec(`ps -eo pid,pmem,pcpu,comm | grep -E '(${serviceNames})' | grep -v grep`);
        if (!psErr && (psOut || '').trim()) {
          const psLines = psOut.trim().split('\n');
          psLines.forEach(psLine => {
            const [pid, mem, cpu, comm] = psLine.trim().split(/\s+/);
            const service = services.find(s => comm.includes(s.name.replace('.service', '')));
            if (service) { service.cpu = parseFloat(cpu); service.mem = parseFloat(mem); }
          });
        }
      }
      servicesCache = { data: services, ts: now };
      return services;
    } catch (_) { return []; }
  }
}

function getSystemInfoRaw() {
  return new Promise((resolve) => {
    const basicInfo = {
      hostname: os.hostname(),
      platform: os.platform(),
      arch: os.arch(),
      uptime: os.uptime(),
      loadavg: os.loadavg(),
      totalmem: os.totalmem(),
      freemem: os.freemem()
    };

    exec('cat /etc/os-release 2>/dev/null || echo "ID=unknown\\nVERSION_ID=unknown"', (error, stdout) => {
      let osInfo = {
        distro: 'Unknown',
        release: 'Unknown',
        version: 'Unknown'
      };

      if (!error) {
        const lines = stdout.trim().split('\n');
        lines.forEach(line => {
          const [key, ...valueParts] = line.split('=');
          const value = valueParts.join('=').replace(/"/g, '');

          if (key === 'ID') osInfo.distro = value;
          if (key === 'VERSION_ID') osInfo.version = value;
          if (key === 'PRETTY_NAME') osInfo.release = value;
        });
      }

      exec('cat /proc/sys/fs/file-nr 2>/dev/null || echo "0 0 0"', (fileErr, fileOut) => {
        let fileStats = {};

        if (!fileErr) {
          const [allocated, _, max] = fileOut.trim().split(/\s+/);
          fileStats = {
            open_files: parseInt(allocated),
            max_files: parseInt(max),
            file_usage_percent: (parseInt(allocated) / parseInt(max) * 100).toFixed(2)
          };
        }

        const commands = [
          'cat /sys/class/dmi/id/sys_vendor 2>/dev/null || echo "Unknown"',
          'cat /sys/class/dmi/id/product_name 2>/dev/null || echo "Unknown"',
          'cat /sys/class/dmi/id/product_version 2>/dev/null || echo "Unknown"',
          'uname -r'
        ];

        Promise.all(commands.map(cmd => new Promise((resolve) =>
          exec(cmd, (err, out) => resolve(err ? 'Unknown' : out.trim()))
        ))).then(([manufacturer, model, version, kernel]) => {
          resolve({
            ...basicInfo,
            ...osInfo,
            ...fileStats,
            manufacturer,
            model,
            version,
            kernel
          });
        });
      });
    });
  });
}

let __versionsCache = { data: null, ts: 0 };
const VERSIONS_TTL_MS = parseInt(process.env.SI_VERSIONS_TTL_MS || String(5 * 60 * 1000), 10);
async function getVersionsInfo() {
  const now = Date.now();
  if (__versionsCache.data && now - __versionsCache.ts < VERSIONS_TTL_MS) return __versionsCache.data;
  const commands = {
    node: 'node -v 2>/dev/null || echo "N/A"',
    npm: 'npm -v 2>/dev/null || echo "N/A"',
    kernel: 'uname -r 2>/dev/null || echo "N/A"',
    openssl: 'openssl version 2>/dev/null || echo "N/A"',
    php: 'php -v 2>/dev/null | head -n 1 || echo "N/A"',
    mysql: 'mysql --version 2>/dev/null || echo "N/A"',
    apache: 'apache2 -v 2>/dev/null || httpd -v 2>/dev/null || echo "N/A"',
    nginx: 'nginx -v 2>&1 || echo "N/A"'
  };
  const versions = {};
  for (const [key, cmd] of Object.entries(commands)) {
    const { error, stdout } = await safeExec(cmd);
    let version = error ? 'N/A' : (stdout || '').trim();
    if (key === 'openssl') {
      const match = version.match(/OpenSSL\s+(\S+)/i);
      version = match ? match[1] : version;
      if (version.includes('quic')) {
        const parts = version.split('+');
        if (parts.length > 1) version = parts[0] + '+quic';
      }
    } else if (key === 'php') {
      const match = version.match(/PHP\s+(\S+)/);
      version = match ? match[1] : version;
    } else if (key === 'mysql') {
      const match = version.match(/Distrib\s+(\S+)/);
      version = match ? match[1] : version;
    } else if (key === 'apache') {
      const match = version.match(/version:?\s+Apache\/(\S+)/i);
      version = match ? match[1] : version;
    } else if (key === 'nginx') {
      const match = version.match(/nginx\/(\S+)/);
      version = match ? match[1] : version;
    }
    if (version !== 'N/A') versions[key] = version;
  }
  __versionsCache = { data: versions, ts: now };
  return versions;
}

async function gatherSystemData() {
  try {
    const [
      cpuDetailedInfo,
      memoryDetailedInfo,
      diskInfo,
      diskIOStats,
      detailedDiskInfo,
      networkInfo,
      processInfo,
      userInfo,
      servicesInfo,
      systemInfo,
      versionsInfo
    ] = await Promise.all([
      getDetailedCpuInfo(),
      getDetailedMemoryInfo(),
      getDiskInfo(),
      getDiskIOStats(),
      getDetailedDiskInfo(),
      getNetworkInfo(),
      getProcessInfo(),
      getUserInfo(),
      getServicesInfo(),
      // Important: use the raw OS info collector here to avoid recursion
      // The public getSystemInfo() below triggers the sampler which calls gatherSystemData() again
      // causing infinite recursion and EMFILE errors. Use getSystemInfoRaw() instead.
      getSystemInfoRaw(),
      getVersionsInfo()
    ]);

    const cpuInfo = getCpuInfo();
    const memoryInfo = getMemoryInfo();

    return {
      timestamp: Date.now(),
      cpu: {
        ...cpuInfo,
        ...cpuDetailedInfo
      },
      memory: {
        ...memoryInfo,
        ...memoryDetailedInfo
      },
      disk: diskInfo,
      disk_io: diskIOStats,
      storage: detailedDiskInfo,
      network: networkInfo,
      processes: processInfo,
      users: userInfo,
      services: servicesInfo,
      os: systemInfo,
      system: {
        manufacturer: systemInfo.manufacturer,
        model: systemInfo.model,
        version: systemInfo.version
      },
      versions: versionsInfo,
      time: systemInfo.uptime
    };
  } catch (error) {
    console.error('Error gathering system data:', error);

    return {
      timestamp: Date.now(),
      cpu: getCpuInfo(),
      memory: getMemoryInfo(),
      error: error.message
    };
  }
}

// Public API: return cached snapshot to callers to avoid heavy work per call
async function getSystemInfo(/* type */) {
  ensureSamplerStarted();
  if (cachedSystemData) return cachedSystemData;
  // If a sample is in-flight, await it; otherwise run one synchronously
  if (inflightSample) return inflightSample;
  inflightSample = sampleSystemInfoOnce();
  try {
    return await inflightSample;
  } finally {
    inflightSample = null;
  }
}

module.exports = {
  gatherSystemData,
  getSystemInfo
};
