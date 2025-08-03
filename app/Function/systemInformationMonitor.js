const os = require('os');
const { exec } = require('child_process');
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


function getDetailedCpuInfo() {
  return new Promise((resolve) => {
    exec('cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null || echo "N/A"', (error, stdout) => {
      const temps = stdout.trim().split('\n').map(t => parseInt(t) / 1000).filter(t => !isNaN(t));
      const avgTemp = temps.length ? temps.reduce((a, b) => a + b) / temps.length : null;

      exec('cat /proc/cpuinfo | grep "cpu MHz"', (err, freqOut) => {
        const frequencies = freqOut.trim().split('\n').map(line => {
          const match = line.match(/([0-9.]+)$/);
          return match ? parseFloat(match[1]) : null;
        }).filter(f => f !== null);

        const avgFreq = frequencies.length ?
          frequencies.reduce((a, b) => a + b) / frequencies.length : null;

        exec('mpstat -P ALL 1 1 | grep -v CPU | grep -v Average', (err2, coreOut) => {
          let coreLoad = [];

          if (!err2) {
            const lines = coreOut.trim().split('\n');
            coreLoad = lines.map(line => {
              const parts = line.trim().split(/\s+/);
              return parts.length > 10 ? 100 - parseFloat(parts[parts.length - 1]) : null;
            }).filter(load => load !== null);
          }

          resolve({
            temperature: avgTemp,
            frequency: avgFreq ? (avgFreq / 1000).toFixed(2) : null,
            coreLoad: coreLoad.length ? coreLoad : null
          });
        });
      });
    });
  });
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

function getDetailedMemoryInfo() {
  return new Promise((resolve) => {
    exec('cat /proc/meminfo', (error, stdout) => {
      if (error) {
        console.error(`Error getting detailed memory info: ${error}`);
        resolve({});
        return;
      }

      const memInfo = {};
      const lines = stdout.trim().split('\n');

      lines.forEach(line => {
        const [key, valueWithUnit] = line.split(':');
        const valueMatch = valueWithUnit && valueWithUnit.trim().match(/^(\d+)/);
        if (valueMatch) {
          memInfo[key.trim()] = parseInt(valueMatch[1]) * 1024;
        }
      });

      resolve({
        buffers: memInfo.Buffers || 0,
        cached: memInfo.Cached || 0,
        swap_total: memInfo.SwapTotal || 0,
        swap_free: memInfo.SwapFree || 0,
        swap_used: memInfo.SwapTotal ? (memInfo.SwapTotal - memInfo.SwapFree) : 0,
        swap_usage: memInfo.SwapTotal ? ((memInfo.SwapTotal - memInfo.SwapFree) / memInfo.SwapTotal * 100) : 0
      });
    });
  });
}

// Replace diskusage with df command
async function getDiskInfo() {
  return new Promise((resolve) => {
    exec('df / --output=size,used,avail | tail -n 1', (error, stdout) => {
      if (error) {
        console.error('Error getting disk info:', error);
        resolve({
          total: 0,
          free: 0,
          used: 0,
          usage: 0
        });
        return;
      }

      try {
        const parts = stdout.trim().split(/\s+/);
        if (parts.length >= 3) {
          const total = parseInt(parts[0]) * 1024; // Convert from KB to bytes
          const used = parseInt(parts[1]) * 1024;
          const free = parseInt(parts[2]) * 1024;

          resolve({
            total: total,
            free: free,
            used: used,
            usage: total > 0 ? (used / total) * 100 : 0
          });
        } else {
          resolve({
            total: 0,
            free: 0,
            used: 0,
            usage: 0
          });
        }
      } catch (err) {
        console.error('Error parsing disk info:', err);
        resolve({
          total: 0,
          free: 0,
          used: 0,
          usage: 0
        });
      }
    });
  });
}

async function getDetailedDiskInfo() {
  return new Promise((resolve) => {
    exec('df -h', (error, stdout) => {
      if (error) {
        console.error(`Error getting detailed disk info: ${error}`);
        resolve([]);
        return;
      }

      try {
        const lines = stdout.trim().split('\n').slice(1);
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

          return {
            fs: filesystem,
            size: convertToBytes(size),
            used: convertToBytes(used),
            available: convertToBytes(available),
            use,
            mount
          };
        });

        resolve(disks);
      } catch (err) {
        console.error('Error parsing disk info:', err);
        resolve([]);
      }
    });
  });
}

function getDiskIOStats() {
  return new Promise((resolve) => {
    exec('cat /proc/diskstats', (error, stdout) => {
      if (error) {
        console.error(`Error getting disk I/O stats: ${error}`);
        resolve([]);
        return;
      }

      try {
        const lines = stdout.trim().split('\n');
        const currentTime = Date.now();
        const timeDiff = (currentTime - previousTimeStamp) / 1000;
        const diskStats = [];

        lines.forEach(line => {
          const parts = line.trim().split(/\s+/);
          if (parts.length >= 14) {
            const deviceName = parts[2];
            if (!deviceName.startsWith('loop') &&
              !deviceName.startsWith('ram') &&
              !deviceName.startsWith('dm-')) {

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

              if (previousDiskStats) {
                const prevDeviceStats = previousDiskStats.find(d => d.device === deviceName);

                if (prevDeviceStats && timeDiff > 0) {
                  const sectorSize = 512;

                  currentStats.read_rate_bytes_per_sec = ((currentStats.sectors_read - prevDeviceStats.sectors_read) * sectorSize) / timeDiff;
                  currentStats.write_rate_bytes_per_sec = ((currentStats.sectors_written - prevDeviceStats.sectors_written) * sectorSize) / timeDiff;

                  currentStats.read_rate_mb_per_sec = currentStats.read_rate_bytes_per_sec / (1024 * 1024);
                  currentStats.write_rate_mb_per_sec = currentStats.write_rate_bytes_per_sec / (1024 * 1024);

                  currentStats.io_rate = (
                    (currentStats.reads_completed - prevDeviceStats.reads_completed) +
                    (currentStats.writes_completed - prevDeviceStats.writes_completed)
                  ) / timeDiff;
                }
              }

              diskStats.push(currentStats);
            }
          }
        });

        previousDiskStats = diskStats;
        previousTimeStamp = currentTime;

        resolve(diskStats);
      } catch (err) {
        console.error('Error parsing disk I/O stats:', err);
        resolve([]);
      }
    });
  });
}

function getNetworkInfo() {
  return new Promise((resolve) => {
    exec('cat /proc/net/dev', (error, stdout) => {
      if (error) {
        console.error(`Error getting network stats: ${error}`);
        resolve([]);
        return;
      }

      try {
        const lines = stdout.trim().split('\n').slice(2);
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

                currentStats.rx_mbps = currentStats.rx_bytes_per_sec * 8 / 1000000; // Convert to Mbps
                currentStats.tx_mbps = currentStats.tx_bytes_per_sec * 8 / 1000000; // Convert to Mbps
              }
            }

            previousNetworkData[interfaceName] = {
              rx_bytes: currentStats.rx_bytes,
              tx_bytes: currentStats.tx_bytes,
              timestamp: currentTime
            };

            interfaces.push(currentStats);
          }
        });

        resolve(interfaces);
      } catch (err) {
        console.error('Error parsing network stats:', err);
        resolve([]);
      }
    });
  });
}

function getProcessInfo() {
  return new Promise((resolve) => {
    exec('ps -eo state --no-header | sort | uniq -c', (error, stdout) => {
      let processSummary = {
        all: 0,
        running: 0,
        sleeping: 0,
        stopped: 0,
        zombie: 0,
        other: 0
      };

      if (!error) {
        stdout.trim().split('\n').forEach(line => {
          const [count, state] = line.trim().split(/\s+/);
          const countNum = parseInt(count);

          processSummary.all += countNum;

          switch (state) {
            case 'R':
              processSummary.running += countNum;
              break;
            case 'S':
            case 'I':
              processSummary.sleeping += countNum;
              break;
            case 'T':
              processSummary.stopped += countNum;
              break;
            case 'Z':
              processSummary.zombie += countNum;
              break;
            case 'D':
              processSummary.blocked = (processSummary.blocked || 0) + countNum;
              break;
            default:
              processSummary.other += countNum;
          }
        });
      }

      exec('ps aux --sort=-%cpu | head -n 11', (error2, stdout2) => {
        let topProcesses = [];

        if (!error2) {
          try {
            const lines = stdout2.trim().split('\n').slice(1);
            topProcesses = lines.map(line => {
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
              const all = processSummary.all;

              return {
                user,
                pid,
                cpu,
                mem,
                vsz,
                rss,
                tty,
                stat,
                start,
                time,
                command: command.length > 50 ? command.substring(0, 47) + '...' : command,
                all,
              };
            });
          } catch (err) {
            console.error('Error parsing process data:', err);
          }
        }

        resolve({
          ...processSummary,
          top: topProcesses
        });
      });
    });
  });
}

function getUserInfo() {
  return new Promise((resolve) => {
    exec('who', (error, stdout) => {
      if (error) {
        console.error(`Error getting user info: ${error}`);
        resolve([]);
        return;
      }

      try {
        const lines = stdout.trim().split('\n');
        const users = lines.filter(line => line.trim() !== '').map(line => {
          const parts = line.trim().split(/\s+/);
          let user, tty, date, time, from;

          if (parts.length >= 5) {
            user = parts[0];
            tty = parts[1];
            date = parts[2];
            time = parts[3];
            from = parts[4].replace(/[\(\)]/g, '');
          } else {
            user = parts[0] || 'unknown';
            tty = parts[1] || 'unknown';
            date = parts[2] || '';
            time = parts[3] || '';
            from = 'local';
          }

          return { user, tty, date, time, ip: from !== 'local' ? from : null };
        });

        resolve(users);
      } catch (err) {
        console.error('Error parsing user data:', err);
        resolve([]);
      }
    });
  });
}

function getServicesInfo() {
  return new Promise((resolve) => {
    exec('systemctl list-units --type=service --state=active,running,failed --no-legend 2>/dev/null || echo ""',
      (error, stdout) => {
        if (error || !stdout.trim()) {
          exec('service --status-all 2>&1', (err2, stdout2) => {
            if (err2) {
              console.error(`Error getting services info: ${err2}`);
              resolve([]);
              return;
            }

            try {
              const lines = stdout2.trim().split('\n');
              const services = lines.map(line => {
                const match = line.trim().match(/\[\s*([+-?])\s*\]\s+(.+)$/);
                if (!match) return null;

                const status = match[1];
                const name = match[2];

                return {
                  name,
                  running: status === '+',
                  cpu: null,
                  mem: null
                };
              }).filter(service => service !== null);

              resolve(services);
            } catch (err) {
              console.error('Error parsing service data:', err);
              resolve([]);
            }
          });
        } else {
          try {
            const lines = stdout.trim().split('\n');
            const services = lines.filter(line => line.trim() !== '').map(line => {
              const parts = line.trim().split(/\s+/);
              let name = '';
              let state = '';

              if (parts.length >= 3) {
                name = parts[0];
                state = parts[2];
              }

              return {
                name,
                running: state === 'running',
                cpu: null,
                mem: null
              };
            });

            if (services.length > 0) {
              const serviceNames = services.map(s => s.name.replace('.service', '')).join('|');

              exec(`ps -eo pid,pmem,pcpu,comm | grep -E '(${serviceNames})' | grep -v grep`,
                (psErr, psOut) => {
                  if (!psErr && psOut.trim()) {
                    const psLines = psOut.trim().split('\n');

                    psLines.forEach(psLine => {
                      const [pid, mem, cpu, comm] = psLine.trim().split(/\s+/);

                      const service = services.find(s =>
                        comm.includes(s.name.replace('.service', ''))
                      );

                      if (service) {
                        service.cpu = parseFloat(cpu);
                        service.mem = parseFloat(mem);
                      }
                    });
                  }

                  resolve(services);
                }
              );
            } else {
              resolve(services);
            }
          } catch (err) {
            console.error('Error parsing systemd service data:', err);
            resolve([]);
          }
        }
      }
    );
  });
}

function getSystemInfo() {
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

function getVersionsInfo() {
  return new Promise((resolve) => {
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
    const promises = [];

    for (const [key, cmd] of Object.entries(commands)) {
      promises.push(
        new Promise((resolve) => {
          exec(cmd, (err, out) => {
            let version = err ? 'N/A' : out.trim();

            if (key === 'openssl') {
              const match = version.match(/OpenSSL\s+(\S+)/i);
              version = match ? match[1] : version;
              if (version.includes('quic')) {
                const parts = version.split('+');
                if (parts.length > 1) {
                  version = parts[0] + '+quic';
                }
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

            if (version !== 'N/A') {
              versions[key] = version;
            }
            resolve();
          });
        })
      );
    }

    Promise.all(promises).then(() => resolve(versions));
  });
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
      getSystemInfo(),
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

module.exports = {
  gatherSystemData,
  getSystemInfo: gatherSystemData
};