const si = require('systeminformation');
const TYPE_ALLOWED = ['sysinfo', 'system'];
const { AuthenticationError } = require('../Helper/errorHandler');
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);

async function getSystemInfo(type) {
  try {
    if (!TYPE_ALLOWED.includes(type)) {
      throw new AuthenticationError('Invalid type', `Invalid type requested: ${type}`);
    }

    // Only do the full data collection for sysinfo type
    if (type === 'system') {
      const system = await si.system();
      return {
        timestamp: Date.now(),
        system: {
          manufacturer: system.manufacturer,
          model: system.model,
          version: system.version
        }
      };
    }

    // For full sysinfo, collect all data
    const [
      system,
      cpu,
      cpuTemp,
      cpuCurrentSpeed,
      cpuFlags,
      mem,
      memLayout,
      osInfo,
      currentLoad,
      fullLoad,
      processes,
      processLoad,
      services,
      disk,
      blockDevices,
      fsSize,
      disksIO,
      networkInterfaces,
      networkStats,
      networkConnections,
      inetLatency,
      users,
      versions,
      time,
      fileSystemStats,
      chassis
    ] = await Promise.all([
      si.system(),
      si.cpu(),
      si.cpuTemperature(),
      si.cpuCurrentSpeed(),
      si.cpuFlags(),
      si.mem(),
      si.memLayout(),
      si.osInfo(),
      si.currentLoad(),
      si.fullLoad(),
      si.processes(),
      si.processLoad('*'),
      si.services('*'),
      si.diskLayout(),
      si.blockDevices(),
      si.fsSize(),
      si.disksIO(),
      si.networkInterfaces(),
      si.networkStats(),
      si.networkConnections(),
      si.inetLatency(),
      si.users(),
      si.versions(),
      si.time(),
      si.fsStats(),
      si.chassis()
    ]);

    // Get additional thermal data (Linux-specific)
    let thermalData = {
      thermalZones: 0,
      fanSpeed: 'N/A',
      zoneTemps: []
    };

    try {
      // Read thermal zone data on Linux systems
      if (osInfo.platform === 'linux') {
        // Get thermal zone count and temperatures
        const { stdout: thermalOutput } = await execPromise('ls -1 /sys/class/thermal/ | grep thermal_zone | wc -l');
        thermalData.thermalZones = parseInt(thermalOutput.trim()) || 0;
        
        if (thermalData.thermalZones > 0) {
          const { stdout: tempOutput } = await execPromise('cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null || echo "N/A"');
          thermalData.zoneTemps = tempOutput.trim().split('\n')
            .map(t => parseInt(t) / 1000)
            .filter(t => !isNaN(t));
        }
        
        // Try to get fan speed - this is very hardware dependent
        try {
          const { stdout: fanOutput } = await execPromise('cat /sys/class/hwmon/*/fan*_input 2>/dev/null || echo "N/A"');
          if (fanOutput && fanOutput.trim() !== 'N/A') {
            const fanSpeed = parseInt(fanOutput.trim());
            thermalData.fanSpeed = isNaN(fanSpeed) ? 'N/A' : fanSpeed + ' RPM';
          } else {
            // Alternative method via lm-sensors if installed
            const { stdout: sensorsOutput } = await execPromise('sensors 2>/dev/null | grep fan | head -n 1 || echo "N/A"');
            if (sensorsOutput && sensorsOutput.trim() !== 'N/A') {
              const match = sensorsOutput.match(/(\d+)\s*RPM/);
              thermalData.fanSpeed = match ? match[1] + ' RPM' : 'N/A';
            }
          }
        } catch (fanErr) {
          // Fan speed reading failed, keep default 'N/A'
        }
      } else if (osInfo.platform === 'darwin') {
        // macOS-specific temperature monitoring
        try {
          const { stdout: smmOutput } = await execPromise('sudo smckit -k "TC0P" 2>/dev/null || echo "N/A"');
          if (smmOutput && smmOutput.trim() !== 'N/A') {
            const match = smmOutput.match(/(\d+\.\d+)/);
            if (match) {
              cpuTemp.main = parseFloat(match[1]);
            }
          }
        } catch (macTempErr) {
          // macOS temp reading failed, keep values from si.cpuTemperature()
        }
      } else if (osInfo.platform === 'win32') {
        // Windows-specific monitoring could be added here
        // Most data should already be available through si.cpuTemperature()
      }
    } catch (thermalErr) {
      console.error('Error getting thermal data:', thermalErr);
      // Keep default values
    }

    // Calculate CPU usage and load
    const cpuUsage = currentLoad.currentLoad;
    const cpuLoadPerCore = currentLoad.cpus.map(cpu => cpu.load);
    
    // Get CPU temperature - if main is null, try to use average of cores or thermal zone data
    let cpuTemperature = cpuTemp.main;
    if (cpuTemperature === null || cpuTemperature === 0) {
      if (cpuTemp.cores && cpuTemp.cores.length > 0) {
        // Try to use average of core temperatures
        const validCoreTemps = cpuTemp.cores.filter(t => t !== null && t > 0);
        if (validCoreTemps.length > 0) {
          cpuTemperature = validCoreTemps.reduce((sum, t) => sum + t, 0) / validCoreTemps.length;
        }
      }
      
      // If still null, try thermal zone temps
      if ((cpuTemperature === null || cpuTemperature === 0) && thermalData.zoneTemps.length > 0) {
        cpuTemperature = thermalData.zoneTemps[0]; // Use first thermal zone as CPU temp
      }
    }

    // Get CPU frequency
    let cpuFrequency = cpuCurrentSpeed.avg;
    if (cpuFrequency === 0 || cpuFrequency === null) {
      // If avg is not available, try to use the max of core speeds
      if (cpuCurrentSpeed.cores && cpuCurrentSpeed.cores.length > 0) {
        const validCoreSpeeds = cpuCurrentSpeed.cores.filter(s => s !== null && s > 0);
        if (validCoreSpeeds.length > 0) {
          cpuFrequency = Math.max(...validCoreSpeeds);
        }
      }
      
      // If still not available, try to get it from CPU model string
      if ((cpuFrequency === 0 || cpuFrequency === null) && cpu.brand) {
        const matchFreq = cpu.brand.match(/(\d+\.\d+)GHz/);
        if (matchFreq) {
          cpuFrequency = parseFloat(matchFreq[1]);
        }
      }
    }

    // Get process information
    const topProcesses = processes.list
      .sort((a, b) => b.cpu - a.cpu)
      .slice(0, 10)
      .map(p => ({
        pid: p.pid,
        user: p.user || 'unknown',
        name: p.name,
        cpu: p.cpu,
        mem: p.mem,
        vsz: p.memVsz || 0,
        rss: p.memRss || 0,
        tty: p.tty || '',
        stat: p.state || '',
        start: p.started || '',
        time: p.time || '',
        command: p.command || p.name
      }));

    // Get service information
    const allServices = services.map(s => ({
      name: s.name,
      running: s.running,
      cpu: s.cpu !== undefined ? s.cpu : null,
      mem: s.mem !== undefined ? s.mem : null
    }));

    // Format disk IO stats
    const diskIOStats = Object.keys(disksIO.rIO_sec || {}).map(device => {
      const readRate = disksIO.rIO_sec[device] || 0;
      const writeRate = disksIO.wIO_sec[device] || 0;
      const readBytes = disksIO.rBytes_sec[device] || 0;
      const writeBytes = disksIO.wBytes_sec[device] || 0;

      return {
        device,
        reads_completed: disksIO.rIO[device] || 0,
        writes_completed: disksIO.wIO[device] || 0,
        read_rate_bytes_per_sec: readBytes,
        write_rate_bytes_per_sec: writeBytes,
        read_rate_mb_per_sec: readBytes / (1024 * 1024),
        write_rate_mb_per_sec: writeBytes / (1024 * 1024),
        io_rate: readRate + writeRate
      };
    });

    // Format detailed storage information
    const detailedStorage = fsSize.map(fs => {
      return {
        fs: fs.fs,
        type: fs.type,
        size: fs.size,
        used: fs.used,
        available: fs.size - fs.used,
        use: fs.use,
        mount: fs.mount
      };
    });

    // Format network interface statistics
    const formattedNetworkStats = networkStats.map(intf => {
      return {
        interface: intf.iface,
        rx_bytes: intf.rx_bytes,
        rx_packets: intf.rx_packets,
        rx_errors: intf.rx_errors,
        rx_dropped: intf.rx_dropped,
        tx_bytes: intf.tx_bytes,
        tx_packets: intf.tx_packets,
        tx_errors: intf.tx_errors,
        tx_dropped: intf.tx_dropped,
        rx_bytes_per_sec: intf.rx_sec,
        tx_bytes_per_sec: intf.tx_sec,
        rx_mbps: (intf.rx_sec * 8) / 1000000,
        tx_mbps: (intf.tx_sec * 8) / 1000000
      };
    });

    // Calculate swap information
    const swapTotal = mem.swaptotal || 0;
    const swapUsed = mem.swapused || 0;
    const swapFree = mem.swapfree || 0;
    const swapPercentage = swapTotal > 0 ? (swapUsed / swapTotal) * 100 : 0;

    // Get file descriptor information
    const fileDescriptors = {
      open_files: fileSystemStats.openFiles,
      max_files: fileSystemStats.maxFiles,
      file_usage_percent: fileSystemStats.maxFiles > 0 
        ? (fileSystemStats.openFiles / fileSystemStats.maxFiles * 100).toFixed(2)
        : 0
    };

    // Format and return the complete system data
    return {
      timestamp: Date.now(),
      cpu: {
        manufacturer: cpu.manufacturer,
        brand: cpu.brand,
        model: cpu.brand,
        cores: cpu.cores,
        physicalCores: cpu.physicalCores,
        speed: cpuFrequency,
        usage: cpuUsage,
        load_1min: currentLoad.avgLoad,
        load_5min: currentLoad.avgLoad5min || 0,
        load_15min: currentLoad.avgLoad15min || 0,
        temperature: cpuTemperature,
        frequency: cpuFrequency,
        coreLoad: cpuLoadPerCore,
        flags: cpuFlags
      },
      memory: {
        total: mem.total,
        free: mem.free,
        used: mem.used,
        active: mem.active,
        available: mem.available,
        usage: (mem.used / mem.total) * 100,
        buffers: mem.buffers || 0,
        cached: mem.cached || 0,
        swap_total: swapTotal,
        swap_free: swapFree,
        swap_used: swapUsed,
        swap_usage: swapPercentage
      },
      disk: {
        total: fsSize.reduce((total, fs) => total + fs.size, 0),
        free: fsSize.reduce((total, fs) => total + (fs.size - fs.used), 0),
        used: fsSize.reduce((total, fs) => total + fs.used, 0),
        usage: fsSize.length > 0 
          ? (fsSize.reduce((total, fs) => total + fs.used, 0) / fsSize.reduce((total, fs) => total + fs.size, 0)) * 100 
          : 0
      },
      disk_io: diskIOStats,
      storage: detailedStorage,
      network: formattedNetworkStats,
      processes: {
        all: processes.all,
        running: processes.running,
        blocked: processes.blocked,
        sleeping: processes.sleeping,
        stopped: processes.stopped || 0,
        zombie: processes.zombie || 0,
        unknown: processes.unknown || 0,
        top: topProcesses
      },
      users: users,
      services: allServices,
      os: {
        hostname: osInfo.hostname,
        platform: osInfo.platform,
        distro: osInfo.distro,
        release: osInfo.release,
        codename: osInfo.codename,
        kernel: osInfo.kernel,
        arch: osInfo.arch,
        uptime: time.uptime,
        loadavg: [currentLoad.avgLoad, currentLoad.avgLoad5min || 0, currentLoad.avgLoad15min || 0],
        totalmem: mem.total,
        freemem: mem.free,
        ...fileDescriptors,
        manufacturer: system.manufacturer,
        model: system.model,
        version: system.version
      },
      system: {
        manufacturer: system.manufacturer,
        model: system.model,
        version: system.version
      },
      versions: {
        kernel: versions.kernel,
        openssl: versions.openssl,
        node: versions.node,
        npm: versions.npm,
        php: versions.php,
        mysql: versions.mysql,
        apache: versions.apache,
        nginx: versions.nginx,
        yarn: versions.yarn,
        pm2: versions.pm2,
        docker: versions.docker,
        redis: versions.redis,
        postgresql: versions.postgresql,
      },
      time: time.uptime,
      thermal: {
        cpuTemperature: cpuTemperature,
        cpuFrequency: cpuFrequency,
        fanSpeed: thermalData.fanSpeed,
        thermalZones: thermalData.thermalZones,
        zoneTemperatures: thermalData.zoneTemps
      }
    };
  } catch (error) {
    console.error('Error gathering system data:', error);
    throw error;
  }
}

module.exports = {
  getSystemInfo
};