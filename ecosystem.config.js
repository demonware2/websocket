module.exports = {
  apps: [{
    name: "websocket",
    script: 'server.js',
    watch: false,
    max_restarts: 3,
    restart_delay: 5000,
    env: {
      PORT: 9950,
      NODE_ENV: "development"
    },
    env_production: {
      PORT: 9950,
      NODE_ENV: "production"
    }
  }],
  deploy: {
    production: {
      user: 'SSH_USERNAME',
      host: 'SSH_HOSTMACHINE',
      ref: 'origin/master',
      repo: 'GIT_REPOSITORY',
      path: 'DESTINATION_PATH',
      'pre-deploy-local': '',
      'post-deploy': 'npm install && pm2 reload ecosystem.config.js --env production',
      'pre-setup': ''
    }
  }
};
