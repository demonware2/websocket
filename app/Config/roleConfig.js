module.exports = {
    routes: {
        '/api/users': [1, 2],
        '/api/users/create': [1],
        '/api/reports': [1, 2, 3],
        '/api/settings': [1],
        '/api/dashboard': [1, 2, 3, 4],
        '/handleSystemInfo': [1, 2, 3, 4],
        '/gatherPM2Data': [1, 2, 3, 4],
        '/handleChat': [1, 2, 3, 4], // Added new route for chat
        '/editor': [1, 2, 3, 4], // Editor route for all authenticated users
    },
};