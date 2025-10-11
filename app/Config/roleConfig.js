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
        '/call-center/chat': [1, 2, 3, 4],
        '/kajian-presence': [7],
        '/kajian-presence/{rppId}': [7],
        '/penetapan-presence': [7],
        '/penetapan-presence/{rppId}': [7],
        '/editor': [1, 2, 3, 4], // Editor route for all authenticated users
    },
    publicRoutes: [
        // Define websocket paths that should skip role enforcement entirely.
        // Supports exact paths (`/call-center/public`) or prefix matches using a trailing wildcard (`/widget/public/*`).
    ],
};
