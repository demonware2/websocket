module.exports = {
    routes: {
        '/api/users': [1, 2],
        '/api/users/create': [1],
        '/api/reports': [1, 2, 3],
        '/api/settings': [1],
        '/api/dashboard': [1, 2, 3, 4],
        '/handleSystemInfo': [1, 2, 3, 4],
        '/gatherPM2Data': [1, 2, 3, 4],
        '/handleNetdata': [1, 2, 3, 4],
        '/handleChat': [1, 2, 3, 4],
        '/call-center/admin/broadcast': [1, 64, 65, 66, 67, 68],
        '/kajian-presence': [7],
        '/kajian-presence/{rppId}': [7],
        '/penetapan-presence': [7],
        '/penetapan-presence/{rppId}': [7],
        '/editor': [1, 2, 3, 4],
    },
    publicRoutes: [
        '/call-center/chat',
        '/kiosk',
        '/realtime'
    ],
};
