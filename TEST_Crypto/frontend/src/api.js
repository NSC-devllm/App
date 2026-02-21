import axios from 'axios';

const api = axios.create({
    baseURL: '/api', // Proxy will handle this in Vite config or explicit URL
    headers: {
        'Content-Type': 'application/json',
    },
});

export default api;
