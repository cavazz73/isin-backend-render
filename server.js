const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

// Log
app.use((req, res, next) => {
    console.log(`${req.method} ${req.path}`);
    next();
});

// Root
app.get('/', (req, res) => {
    res.json({
        success: true,
        message: 'ISIN Research API v2.0',
        status: 'operational',
        timestamp: new Date().toISOString()
    });
});

// Health
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        uptime: process.uptime(),
        timestamp: new Date().toISOString()
    });
});

// Test
app.get('/api/test', (req, res) => {
    res.json({
        success: true,
        message: 'API is working!',
        timestamp: new Date().toISOString()
    });
});

// 404
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Not found'
    });
});

// Error handler
app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error'
    });
});

// Start
const PORT = process.env.PORT || 10000;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Server running on port ${PORT}`);
});