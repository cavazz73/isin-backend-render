/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * AI Financial Assistant - Powered by Claude (Anthropic)
 */

const express = require('express');
const router = express.Router();
const Anthropic = require('@anthropic-ai/sdk');
const multer = require('multer');
const pdfParse = require('pdf-parse');

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_API_KEY) console.error('[AI] ANTHROPIC_API_KEY not set!');
const client = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

// Rate limiting
const rateLimits = new Map();
function checkRateLimit(ip) {
    const now = Date.now();
    const key = ip || 'unknown';
    if (!rateLimits.has(key)) { rateLimits.set(key, { count: 1, resetAt: now + 60000 }); return true; }
    const limit = rateLimits.get(key);
    if (now > limit.resetAt) { rateLimits.set(key, { count: 1, resetAt: now + 60000 }); return true; }
    if (limit.count >= 15) return false;
    limit.count++;
    return true;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of rateLimits) { if (now > v.resetAt) rateLimits.delete(k); } }, 300000);

// ===================================
// SYSTEM PROMPT
// ===================================

const SYSTEM_PROMPT = `Sei un consulente finanziario AI esperto, integrato nella piattaforma ISIN Research & Compare di Mutna S.R.L.S.

Analista finanziario senior specializzato in mercati europei e italiani. Rispondi sempre in italiano.

Competenze: azioni, obbligazioni, certificati (Cash Collect, Phoenix, Bonus Cap, Express), ETF, fondi, mercati (FTSE MIB, DAX, S&P500), tassazione italiana (26% capital gain, 12.5% titoli di stato), normativa KIID/KID.

Regole:
- Usa i dati dalla piattaforma come base. NON inventare numeri.
- 52W Range NON e la performance YTD. Non calcolare dati che non hai.
- Distingui tra dati forniti e tue considerazioni generali.
- NO disclaimer nelle risposte (ce gia fisso nella piattaforma).
- Markdown per formattare, tabelle per confronti.
- Conciso e diretto, max 300-400 parole.
- Presenta pro e contro, mai consigliare comprare/vendere.`;

// ===================================
// FILE UPLOAD
// ===================================

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 },
    fileFilter: (req, file, cb) => {
        const ext = file.originalname.split('.').pop().toLowerCase();
        cb(null, ['pdf', 'txt', 'csv', 'doc', 'docx'].includes(ext));
    }
});

async function extractText(file) {
    const ext = file.originalname.split('.').pop().toLowerCase();
    if (ext === 'txt' || ext === 'csv') return file.buffer.toString('utf-8');
    if (ext === 'pdf') { const data = await pdfParse(file.buffer); return data.text; }
    if (ext === 'docx') {
        const AdmZip = require('adm-zip');
        const zip = new AdmZip(file.buffer);
        return zip.readAsText('word/document.xml').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    }
    throw new Error('Formato non supportato');
}

// ===================================
// POST /api/ai/chat - Pure fast streaming
// ===================================

router.post('/chat', async (req, res) => {
    if (!client) return res.status(503).json({ success: false, error: 'AI not configured.' });
    if (!checkRateLimit(req.ip)) return res.status(429).json({ success: false, error: 'Troppi messaggi. Riprova tra un minuto.' });

    const { message, history, documentText, instrumentData } = req.body;
    if (!message || typeof message !== 'string' || !message.trim()) return res.status(400).json({ success: false, error: 'Message is required' });
    if (message.length > 5000) return res.status(400).json({ success: false, error: 'Messaggio troppo lungo (max 5000).' });

    // Build messages array
    const messages = [];
    if (history && Array.isArray(history)) {
        for (const msg of history.slice(-20)) {
            if (msg.role === 'user' || msg.role === 'assistant') {
                messages.push({ role: msg.role, content: msg.content });
            }
        }
    }

    let content = '';
    if (instrumentData) {
        const cleanData = {};
        for (const [key, value] of Object.entries(instrumentData)) {
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') cleanData[key] = value;
        }
        content += `[DATI STRUMENTO DALLA PIATTAFORMA]\n${JSON.stringify(cleanData, null, 2)}\n[FINE DATI]\n\n`;
    }
    if (documentText) {
        content += `[DOCUMENTO]\n${documentText.substring(0, 30000)}\n[FINE DOCUMENTO]\n\n`;
    }
    content += message.trim();
    messages.push({ role: 'user', content });

    // SSE streaming
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.flushHeaders(); // CRITICAL: force headers to be sent immediately

    // Send initial heartbeat to keep connection alive
    res.write(`data: ${JSON.stringify({ type: 'status', status: 'thinking' })}\n\n`);

    let aborted = false;

    // Keep-alive heartbeat every 15s to prevent Render proxy timeout
    const heartbeat = setInterval(() => {
        if (!aborted) {
            try { res.write(': heartbeat\n\n'); } catch(e) {}
        }
    }, 15000);

    req.on('close', () => { aborted = true; clearInterval(heartbeat); });

    try {
        console.log('[AI] Starting stream, message length:', content.length);
        
        const stream = client.messages.stream({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 2048,
            system: SYSTEM_PROMPT,
            messages: messages
        });

        let fullText = '';

        stream.on('text', (text) => {
            if (aborted) return;
            fullText += text;
            res.write(`data: ${JSON.stringify({ type: 'text', text })}\n\n`);
        });

        stream.on('end', () => {
            clearInterval(heartbeat);
            if (!aborted) {
                res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                res.end();
            }
        });

        stream.on('error', (error) => {
            clearInterval(heartbeat);
            console.error('[AI] Stream error:', error.message, error.status || '');
            if (!aborted) {
                res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore nella risposta AI: ' + (error.message || 'unknown') })}\n\n`);
                res.end();
            }
        });

        // Abort stream if client disconnects
        req.on('close', () => { 
            try { stream.abort(); } catch(e) {} 
        });

    } catch (error) {
        clearInterval(heartbeat);
        console.error('[AI] Error:', error.message);
        if (!aborted) {
            try {
                res.write(`data: ${JSON.stringify({ type: 'error', error: error.message || 'Errore AI.' })}\n\n`);
                res.end();
            } catch(e) { /* response already ended */ }
        }
    }
});

// ===================================
// POST /api/ai/upload
// ===================================

router.post('/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ success: false, error: 'Nessun file' });
        const text = await extractText(req.file);
        res.json({ success: true, fileName: req.file.originalname, fileSize: req.file.size, textLength: text.length, text, preview: text.substring(0, 500) });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

// ===================================
// GET /api/ai/test-stream - Test SSE connection
// ===================================

router.get('/test-stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    
    res.write(`data: ${JSON.stringify({ type: 'text', text: 'Test SSE funziona! ' })}\n\n`);
    
    let count = 0;
    const interval = setInterval(() => {
        count++;
        if (count <= 5) {
            res.write(`data: ${JSON.stringify({ type: 'text', text: `Chunk ${count}... ` })}\n\n`);
        } else {
            clearInterval(interval);
            res.write(`data: ${JSON.stringify({ type: 'done', fullText: 'Test completato!' })}\n\n`);
            res.end();
        }
    }, 500);
    
    req.on('close', () => clearInterval(interval));
});

// ===================================
// GET /api/ai/status
// ===================================

router.get('/status', (req, res) => {
    res.json({ success: true, configured: !!client, model: 'claude-sonnet-4-20250514', features: ['chat', 'streaming', 'document-upload'] });
});

module.exports = router;
