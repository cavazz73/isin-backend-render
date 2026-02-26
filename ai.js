/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * AI Financial Assistant - Powered by Claude (Anthropic)
 */

const express = require('express');
const router = express.Router();
const Anthropic = require('@anthropic-ai/sdk');
const multer = require('multer');
const pdfParse = require('pdf-parse');

// ===================================
// CONFIGURATION
// ===================================

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY not set!');
}

const client = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

// Simple rate limiting
const rateLimits = new Map();
function checkRateLimit(ip) {
    const now = Date.now();
    const key = ip || 'unknown';
    if (!rateLimits.has(key)) {
        rateLimits.set(key, { count: 1, resetAt: now + 60000 });
        return true;
    }
    const limit = rateLimits.get(key);
    if (now > limit.resetAt) {
        rateLimits.set(key, { count: 1, resetAt: now + 60000 });
        return true;
    }
    if (limit.count >= 15) return false;
    limit.count++;
    return true;
}
setInterval(() => {
    const now = Date.now();
    for (const [key, val] of rateLimits) {
        if (now > val.resetAt) rateLimits.delete(key);
    }
}, 300000);

// ===================================
// SYSTEM PROMPT
// ===================================

const SYSTEM_PROMPT = `Sei un consulente finanziario AI esperto con accesso a ricerca web in tempo reale, integrato nella piattaforma ISIN Research & Compare di Mutna S.R.L.S.

## Ruolo
Analista finanziario senior specializzato in mercati europei e italiani. Rispondi in italiano. Usa la ricerca web per dati aggiornati quando utile.

## Competenze
Azioni, obbligazioni, certificati (Cash Collect, Phoenix, Bonus Cap, Express), ETF, fondi, mercati (FTSE MIB, DAX, S&P500), tassazione italiana (26% capital gain, 12.5% titoli di stato).

## Regole sui Dati
- Usa i dati dalla piattaforma come base
- Usa web search per integrare dati mancanti, notizie recenti, target price analisti
- NON inventare numeri. Se non trovi un dato, dillo.
- 52W Range ≠ performance YTD
- Cita le fonti web quando le usi

## Formato
- NO disclaimer (c'è già nella piattaforma)
- Markdown per formattare, tabelle per confronti
- Conciso e diretto
- Presenta pro e contro, mai consigliare comprare/vendere`;

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
    if (ext === 'pdf') {
        const data = await pdfParse(file.buffer);
        return data.text;
    }
    if (ext === 'docx') {
        const AdmZip = require('adm-zip');
        const zip = new AdmZip(file.buffer);
        const content = zip.readAsText('word/document.xml');
        return content.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    }
    throw new Error('Formato non supportato');
}

// ===================================
// BUILD MESSAGES
// ===================================

function buildMessages(userMessage, options = {}) {
    const { history = [], documentText = null, instrumentData = null } = options;
    const messages = [];
    
    for (const msg of history.slice(-20)) {
        if (msg.role === 'user' || msg.role === 'assistant') {
            messages.push({ role: msg.role, content: msg.content });
        }
    }
    
    let content = '';
    
    if (instrumentData) {
        const cleanData = {};
        for (const [key, value] of Object.entries(instrumentData)) {
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') {
                cleanData[key] = value;
            }
        }
        content += `[DATI STRUMENTO DALLA PIATTAFORMA]\n${JSON.stringify(cleanData, null, 2)}\n[FINE DATI - cerca sul web dati mancanti]\n\n`;
    }
    
    if (documentText) {
        const truncated = documentText.substring(0, 30000);
        content += `[DOCUMENTO CARICATO]\n${truncated}`;
        if (documentText.length > 30000) content += `\n... (troncato, ${documentText.length} char totali)`;
        content += `\n[FINE DOCUMENTO]\n\n`;
    }
    
    content += userMessage;
    messages.push({ role: 'user', content });
    return messages;
}

// ===================================
// POST /api/ai/chat - Streaming SSE + Web Search
// ===================================

router.post('/chat', async (req, res) => {
    if (!client) {
        return res.status(503).json({ success: false, error: 'AI not configured.' });
    }
    if (!checkRateLimit(req.ip)) {
        return res.status(429).json({ success: false, error: 'Troppi messaggi. Riprova tra un minuto.' });
    }
    
    const { message, history, documentText, instrumentData } = req.body;
    
    if (!message || typeof message !== 'string' || !message.trim()) {
        return res.status(400).json({ success: false, error: 'Message is required' });
    }
    if (message.length > 5000) {
        return res.status(400).json({ success: false, error: 'Messaggio troppo lungo (max 5000).' });
    }
    
    // SSE headers
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    
    let aborted = false;
    req.on('close', () => { aborted = true; });
    
    try {
        const messages = buildMessages(message.trim(), {
            history: history || [],
            documentText: documentText || null,
            instrumentData: instrumentData || null
        });
        
        res.write(`data: ${JSON.stringify({ type: 'status', status: 'thinking' })}\n\n`);
        
        // Use streaming with web search tool
        // web_search_20250305 is a server-side tool - the API handles searches internally
        const stream = client.messages.stream({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 4096,
            system: SYSTEM_PROMPT,
            tools: [{
                type: "web_search_20250305",
                name: "web_search",
                max_uses: 3
            }],
            messages: messages
        });
        
        let fullText = '';
        let searchNotified = false;
        
        stream.on('event', (event) => {
            if (aborted) return;
            
            // Detect when web search is happening
            if (event.type === 'content_block_start' && 
                event.content_block && 
                (event.content_block.type === 'web_search_tool_use' || event.content_block.type === 'tool_use')) {
                if (!searchNotified) {
                    res.write(`data: ${JSON.stringify({ type: 'status', status: 'searching' })}\n\n`);
                    searchNotified = true;
                }
            }
        });
        
        stream.on('text', (text) => {
            if (aborted) return;
            fullText += text;
            res.write(`data: ${JSON.stringify({ type: 'text', text })}\n\n`);
        });
        
        stream.on('end', () => {
            if (aborted) return;
            res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
            res.end();
        });
        
        stream.on('error', (error) => {
            console.error('Stream error:', error);
            if (aborted) return;
            
            // If web search fails, retry without it
            retryWithoutSearch(client, messages, res, aborted);
        });
        
    } catch (error) {
        console.error('AI chat error:', error);
        if (!aborted && !res.headersSent) {
            res.status(500).json({ success: false, error: 'Errore AI.' });
        } else if (!aborted) {
            res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore nella risposta.' })}\n\n`);
            res.end();
        }
    }
});

// Fallback without web search
async function retryWithoutSearch(client, messages, res, aborted) {
    try {
        console.log('Retrying without web search...');
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
            if (!aborted) {
                res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                res.end();
            }
        });
        stream.on('error', () => {
            if (!aborted) {
                res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore nella risposta.' })}\n\n`);
                res.end();
            }
        });
    } catch (e) {
        if (!aborted) {
            res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore.' })}\n\n`);
            res.end();
        }
    }
}

// ===================================
// POST /api/ai/upload
// ===================================

router.post('/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ success: false, error: 'Nessun file' });
        const text = await extractText(req.file);
        res.json({
            success: true,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            textLength: text.length,
            text: text,
            preview: text.substring(0, 500)
        });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// ===================================
// POST /api/ai/analyze
// ===================================

router.post('/analyze', async (req, res) => {
    if (!client) return res.status(503).json({ success: false, error: 'AI not configured.' });
    if (!checkRateLimit(req.ip)) return res.status(429).json({ success: false, error: 'Rate limit.' });
    
    const { instrumentData } = req.body;
    if (!instrumentData) return res.status(400).json({ success: false, error: 'instrumentData required' });
    
    // Redirect to chat endpoint with analysis prompt
    req.body.message = 'Analizza questo strumento. Cerca notizie recenti e target price sul web.';
    req.body.history = [];
    
    // Forward to chat handler
    return router.handle(Object.assign(req, { url: '/chat', method: 'POST' }), res);
});

// ===================================
// GET /api/ai/status
// ===================================

router.get('/status', (req, res) => {
    res.json({
        success: true,
        configured: !!client,
        model: 'claude-sonnet-4-20250514',
        features: ['chat', 'streaming', 'web-search', 'document-upload', 'instrument-analysis']
    });
});

module.exports = router;
