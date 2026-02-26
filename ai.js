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
if (!ANTHROPIC_API_KEY) console.error('❌ ANTHROPIC_API_KEY not set!');
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

## Ruolo
Analista finanziario senior specializzato in mercati europei e italiani. Rispondi sempre in italiano.

## Competenze
Azioni, obbligazioni, certificati (Cash Collect, Phoenix, Bonus Cap, Express), ETF, fondi, mercati (FTSE MIB, DAX, S&P500), tassazione italiana (26% capital gain, 12.5% titoli di stato), normativa KIID/KID.

## Regole sui Dati
- Usa i dati dalla piattaforma come base per l'analisi
- NON inventare numeri. Se un dato manca, dillo chiaramente.
- 52W Range NON e' la performance YTD. Non calcolare performance da dati che non hai.
- Distingui tra dati forniti e tue considerazioni generali

## Formato
- NO disclaimer (c'e' gia' fisso nella piattaforma, non ripeterlo MAI)
- Markdown per formattare, tabelle per confronti
- Conciso e diretto, max 300-400 parole per analisi
- Presenta pro e contro, mai consigliare comprare/vendere`;

const SYSTEM_PROMPT_WITH_SEARCH = SYSTEM_PROMPT + `

## Web Search
Hai accesso alla ricerca web. Usala per trovare notizie recenti, target price, rating analisti, risultati trimestrali. Cita le fonti.`;

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
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') cleanData[key] = value;
        }
        content += `[DATI STRUMENTO DALLA PIATTAFORMA]\n${JSON.stringify(cleanData, null, 2)}\n[FINE DATI]\n\n`;
    }
    if (documentText) {
        content += `[DOCUMENTO CARICATO]\n${documentText.substring(0, 30000)}\n[FINE DOCUMENTO]\n\n`;
    }
    content += userMessage;
    messages.push({ role: 'user', content });
    return messages;
}

// ===================================
// Detect if user wants web search
// ===================================

function needsWebSearch(message) {
    const triggers = [
        'notizie', 'news', 'ultime', 'oggi', 'ieri', 'cerca online',
        'cerca sul web', 'aggiornamenti', 'target price', 'analisti',
        'previsioni', 'forecast', 'trimestrale', 'earnings',
        'cosa e successo', 'perche sale', 'perche scende',
        'cerca notizie', 'ricerca web', 'cerca info'
    ];
    const lower = message.toLowerCase();
    return triggers.some(t => lower.includes(t));
}

// ===================================
// POST /api/ai/chat - FAST streaming, web search only on demand
// ===================================

router.post('/chat', async (req, res) => {
    if (!client) return res.status(503).json({ success: false, error: 'AI not configured.' });
    if (!checkRateLimit(req.ip)) return res.status(429).json({ success: false, error: 'Troppi messaggi. Riprova tra un minuto.' });
    
    const { message, history, documentText, instrumentData } = req.body;
    if (!message || typeof message !== 'string' || !message.trim()) return res.status(400).json({ success: false, error: 'Message is required' });
    if (message.length > 5000) return res.status(400).json({ success: false, error: 'Messaggio troppo lungo (max 5000).' });
    
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
        
        const useSearch = needsWebSearch(message);
        res.write(`data: ${JSON.stringify({ type: 'status', status: useSearch ? 'searching' : 'thinking' })}\n\n`);
        
        const streamOpts = {
            model: 'claude-sonnet-4-20250514',
            max_tokens: useSearch ? 4096 : 2048,
            system: useSearch ? SYSTEM_PROMPT_WITH_SEARCH : SYSTEM_PROMPT,
            messages: messages
        };
        
        if (useSearch) {
            streamOpts.tools = [{ type: "web_search_20250305", name: "web_search", max_uses: 3 }];
        }
        
        const stream = client.messages.stream(streamOpts);
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
        
        stream.on('error', (error) => {
            console.error('Stream error:', error);
            if (aborted) return;
            if (useSearch) {
                // Retry without search
                const retry = client.messages.stream({ model: 'claude-sonnet-4-20250514', max_tokens: 2048, system: SYSTEM_PROMPT, messages });
                let rt = '';
                retry.on('text', (t) => { if (!aborted) { rt += t; res.write(`data: ${JSON.stringify({ type: 'text', text: t })}\n\n`); }});
                retry.on('end', () => { if (!aborted) { res.write(`data: ${JSON.stringify({ type: 'done', fullText: rt })}\n\n`); res.end(); }});
                retry.on('error', () => { if (!aborted) { res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore.' })}\n\n`); res.end(); }});
            } else {
                res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore nella risposta.' })}\n\n`);
                res.end();
            }
        });
        
    } catch (error) {
        console.error('AI error:', error);
        if (!aborted && !res.headersSent) res.status(500).json({ success: false, error: 'Errore AI.' });
        else if (!aborted) { res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore.' })}\n\n`); res.end(); }
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
// GET /api/ai/status
// ===================================

router.get('/status', (req, res) => {
    res.json({ success: true, configured: !!client, model: 'claude-sonnet-4-20250514', features: ['chat', 'streaming', 'web-search-on-demand', 'document-upload'] });
});

module.exports = router;
