/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * AI Financial Assistant Module
 * Powered by Claude (Anthropic) - with Web Search (Perplexity-style)
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
    console.error('❌ ANTHROPIC_API_KEY not set! AI module will not work.');
}

const client = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

// Rate limiting
const rateLimits = new Map();
const RATE_LIMIT_WINDOW = 60 * 1000;
const RATE_LIMIT_MAX = 15;

function checkRateLimit(ip) {
    const now = Date.now();
    const key = ip || 'unknown';
    if (!rateLimits.has(key)) {
        rateLimits.set(key, { count: 1, resetAt: now + RATE_LIMIT_WINDOW });
        return true;
    }
    const limit = rateLimits.get(key);
    if (now > limit.resetAt) {
        rateLimits.set(key, { count: 1, resetAt: now + RATE_LIMIT_WINDOW });
        return true;
    }
    if (limit.count >= RATE_LIMIT_MAX) return false;
    limit.count++;
    return true;
}

setInterval(() => {
    const now = Date.now();
    for (const [key, val] of rateLimits) {
        if (now > val.resetAt) rateLimits.delete(key);
    }
}, 5 * 60 * 1000);

// ===================================
// SYSTEM PROMPT - Financial Expert + Web Search
// ===================================

const SYSTEM_PROMPT = `Sei un consulente finanziario AI esperto con accesso a ricerca web in tempo reale, integrato nella piattaforma ISIN Research & Compare di Mutna S.R.L.S.

## Il Tuo Ruolo
Sei un analista finanziario senior specializzato nei mercati europei e italiani, con capacità di ricerca web simile a Perplexity Finance. Fornisci analisi professionali, oggettive, dettagliate e AGGIORNATE. Parli in italiano a meno che l'utente non scriva in un'altra lingua.

## Web Search - USALA ATTIVAMENTE
Hai accesso al tool di ricerca web. USALO PROATTIVAMENTE per:
- Cercare notizie recenti su aziende e mercati
- Verificare prezzi e dati aggiornati  
- Trovare rating, target price degli analisti
- Cercare dati fondamentali mancanti (P/E, EPS, debito, ecc.)
- Notizie di settore e macroeconomiche
- Risultati trimestrali e guidance
Quando l'utente chiede di analizzare uno strumento o chiede notizie, CERCA SEMPRE informazioni aggiornate prima di rispondere. Cita le fonti quando possibile.

## Le Tue Competenze
- **Azioni**: Analisi fondamentale e tecnica, multipli (P/E, EV/EBITDA, PEG), dividend yield, settori e comparabili
- **Obbligazioni/Bond**: Duration, yield to maturity, credit spread, rating, curva dei rendimenti, BTP/BOT italiani, bond corporate
- **Certificati di investimento**: Cash Collect, Phoenix, Bonus Cap, Express, Barrier — barriere, cedole, sottostanti, scenari a scadenza, rischio emittente
- **ETF e Fondi**: Expense ratio (TER), tracking error, diversificazione, confronto benchmark, categorie Morningstar
- **Mercati**: Borsa Italiana (FTSE MIB, STAR), mercati EU (DAX, CAC40, IBEX), US (S&P500, Nasdaq), materie prime, forex
- **Normativa**: Tassazione italiana su capital gain (26%), titoli di stato (12.5%), regime dichiarativo vs amministrato, KIID/KID

## Regole CRITICHE sui Dati
1. **USA dati forniti + ricerca web**: Quando ricevi dati dalla piattaforma, usali come base. Se mancano dati importanti, CERCALI sul web.
2. **NON inventare MAI numeri**: Se non trovi un dato né dalla piattaforma né dal web, scrivi "dato non disponibile".
3. **52W Range ≠ YTD**: Non usare il range 52 settimane per calcolare performance YTD. Cerca il prezzo di inizio anno sul web se necessario.
4. **Distingui tra fonti**: Specifica se un dato viene dalla piattaforma, dal web, o è una tua considerazione.
5. **Cita le fonti**: Quando usi informazioni dal web, indica da dove provengono.

## Altre Regole
- **Disclaimer**: Ricorda SEMPRE che non sei un consulente finanziario abilitato. Analisi a scopo informativo/educativo.
- **Oggettività**: Presenta pro E contro. Non consigliare mai di comprare o vendere.
- **Formato**: Usa markdown. Tabelle per confronti. Conciso ma completo.

## Contesto Piattaforma
ISIN Research & Compare aggrega dati da Yahoo Finance, Finnhub, Alpha Vantage e TwelveData. Copre azioni, bond, certificati, ETF e fondi. Se mancano dati dalla piattaforma, cercali sul web.`;

// ===================================
// WEB SEARCH TOOL CONFIG
// ===================================

const WEB_SEARCH_TOOL = {
    type: "web_search_20250305",
    name: "web_search",
    max_uses: 5
};

// ===================================
// MULTER CONFIG
// ===================================

const storage = multer.memoryStorage();
const upload = multer({ 
    storage,
    limits: { fileSize: 10 * 1024 * 1024 },
    fileFilter: (req, file, cb) => {
        const ext = file.originalname.split('.').pop().toLowerCase();
        const allowedExt = ['pdf', 'txt', 'csv', 'doc', 'docx'];
        if (allowedExt.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('Tipo file non supportato. Usa: PDF, TXT, CSV, DOCX'));
        }
    }
});

// ===================================
// HELPER: Extract text from uploaded file
// ===================================

async function extractText(file) {
    const ext = file.originalname.split('.').pop().toLowerCase();
    
    if (ext === 'txt' || ext === 'csv') {
        return file.buffer.toString('utf-8');
    }
    
    if (ext === 'pdf') {
        try {
            const data = await pdfParse(file.buffer);
            return data.text;
        } catch (error) {
            throw new Error('Impossibile estrarre testo dal PDF. File protetto o scansionato.');
        }
    }
    
    if (ext === 'docx') {
        try {
            const AdmZip = require('adm-zip');
            const zip = new AdmZip(file.buffer);
            const content = zip.readAsText('word/document.xml');
            return content.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
        } catch (error) {
            throw new Error('Impossibile estrarre testo dal DOCX.');
        }
    }
    
    throw new Error('Formato file non supportato');
}

// ===================================
// HELPER: Build messages with context
// ===================================

function buildMessages(userMessage, options = {}) {
    const { history = [], documentText = null, instrumentData = null } = options;
    const messages = [];
    
    // Conversation history (last 20)
    const recentHistory = history.slice(-20);
    for (const msg of recentHistory) {
        if (msg.role === 'user' || msg.role === 'assistant') {
            messages.push({ role: msg.role, content: msg.content });
        }
    }
    
    let content = '';
    
    if (instrumentData) {
        content += `\n[DATI STRUMENTO DALLA PIATTAFORMA]\n`;
        const cleanData = {};
        for (const [key, value] of Object.entries(instrumentData)) {
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') {
                cleanData[key] = value;
            }
        }
        content += JSON.stringify(cleanData, null, 2);
        content += `\n[FINE DATI PIATTAFORMA - Usa web search per trovare dati mancanti]\n\n`;
    }
    
    if (documentText) {
        const truncated = documentText.substring(0, 30000);
        content += `\n[DOCUMENTO CARICATO]\n`;
        content += truncated;
        if (documentText.length > 30000) {
            content += `\n... (troncato, ${documentText.length} caratteri totali)`;
        }
        content += `\n[FINE DOCUMENTO]\n\n`;
    }
    
    content += userMessage;
    messages.push({ role: 'user', content });
    
    return messages;
}

// ===================================
// HELPER: Process streaming with web search tool use
// ===================================

async function streamWithWebSearch(client, messages, res) {
    // Use non-streaming for web search (tool use requires it),
    // then stream the final response
    
    let currentMessages = [...messages];
    let iterations = 0;
    const MAX_ITERATIONS = 10; // safety limit
    
    // First pass: let Claude decide if it needs web search
    while (iterations < MAX_ITERATIONS) {
        iterations++;
        
        const response = await client.messages.create({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 4096,
            system: SYSTEM_PROMPT,
            tools: [WEB_SEARCH_TOOL],
            messages: currentMessages
        });
        
        // Check if response has only text (no more tool use needed)
        const hasToolUse = response.content.some(block => block.type === 'tool_use');
        
        if (response.stop_reason === 'end_turn' || !hasToolUse) {
            // Final response - stream it to client
            const fullText = response.content
                .filter(block => block.type === 'text')
                .map(block => block.text)
                .join('');
            
            // Send in chunks to simulate streaming
            const chunkSize = 20;
            for (let i = 0; i < fullText.length; i += chunkSize) {
                const chunk = fullText.substring(i, i + chunkSize);
                res.write(`data: ${JSON.stringify({ type: 'text', text: chunk })}\n\n`);
            }
            
            res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
            return { fullText, usage: response.usage };
        }
        
        // Claude used web search - add results and continue
        // Add assistant's response (with tool_use blocks) to messages
        currentMessages.push({
            role: 'assistant',
            content: response.content
        });
        
        // Process tool results
        const toolResults = [];
        for (const block of response.content) {
            if (block.type === 'web_search_tool_result') {
                // Web search results are automatically handled by the API
                // They come back as part of the content, no manual tool_result needed
                continue;
            }
        }
        
        // If there are server-side tool uses that need results, handle them
        // For web_search, the API handles it automatically
        // But we need to check if it's still going
        if (response.stop_reason === 'end_turn') {
            const fullText = response.content
                .filter(block => block.type === 'text')
                .map(block => block.text)
                .join('');
            
            const chunkSize = 20;
            for (let i = 0; i < fullText.length; i += chunkSize) {
                const chunk = fullText.substring(i, i + chunkSize);
                res.write(`data: ${JSON.stringify({ type: 'text', text: chunk })}\n\n`);
            }
            
            res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
            return { fullText, usage: response.usage };
        }
    }
    
    // Safety: if we hit max iterations
    res.write(`data: ${JSON.stringify({ type: 'error', error: 'Troppe iterazioni di ricerca.' })}\n\n`);
    return null;
}

// ===================================
// POST /api/ai/chat - Main chat endpoint (SSE streaming + web search)
// ===================================

router.post('/chat', async (req, res) => {
    if (!client) {
        return res.status(503).json({ 
            success: false, 
            error: 'AI service not configured. ANTHROPIC_API_KEY missing.' 
        });
    }
    
    if (!checkRateLimit(req.ip)) {
        return res.status(429).json({ 
            success: false, 
            error: 'Troppi messaggi. Riprova tra un minuto.' 
        });
    }
    
    const { message, history, documentText, instrumentData, stream = true } = req.body;
    
    if (!message || typeof message !== 'string' || message.trim().length === 0) {
        return res.status(400).json({ success: false, error: 'Message is required' });
    }
    
    if (message.length > 5000) {
        return res.status(400).json({ success: false, error: 'Messaggio troppo lungo. Massimo 5000 caratteri.' });
    }
    
    try {
        const messages = buildMessages(message.trim(), {
            history: history || [],
            documentText: documentText || null,
            instrumentData: instrumentData || null
        });
        
        // SSE headers
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        
        // Send a "searching" indicator
        res.write(`data: ${JSON.stringify({ type: 'status', status: 'searching' })}\n\n`);
        
        let aborted = false;
        req.on('close', () => { aborted = true; });
        
        // Use web search enabled flow
        try {
            const result = await streamWithWebSearch(client, messages, res);
            if (!aborted) res.end();
        } catch (streamError) {
            console.error('Stream/search error:', streamError);
            if (!aborted) {
                // Fallback: try without web search
                try {
                    console.log('Falling back to non-web-search mode...');
                    const fallbackResponse = await client.messages.create({
                        model: 'claude-sonnet-4-20250514',
                        max_tokens: 2048,
                        system: SYSTEM_PROMPT,
                        messages: messages
                    });
                    
                    const fullText = fallbackResponse.content
                        .filter(block => block.type === 'text')
                        .map(block => block.text)
                        .join('');
                    
                    const chunkSize = 20;
                    for (let i = 0; i < fullText.length; i += chunkSize) {
                        res.write(`data: ${JSON.stringify({ type: 'text', text: fullText.substring(i, i + chunkSize) })}\n\n`);
                    }
                    res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                    res.end();
                } catch (fallbackError) {
                    res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore nella generazione della risposta.' })}\n\n`);
                    res.end();
                }
            }
        }
        
    } catch (error) {
        console.error('AI chat error:', error);
        
        if (error.status === 401) {
            return res.status(500).json({ success: false, error: 'API key non valida.' });
        }
        if (error.status === 429) {
            return res.status(429).json({ success: false, error: 'Rate limit API raggiunto. Riprova tra poco.' });
        }
        
        res.status(500).json({ success: false, error: 'Errore nella generazione della risposta AI.' });
    }
});

// ===================================
// POST /api/ai/upload - Upload & extract document text
// ===================================

router.post('/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'Nessun file caricato' });
        }
        
        console.log(`📄 AI Upload: ${req.file.originalname} (${(req.file.size / 1024).toFixed(1)} KB)`);
        
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
        console.error('Upload error:', error);
        res.status(400).json({ success: false, error: error.message || 'Errore elaborazione file' });
    }
});

// ===================================
// POST /api/ai/analyze - Quick analyze instrument (with web search)
// ===================================

router.post('/analyze', async (req, res) => {
    if (!client) {
        return res.status(503).json({ success: false, error: 'AI service not configured.' });
    }
    
    if (!checkRateLimit(req.ip)) {
        return res.status(429).json({ success: false, error: 'Troppi messaggi. Riprova tra un minuto.' });
    }
    
    const { instrumentData, analysisType = 'general' } = req.body;
    
    if (!instrumentData) {
        return res.status(400).json({ success: false, error: 'instrumentData is required' });
    }
    
    const prompt = `Analizza questo strumento finanziario. Usa il web search per trovare notizie recenti, target price degli analisti, e dati fondamentali mancanti.
Fornisci:
1. **Overview**: Cos'è, settore, business
2. **Dati dalla piattaforma**: Commenta i dati forniti
3. **Dati dal web**: Cerca e aggiungi dati mancanti (target price, rating analisti, ultime notizie)
4. **Punti di forza e rischi**: 2-3 per categoria
5. **Valutazione**: Come si posiziona nel settore
Sii conciso, max 400 parole.`;
    
    try {
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        
        const cleanData = {};
        for (const [key, value] of Object.entries(instrumentData)) {
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') {
                cleanData[key] = value;
            }
        }
        
        const messages = [{
            role: 'user',
            content: `[DATI STRUMENTO]\n${JSON.stringify(cleanData, null, 2)}\n[FINE DATI]\n\n${prompt}`
        }];
        
        let aborted = false;
        req.on('close', () => { aborted = true; });
        
        await streamWithWebSearch(client, messages, res);
        if (!aborted) res.end();
        
    } catch (error) {
        console.error('Analyze error:', error);
        res.status(500).json({ success: false, error: 'Errore nell\'analisi dello strumento.' });
    }
});

// ===================================
// GET /api/ai/status
// ===================================

router.get('/status', (req, res) => {
    res.json({
        success: true,
        configured: !!client,
        model: 'claude-sonnet-4-20250514',
        features: ['chat', 'streaming', 'web-search', 'document-upload', 'instrument-analysis'],
        rateLimits: { maxPerMinute: RATE_LIMIT_MAX, windowMs: RATE_LIMIT_WINDOW }
    });
});

module.exports = router;
