/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * AI Financial Assistant Module
 * Powered by Claude (Anthropic) - Specialized in Italian/EU Finance
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

// Rate limiting simple (in-memory)
const rateLimits = new Map();
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX = 15; // 15 messages per minute

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
    
    if (limit.count >= RATE_LIMIT_MAX) {
        return false;
    }
    
    limit.count++;
    return true;
}

// Clean up rate limits every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [key, val] of rateLimits) {
        if (now > val.resetAt) rateLimits.delete(key);
    }
}, 5 * 60 * 1000);

// ===================================
// SYSTEM PROMPT - Financial Expert
// ===================================

const SYSTEM_PROMPT = `Sei un consulente finanziario AI esperto, integrato nella piattaforma ISIN Research & Compare di Mutna S.R.L.S.

## Il Tuo Ruolo
Sei un analista finanziario senior specializzato nei mercati europei e italiani. Fornisci analisi professionali, oggettive e dettagliate. Parli in italiano a meno che l'utente non scriva in un'altra lingua.

## Le Tue Competenze
- **Azioni**: Analisi fondamentale e tecnica, multipli (P/E, EV/EBITDA, PEG), dividend yield, settori e comparabili
- **Obbligazioni/Bond**: Duration, yield to maturity, credit spread, rating, curva dei rendimenti, BTP/BOT italiani, bond corporate
- **Certificati di investimento**: Cash Collect, Phoenix, Bonus Cap, Express, Barrier — comprendi barriere, cedole, sottostanti, scenari a scadenza, rischio emittente
- **ETF e Fondi**: Expense ratio (TER), tracking error, diversificazione, confronto benchmark, categorie Morningstar
- **Mercati**: Borsa Italiana (FTSE MIB, STAR), mercati EU (DAX, CAC40, IBEX), US (S&P500, Nasdaq), materie prime, forex
- **Normativa**: Tassazione italiana su capital gain (26%), titoli di stato (12.5%), regime dichiarativo vs amministrato, KIID/KID

## Regole CRITICHE sui Dati
1. **USA SOLO i dati forniti**: Quando ricevi dati dalla piattaforma, analizza ESCLUSIVAMENTE quelli. Non calcolare performance YTD, rendimenti storici, o altri dati derivati che non sono esplicitamente presenti.
2. **NON inventare MAI numeri**: Se un dato non è presente (es. performance YTD, beta, debito, ROE), scrivi chiaramente "dato non disponibile dalla piattaforma". Non calcolarlo o stimarlo da altri dati.
3. **52W Range ≠ YTD**: Il range a 52 settimane indica il minimo e massimo dell'anno, NON il prezzo a inizio anno. Non usarlo per calcolare performance YTD.
4. **Distingui tra fatti e opinioni**: I dati dalla piattaforma sono fatti. Le tue considerazioni su settore, competitor, outlook sono opinioni da etichettare come tali.
5. **Precisione**: Riporta i numeri esattamente come forniti, senza arrotondamenti non richiesti.

## Altre Regole
- **Disclaimer**: Ricorda SEMPRE che non sei un consulente finanziario abilitato. Le tue analisi sono a scopo informativo/educativo.
- **Oggettività**: Presenta pro E contro. Non consigliare mai di comprare o vendere.
- **Formato**: Usa markdown per formattare le risposte. Usa tabelle per confronti. Sii conciso ma completo.

## Contesto Piattaforma
ISIN Research & Compare è una piattaforma di ricerca finanziaria che aggrega dati da Yahoo Finance, Finnhub, Alpha Vantage e TwelveData. Copre azioni, bond, certificati, ETF e fondi. I dati dello strumento che ricevi sono quelli attualmente disponibili sulla piattaforma — se mancano campi importanti, segnalalo all'utente.`;

// ===================================
// MULTER CONFIG for file uploads
// ===================================

const storage = multer.memoryStorage();
const upload = multer({ 
    storage,
    limits: { fileSize: 10 * 1024 * 1024 }, // 10MB max
    fileFilter: (req, file, cb) => {
        const allowed = [
            'application/pdf',
            'text/plain',
            'text/csv',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/msword'
        ];
        // Also check extension
        const ext = file.originalname.split('.').pop().toLowerCase();
        const allowedExt = ['pdf', 'txt', 'csv', 'doc', 'docx'];
        
        if (allowed.includes(file.mimetype) || allowedExt.includes(ext)) {
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
            console.error('PDF parse error:', error.message);
            throw new Error('Impossibile estrarre testo dal PDF. Il file potrebbe essere protetto o scansionato.');
        }
    }
    
    if (ext === 'docx') {
        // Basic DOCX text extraction (XML-based)
        try {
            const AdmZip = require('adm-zip');
            const zip = new AdmZip(file.buffer);
            const content = zip.readAsText('word/document.xml');
            // Strip XML tags, keep text
            return content.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
        } catch (error) {
            console.error('DOCX parse error:', error.message);
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
    
    // Add conversation history (last 20 messages max to save tokens)
    const recentHistory = history.slice(-20);
    for (const msg of recentHistory) {
        if (msg.role === 'user' || msg.role === 'assistant') {
            messages.push({ role: msg.role, content: msg.content });
        }
    }
    
    // Build current user message with context
    let content = '';
    
    if (instrumentData) {
        content += `\n[DATI STRUMENTO DALLA PIATTAFORMA - Usa SOLO questi dati, non inventare o calcolare dati non presenti]\n`;
        // Clean up data - only send fields that have actual values
        const cleanData = {};
        for (const [key, value] of Object.entries(instrumentData)) {
            if (value !== null && value !== undefined && value !== 'N/A' && value !== '') {
                cleanData[key] = value;
            }
        }
        content += JSON.stringify(cleanData, null, 2);
        content += `\n[FINE DATI - Campi non presenti qui sopra NON sono disponibili. Non calcolarli.]\n\n`;
    }
    
    if (documentText) {
        // Truncate to ~30k chars to leave room for response
        const truncated = documentText.substring(0, 30000);
        content += `\n[DOCUMENTO CARICATO]\n`;
        content += truncated;
        if (documentText.length > 30000) {
            content += `\n... (documento troncato, ${documentText.length} caratteri totali)`;
        }
        content += `\n[FINE DOCUMENTO]\n\n`;
    }
    
    content += userMessage;
    
    messages.push({ role: 'user', content });
    
    return messages;
}

// ===================================
// POST /api/ai/chat - Main chat endpoint (streaming SSE)
// ===================================

router.post('/chat', async (req, res) => {
    if (!client) {
        return res.status(503).json({ 
            success: false, 
            error: 'AI service not configured. ANTHROPIC_API_KEY missing.' 
        });
    }
    
    // Rate limit
    if (!checkRateLimit(req.ip)) {
        return res.status(429).json({ 
            success: false, 
            error: 'Troppi messaggi. Riprova tra un minuto.' 
        });
    }
    
    const { message, history, documentText, instrumentData, stream = true } = req.body;
    
    if (!message || typeof message !== 'string' || message.trim().length === 0) {
        return res.status(400).json({ 
            success: false, 
            error: 'Message is required' 
        });
    }
    
    // Limit message length
    if (message.length > 5000) {
        return res.status(400).json({ 
            success: false, 
            error: 'Messaggio troppo lungo. Massimo 5000 caratteri.' 
        });
    }
    
    try {
        const messages = buildMessages(message.trim(), {
            history: history || [],
            documentText: documentText || null,
            instrumentData: instrumentData || null
        });
        
        if (stream) {
            // SSE streaming response
            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.setHeader('X-Accel-Buffering', 'no');
            
            const streamResponse = await client.messages.stream({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 2048,
                system: SYSTEM_PROMPT,
                messages: messages
            });
            
            let fullText = '';
            
            streamResponse.on('text', (text) => {
                fullText += text;
                res.write(`data: ${JSON.stringify({ type: 'text', text })}\n\n`);
            });
            
            streamResponse.on('end', () => {
                res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                res.end();
            });
            
            streamResponse.on('error', (error) => {
                console.error('Stream error:', error);
                res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore durante la generazione della risposta.' })}\n\n`);
                res.end();
            });
            
            // Handle client disconnect
            req.on('close', () => {
                streamResponse.abort();
            });
            
        } else {
            // Non-streaming response
            const response = await client.messages.create({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 2048,
                system: SYSTEM_PROMPT,
                messages: messages
            });
            
            const aiText = response.content
                .filter(block => block.type === 'text')
                .map(block => block.text)
                .join('');
            
            res.json({
                success: true,
                response: aiText,
                usage: {
                    inputTokens: response.usage.input_tokens,
                    outputTokens: response.usage.output_tokens
                }
            });
        }
        
    } catch (error) {
        console.error('AI chat error:', error);
        
        if (error.status === 401) {
            return res.status(500).json({ success: false, error: 'API key non valida.' });
        }
        if (error.status === 429) {
            return res.status(429).json({ success: false, error: 'Rate limit API raggiunto. Riprova tra poco.' });
        }
        if (error.status === 529) {
            return res.status(503).json({ success: false, error: 'Servizio AI temporaneamente sovraccarico. Riprova tra poco.' });
        }
        
        res.status(500).json({ 
            success: false, 
            error: 'Errore nella generazione della risposta AI.' 
        });
    }
});

// ===================================
// POST /api/ai/upload - Upload & extract document text
// ===================================

router.post('/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ 
                success: false, 
                error: 'Nessun file caricato' 
            });
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
        res.status(400).json({ 
            success: false, 
            error: error.message || 'Errore nell\'elaborazione del file' 
        });
    }
});

// ===================================
// POST /api/ai/analyze - Quick analyze instrument
// ===================================

router.post('/analyze', async (req, res) => {
    if (!client) {
        return res.status(503).json({ 
            success: false, 
            error: 'AI service not configured.' 
        });
    }
    
    if (!checkRateLimit(req.ip)) {
        return res.status(429).json({ 
            success: false, 
            error: 'Troppi messaggi. Riprova tra un minuto.' 
        });
    }
    
    const { instrumentData, analysisType = 'general' } = req.body;
    
    if (!instrumentData) {
        return res.status(400).json({ 
            success: false, 
            error: 'instrumentData is required' 
        });
    }
    
    const analysisPrompts = {
        general: `Analizza questo strumento finanziario in modo sintetico. Fornisci:
1. **Overview**: Cos'è e in che settore opera
2. **Punti di forza**: 2-3 aspetti positivi basati sui dati
3. **Rischi**: 2-3 aspetti critici o di attenzione
4. **Valutazione**: Come si posiziona rispetto ai comparabili del settore (se possibile)
Sii conciso, max 300 parole.`,
        
        dividend: `Analizza questo strumento dal punto di vista dei dividendi:
1. **Dividend Yield attuale** e confronto con media settore
2. **Sostenibilità** del dividendo (payout ratio se disponibile)
3. **Storico** e trend
4. **Tassazione** italiana applicabile
Sii conciso, max 200 parole.`,
        
        risk: `Analizza il profilo di rischio di questo strumento:
1. **Volatilità** e beta (se disponibili)
2. **Rischi specifici** (settore, paese, valuta)
3. **Livello di rischio** su scala 1-5
4. **Per chi è adatto** questo strumento
Sii conciso, max 200 parole.`,
        
        comparison: `Basandoti sui dati forniti, suggerisci:
1. **Comparabili**: 3-5 strumenti simili da confrontare
2. **Benchmark** appropriato
3. **Posizionamento**: Come si colloca rispetto alla media del settore
Sii conciso, max 200 parole.`
    };
    
    const prompt = analysisPrompts[analysisType] || analysisPrompts.general;
    
    try {
        // SSE streaming
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        
        const messages = [{
            role: 'user',
            content: `[DATI STRUMENTO]\n${JSON.stringify(instrumentData, null, 2)}\n[FINE DATI]\n\n${prompt}`
        }];
        
        const streamResponse = await client.messages.stream({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 1024,
            system: SYSTEM_PROMPT,
            messages
        });
        
        streamResponse.on('text', (text) => {
            res.write(`data: ${JSON.stringify({ type: 'text', text })}\n\n`);
        });
        
        streamResponse.on('end', () => {
            res.write(`data: ${JSON.stringify({ type: 'done' })}\n\n`);
            res.end();
        });
        
        streamResponse.on('error', (error) => {
            console.error('Analyze stream error:', error);
            res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore durante l\'analisi.' })}\n\n`);
            res.end();
        });
        
        req.on('close', () => {
            streamResponse.abort();
        });
        
    } catch (error) {
        console.error('Analyze error:', error);
        res.status(500).json({ 
            success: false, 
            error: 'Errore nell\'analisi dello strumento.' 
        });
    }
});

// ===================================
// GET /api/ai/status - Check AI service status
// ===================================

router.get('/status', (req, res) => {
    res.json({
        success: true,
        configured: !!client,
        model: 'claude-sonnet-4-20250514',
        features: ['chat', 'streaming', 'document-upload', 'instrument-analysis'],
        rateLimits: {
            maxPerMinute: RATE_LIMIT_MAX,
            windowMs: RATE_LIMIT_WINDOW
        }
    });
});

module.exports = router;
