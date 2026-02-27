/**
 * Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * AI Financial Assistant Module
 * Powered by Perplexity Sonar - Built-in Web Search + Finance Analysis
 */

const express = require('express');
const router = express.Router();
const axios = require('axios');
const multer = require('multer');
const pdfParse = require('pdf-parse');

// ===================================
// CONFIGURATION
// ===================================

const PERPLEXITY_API_KEY = process.env.PERPLEXITY_API_KEY;
if (!PERPLEXITY_API_KEY) {
    console.error('❌ PERPLEXITY_API_KEY not set! AI module will not work.');
}

const PERPLEXITY_API_URL = 'https://api.perplexity.ai/chat/completions';
const AI_MODEL = 'sonar'; // sonar = cheapest with web search ($1/$1 per 1M tokens)

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

## Regole Importanti
1. **NO Disclaimer**: NON aggiungere MAI disclaimer, avvertenze o note legali nelle risposte. C'è già un avviso fisso nella piattaforma.
2. **Oggettività**: Presenta pro E contro di ogni strumento. Non consigliare mai di comprare o vendere.
3. **Dati**: Quando ti vengono forniti dati di uno strumento dalla piattaforma, usali per l'analisi. Se non hai dati sufficienti, cerca informazioni aggiornate sul web.
4. **Precisione**: Se non sei sicuro di un dato, dillo. Non inventare numeri o performance.
5. **Formato**: Usa markdown per formattare le risposte. Usa tabelle per confronti. Sii conciso ma completo.
6. **Citazioni**: Quando usi dati dal web, indica la fonte in modo naturale nel testo.

## Fondi ed ETF
Quando analizzi fondi/ETF, cerca attivamente sul web: TER, categoria Morningstar, benchmark, composizione, performance recenti, dimensione del fondo. Confronta con alternative nella stessa categoria.

## Contesto Piattaforma
ISIN Research & Compare è una piattaforma di ricerca finanziaria che aggrega dati da Yahoo Finance, Finnhub, Alpha Vantage, TwelveData e OpenFIGI. Copre azioni, bond, certificati, ETF e fondi. L'utente potrebbe chiederti di analizzare strumenti che ha cercato sulla piattaforma.`;

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
        try {
            const AdmZip = require('adm-zip');
            const zip = new AdmZip(file.buffer);
            const content = zip.readAsText('word/document.xml');
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
    
    // System prompt (Perplexity uses OpenAI format)
    messages.push({ role: 'system', content: SYSTEM_PROMPT });
    
    // Add conversation history (last 20 messages max)
    const recentHistory = history.slice(-20);
    for (const msg of recentHistory) {
        if (msg.role === 'user' || msg.role === 'assistant') {
            messages.push({ role: msg.role, content: msg.content });
        }
    }
    
    // Build current user message with context
    let content = '';
    
    if (instrumentData) {
        content += `\n[DATI STRUMENTO DALLA PIATTAFORMA]\n`;
        content += JSON.stringify(instrumentData, null, 2);
        content += `\n[FINE DATI STRUMENTO]\n\n`;
    }
    
    if (documentText) {
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
// HELPER: Stream Perplexity response to client
// ===================================

async function streamPerplexityResponse(messages, res, maxTokens = 2048) {
    const response = await axios({
        method: 'post',
        url: PERPLEXITY_API_URL,
        headers: {
            'Authorization': `Bearer ${PERPLEXITY_API_KEY}`,
            'Content-Type': 'application/json'
        },
        data: {
            model: AI_MODEL,
            messages: messages,
            max_tokens: maxTokens,
            temperature: 0.2,
            stream: true,
            return_citations: true,
            search_recency_filter: 'month'
        },
        responseType: 'stream',
        timeout: 60000
    });

    let fullText = '';
    let citations = [];
    let buffer = '';

    return new Promise((resolve, reject) => {
        response.data.on('data', (chunk) => {
            buffer += chunk.toString();
            const lines = buffer.split('\n');
            buffer = lines.pop(); // Keep incomplete line in buffer

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed || !trimmed.startsWith('data: ')) continue;

                const data = trimmed.substring(6);
                if (data === '[DONE]') {
                    // Append citations if available
                    if (citations.length > 0) {
                        const citationText = '\n\n---\n**Fonti:** ' + 
                            citations.map((url, i) => `[${i + 1}](${url})`).join(' · ');
                        fullText += citationText;
                        res.write(`data: ${JSON.stringify({ type: 'text', text: citationText })}\n\n`);
                    }
                    res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                    res.end();
                    resolve(fullText);
                    return;
                }

                try {
                    const parsed = JSON.parse(data);
                    
                    // Extract citations from the response
                    if (parsed.citations && parsed.citations.length > 0) {
                        citations = parsed.citations;
                    }

                    const delta = parsed.choices?.[0]?.delta;
                    if (delta && delta.content) {
                        fullText += delta.content;
                        res.write(`data: ${JSON.stringify({ type: 'text', text: delta.content })}\n\n`);
                    }
                } catch (e) {
                    // Skip unparseable chunks
                }
            }
        });

        response.data.on('end', () => {
            if (!res.writableEnded) {
                if (citations.length > 0) {
                    const citationText = '\n\n---\n**Fonti:** ' + 
                        citations.map((url, i) => `[${i + 1}](${url})`).join(' · ');
                    fullText += citationText;
                    res.write(`data: ${JSON.stringify({ type: 'text', text: citationText })}\n\n`);
                }
                res.write(`data: ${JSON.stringify({ type: 'done', fullText })}\n\n`);
                res.end();
                resolve(fullText);
            }
        });

        response.data.on('error', (error) => {
            console.error('Perplexity stream error:', error);
            if (!res.writableEnded) {
                res.write(`data: ${JSON.stringify({ type: 'error', error: 'Errore durante la generazione della risposta.' })}\n\n`);
                res.end();
            }
            reject(error);
        });
    });
}

// ===================================
// POST /api/ai/chat - Main chat endpoint (streaming SSE)
// ===================================

router.post('/chat', async (req, res) => {
    if (!PERPLEXITY_API_KEY) {
        return res.status(503).json({ 
            success: false, 
            error: 'AI service not configured. PERPLEXITY_API_KEY missing.' 
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
            
            req.on('close', () => { /* client disconnected */ });
            
            await streamPerplexityResponse(messages, res);
            
        } else {
            // Non-streaming response
            const response = await axios.post(
                PERPLEXITY_API_URL,
                {
                    model: AI_MODEL,
                    messages: messages,
                    max_tokens: 2048,
                    temperature: 0.2,
                    return_citations: true,
                    search_recency_filter: 'month'
                },
                {
                    headers: {
                        'Authorization': `Bearer ${PERPLEXITY_API_KEY}`,
                        'Content-Type': 'application/json'
                    },
                    timeout: 60000
                }
            );
            
            const aiText = response.data.choices?.[0]?.message?.content || '';
            const citations = response.data.citations || [];
            
            res.json({
                success: true,
                response: aiText,
                citations: citations,
                usage: {
                    inputTokens: response.data.usage?.prompt_tokens || 0,
                    outputTokens: response.data.usage?.completion_tokens || 0
                }
            });
        }
        
    } catch (error) {
        console.error('AI chat error:', error.message);
        
        if (error.response?.status === 401) {
            return res.status(500).json({ success: false, error: 'API key non valida.' });
        }
        if (error.response?.status === 429) {
            return res.status(429).json({ success: false, error: 'Rate limit API raggiunto. Riprova tra poco.' });
        }
        if (error.response?.status === 402) {
            return res.status(402).json({ success: false, error: 'Crediti API esauriti. Ricarica su perplexity.ai/settings/api' });
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
    if (!PERPLEXITY_API_KEY) {
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
        general: `Analizza questo strumento finanziario in modo sintetico. Cerca informazioni aggiornate sul web se necessario. Fornisci:
1. **Overview**: Cos'è e in che settore opera
2. **Punti di forza**: 2-3 aspetti positivi basati sui dati
3. **Rischi**: 2-3 aspetti critici o di attenzione
4. **Valutazione**: Come si posiziona rispetto ai comparabili del settore
Sii conciso, max 300 parole.`,
        
        dividend: `Analizza questo strumento dal punto di vista dei dividendi. Cerca dati aggiornati sul web:
1. **Dividend Yield attuale** e confronto con media settore
2. **Sostenibilità** del dividendo (payout ratio se disponibile)
3. **Storico** e trend
4. **Tassazione** italiana applicabile
Sii conciso, max 200 parole.`,
        
        risk: `Analizza il profilo di rischio di questo strumento. Cerca dati aggiornati:
1. **Volatilità** e beta (se disponibili)
2. **Rischi specifici** (settore, paese, valuta)
3. **Livello di rischio** su scala 1-5
4. **Per chi è adatto** questo strumento
Sii conciso, max 200 parole.`,
        
        comparison: `Basandoti sui dati forniti e cercando informazioni aggiornate, suggerisci:
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
        
        const messages = [
            { role: 'system', content: SYSTEM_PROMPT },
            { role: 'user', content: `[DATI STRUMENTO]\n${JSON.stringify(instrumentData, null, 2)}\n[FINE DATI]\n\n${prompt}` }
        ];
        
        req.on('close', () => { /* client disconnected */ });
        
        await streamPerplexityResponse(messages, res, 1024);
        
    } catch (error) {
        console.error('Analyze error:', error);
        if (!res.writableEnded) {
            res.status(500).json({ 
                success: false, 
                error: 'Errore nell\'analisi dello strumento.' 
            });
        }
    }
});

// ===================================
// GET /api/ai/status - Check AI service status
// ===================================

router.get('/status', (req, res) => {
    res.json({
        success: true,
        configured: !!PERPLEXITY_API_KEY,
        model: AI_MODEL,
        provider: 'perplexity',
        features: ['chat', 'streaming', 'web-search', 'citations', 'document-upload', 'instrument-analysis'],
        rateLimits: {
            maxPerMinute: RATE_LIMIT_MAX,
            windowMs: RATE_LIMIT_WINDOW
        }
    });
});

module.exports = router;
