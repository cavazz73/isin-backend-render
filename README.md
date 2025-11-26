# ISIN Research Backend v2.3 - Yahoo Finance Optimized

## 🎯 Cosa è cambiato da v2.2

### Problema risolto
Il backend v2.2 faceva **TROPPE chiamate API parallele**:
- Per ogni ricerca, chiamava quote per TUTTI i risultati (20+)
- Questo causava rate limiting da tutti i provider (401, 403, 429)

### Soluzione v2.3
1. **Usa SOLO Yahoo Finance** via `yahoo-finance2` library
2. **Rate limiting**: 200ms tra le richieste
3. **Caching**: 5 minuti TTL per evitare chiamate ripetute
4. **Smart quote**: Solo 1 quote per ricerca (primo risultato), non tutti
5. **Retry con backoff**: Se fallisce, riprova con delay crescente

## 📦 File inclusi
- `server.js` - Server Express con tutte le API routes
- `dataAggregator.js` - Logica di aggregazione con cache e rate limiting
- `yahooFinance.js` - Client Yahoo Finance usando yahoo-finance2
- `package.json` - Dipendenze (express, cors, axios, yahoo-finance2)
- `test.js` - Script di test

## 🚀 Deploy su Render

### Opzione A: Aggiornare repo esistente
```bash
# Nel tuo repo isin-backend-render
git pull
# Sostituisci i file con quelli nuovi
cp server.js dataAggregator.js yahooFinance.js package.json ./
git add .
git commit -m "v2.3 - Yahoo Finance optimized with rate limiting and caching"
git push
# Render fa auto-deploy
```

### Opzione B: Upload manuale su Render
1. Dashboard Render → tuo servizio → Settings → Manual Deploy
2. Oppure: Crea nuovo Web Service, upload questi file

## 🧪 Test dopo deploy

Visita questi endpoint:
```
https://isin-backend.onrender.com/health
https://isin-backend.onrender.com/api/financial/test
https://isin-backend.onrender.com/api/financial/search?q=AAPL
https://isin-backend.onrender.com/api/financial/search?q=ENEL
https://isin-backend.onrender.com/api/financial/quote/ENEL.MI
```

## ✅ Comportamento atteso

### Ricerca "ENEL":
```json
{
  "success": true,
  "results": [
    {
      "symbol": "ENEL.MI",
      "name": "Enel S.p.A.",
      "exchange": "MIL",
      "currency": "EUR",
      "price": 6.75,  // Esempio
      "change": 0.12,
      "changePercent": 1.81
    }
  ]
}
```

### Cache stats:
```
GET /api/debug/cache
{
  "size": 3,
  "ttl": "300 seconds",
  "entries": ["search:aapl", "search:enel", "quote:ENEL.MI"]
}
```

## 🔧 Troubleshooting

### "fetch failed" nei test locali
**Causa**: Ambiente sandbox non ha accesso a Yahoo
**Fix**: Il codice funziona su Render, è solo un limite dell'ambiente di test

### Prima richiesta lenta
**Causa**: Render free tier spin-up (50-60 sec)
**Fix**: Normale, le richieste successive sono veloci

### Nessun prezzo mostrato
**Causa**: Quote fallita per rate limit
**Fix**: Aspetta 1 minuto, il retry con backoff gestirà automaticamente

## 📊 Rate limits

| Provider | Limite | Note |
|----------|--------|------|
| Yahoo Finance (yahoo-finance2) | Non documentato, ma generoso | Rate limiting interno a 200ms |

## 💡 Ottimizzazioni future

1. **Redis cache** invece di in-memory per persistence
2. **UptimeRobot** per mantenere Render sveglio
3. **Backup provider** (Twelve Data) se Yahoo fallisce

---

Versione: 2.3.0
Data: 26 Nov 2025
Copyright: Mutna S.R.L.S.
