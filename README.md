# 🚀 ISIN RESEARCH v4.0 - DEPLOY

## ✅ STRUTTURA (FLAT - come repo esistente)

```
isin-backend-render/          ← TUO REPO GITHUB
├── server.js                 ← Main server
├── package.json
├── .env                      ← API keys
├── financial.js              ← API routes
├── dataAggregator.js         ← Orchestrator
├── yahooFinance.js           ← PRIMARY (FIXED)
├── finnhub.js                ← Backup
└── alphaVantage.js           ← Backup
```

**NOTA:** Tutto nella ROOT, nessuna cartella routes/ o services/

---

## 📦 DEPLOY BACKEND (3 minuti)

### Metodo 1: Sostituisci file nel repo esistente

```bash
# Clona il tuo repo
git clone https://github.com/cavazz73/isin-backend-render.git
cd isin-backend-render

# Copia i nuovi file (sovrascrivi tutto)
cp /path/to/downloaded/files/* .

# Push
git add .
git commit -m "v4.0 - Yahoo PRIMARY + Headers FIXED"
git push
```

Render farà AUTOMATICAMENTE il redeploy in 2 minuti.

### Metodo 2: Upload manuale su Render

1. Render Dashboard → tuo service
2. Settings → Deploy Hook
3. Manual Deploy → Deploy Latest Commit

---

## ⚙️ ENVIRONMENT VARIABLES (Render)

Verifica che siano configurate:

```
FINNHUB_API_KEY = d4g3tepr01qgiieo60qgd4g3tepr01qgiieo60r0
ALPHA_VANTAGE_API_KEY = 5CU9FRS894ZHVEXF
NODE_ENV = production
```

---

## 🧪 TEST

1. **Health check:**
```
https://isin-backend.onrender.com/health
```

Risposta attesa:
```json
{
  "status": "ok",
  "uptime": 123,
  "version": "2.0.0"
}
```

2. **Search ENEL:**
```
https://isin-backend.onrender.com/api/financial/search?q=ENEL
```

Deve restituire risultati con `"symbol": "ENEL.MI"` e `"currency": "EUR"`

3. **Quote ENEL:**
```
https://isin-backend.onrender.com/api/financial/quote/ENEL.MI
```

Deve restituire `"price": 6.XX` e `"currency": "EUR"`

---

## ✅ COSA È FIXATO

### yahooFinance.js
- ✅ Headers completi (no più 401)
- ✅ Normalizzazione ENEL→ENEL.MI automatica
- ✅ EUR corretto per azioni italiane

### dataAggregator.js
- ✅ Yahoo PRIMARY (non più TwelveData)
- ✅ Fallback automatico a Finnhub/AlphaVantage

---

## 📊 RISULTATO ATTESO

**Cerca "ENEL":**
```json
{
  "success": true,
  "results": [{
    "symbol": "ENEL.MI",
    "name": "Enel S.p.A.",
    "currency": "EUR",
    "price": 6.XX,
    "change": +0.XX,
    "exchange": "MIL"
  }],
  "source": "yahoo"
}
```

**Frontend mostra:**
```
Prezzo: 6.XX EUR  ← CORRETTO!
Valuta: EUR
Fonte: yahoo
```

---

## ❌ TROUBLESHOOTING

**Backend non risponde:**
- Aspetta 60 secondi (cold start)
- Verifica Render logs: Dashboard → Logs

**401 errors nei log:**
- File yahooFinance.js HA headers corretti
- Se persiste: verifica che il file sia aggiornato su GitHub

**ENEL mostra N/A:**
- Verifica log Render: deve dire `[Yahoo] Quote OK: 6.XX EUR`
- Se dice `[Yahoo] Quote error: 401` → file non aggiornato

---

## 📞 SUPPORTO

- GitHub: https://github.com/cavazz73/isin-backend-render
- Email: info@mutna.it
