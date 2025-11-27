# 🚨 FIX DEPLOY v3.0 - GUIDA RAPIDA

## ❌ PROBLEMA RILEVATO
```
Error: Cannot find module 'dotenv'
Error: Cannot find module './routes/financial'
```

## ✅ SOLUZIONE

### STEP 1: Sostituisci questi 2 file su GitHub

**1. package.json** (NUOVO - aggiunge dotenv)
**2. server.js** (NUOVO - fix import `./financial` invece di `./routes/financial`)

### STEP 2: Rinomina i file v3.0

```bash
# Nel tuo repo locale:
cp dataAggregator-v3.js dataAggregator.js
cp test-v3.js test.js
```

### STEP 3: Push su GitHub

```bash
git add .
git commit -m "v3.0 - Fix dotenv + flat structure"
git push origin main
```

### STEP 4: Render Auto-Deploy

Render rileverà il push e farà auto-deploy. Aspetta 2-3 minuti.

---

## 📋 FILE MODIFICATI

### package.json ✅
```json
{
  "dependencies": {
    "express": "^4.18.2",
    "cors": "^2.8.5",
    "axios": "^1.6.2",
    "dotenv": "^16.3.1"  ← AGGIUNTO!
  }
}
```

### server.js ✅
```javascript
require('dotenv').config();  ← AGGIUNTO!
const financialRoutes = require('./financial');  ← FIX (era ./routes/financial)
```

---

## 🧪 TEST DOPO DEPLOY

1. **Health Check:**
   ```
   https://isin-backend.onrender.com/health
   ```
   Deve mostrare: `"version": "3.0.0"`

2. **Search ENEL:**
   ```
   https://isin-backend.onrender.com/api/financial/search?q=ENEL
   ```
   Deve mostrare: `"currency": "EUR"` ✅

3. **Search AAPL:**
   ```
   https://isin-backend.onrender.com/api/financial/search?q=AAPL
   ```
   Deve mostrare: `"currency": "USD"` ✅

---

## 🔑 ENV VARIABLES SU RENDER (verifica)

Dashboard Render → tuo servizio → Environment → Controlla che ci siano:

```
TWELVE_DATA_API_KEY=ce2a8eb85df743e4a798f18829cd9967
FINNHUB_API_KEY=c7kgn0pr01qhqt0p6750c7kgn0pr01qhqt0p6760
ALPHA_VANTAGE_API_KEY=5CU9FRS894ZHVEXF
NODE_ENV=production
```

---

## ✅ SUCCESS CRITERIA

Deploy OK se:
- ✅ Nessun errore "Cannot find module"
- ✅ Logs mostrano "🚀 ISIN RESEARCH BACKEND v3.0 - STARTED"
- ✅ Health check risponde con version 3.0.0
- ✅ ENEL mostra EUR (non USD!)

---

## 🆘 SE PROBLEMI PERSISTONO

1. Controlla logs Render: Dashboard → Logs
2. Verifica che tutti i file siano nella root (no cartelle routes/ o services/)
3. Verifica che package.json contenga "dotenv": "^16.3.1"
4. Se necessario, fai "Clear build cache" su Render e ri-deploy

---

**Tempo stimato fix: 5 minuti** ⏱️
