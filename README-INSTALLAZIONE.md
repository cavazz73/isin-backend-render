# 🚀 ISIN Research Backend v3.0 - INSTALLAZIONE RAPIDA

## ✅ STRUTTURA FILE (tutto nella ROOT)

```
isin-backend-render/
├── server.js          ← SOSTITUISCI con nuovo
├── financial.js       ← SOSTITUISCI con nuovo  
├── dataAggregator.js  ← SOSTITUISCI con nuovo
├── twelveData.js      ← NUOVO FILE da aggiungere
├── yahooFinance.js    (mantieni quello esistente)
├── finnhub.js         (mantieni quello esistente)
├── alphaVantage.js    (mantieni quello esistente)
├── test.js            ← SOSTITUISCI con test-v3.js
├── .env               ← CREA/AGGIORNA
├── package.json       (mantieni quello esistente)
└── node_modules/      (mantieni)
```

---

## 📋 INSTALLAZIONE (5 minuti)

### STEP 1: Backup (opzionale ma consigliato)
```bash
cp server.js server.js.backup
cp financial.js financial.js.backup
cp dataAggregator.js dataAggregator.js.backup
```

### STEP 2: Copia i nuovi file (nella ROOT!)
```bash
# Copia i 4 file scaricati nella ROOT del progetto
cp ~/Downloads/server.js .
cp ~/Downloads/financial.js .
cp ~/Downloads/dataAggregator.js .
cp ~/Downloads/twelveData.js .
cp ~/Downloads/test-v3.js ./test.js
```

### STEP 3: Crea/Aggiorna il file .env
```bash
nano .env
```

Copia questo contenuto:
```bash
# PRIMARY SOURCE - TwelveData (EU Markets)
TWELVE_DATA_API_KEY=ce2a8eb85df743e4a798f18829cd9967

# BACKUP SOURCES
FINNHUB_API_KEY=c7kgn0pr01qhqt0p6750c7kgn0pr01qhqt0p6760
ALPHA_VANTAGE_API_KEY=demo

# SERVER
PORT=3001
NODE_ENV=development
```

Salva con `CTRL+X`, poi `Y`, poi `ENTER`

---

## 🧪 TEST LOCALE

```bash
# Test completo
node test.js
```

**Output atteso:**
```
✅ TEST 2: Search ENEL → Found 1 results
   Symbol: ENEL.MI
   Currency: EUR ← IMPORTANTE!
   Price: 6.85 EUR

✅ TEST 3: Quote ENEL → 6.85 EUR

✅ TEST 4: Search ENI → EUR 13.42
```

**Se vedi EUR (non USD) per ENEL/ENI = FUNZIONA! ✅**

---

## 🚀 AVVIO SERVER

```bash
npm start
```

Dovresti vedere:
```
============================================================
ISIN Research Backend v3.0 - TwelveData Integration
============================================================
Server running on port 3001
Data sources priority:
  EU Stocks: TwelveData → Yahoo → Finnhub → Alpha Vantage
  US Stocks: Yahoo → TwelveData → Finnhub → Alpha Vantage
============================================================
Environment:
  TWELVE_DATA_API_KEY: SET ✓
  FINNHUB_API_KEY: SET ✓
  ALPHA_VANTAGE_API_KEY: SET ✓
============================================================
```

---

## 🌐 TEST API

```bash
# Test ENEL (deve tornare EUR!)
curl "http://localhost:3001/api/financial/search?q=ENEL" | jq .

# Test Quote ENEL
curl "http://localhost:3001/api/financial/quote/ENEL.MI" | jq .

# Health Check
curl "http://localhost:3001/health" | jq .
```

**Verifica che vedi:**
```json
{
  "currency": "EUR",  ← NON "USD"!
  "price": 6.85
}
```

---

## 🎯 DEPLOY SU RENDER

### 1. Commit e Push
```bash
git add .
git commit -m "v3.0 - TwelveData integration for correct EUR pricing"
git push origin main
```

### 2. Aggiorna Environment Variables su Render
1. Vai su https://dashboard.render.com
2. Seleziona il tuo servizio `isin-backend-render`
3. Click su **Environment** tab
4. **Aggiungi questa nuova variabile:**
   ```
   TWELVE_DATA_API_KEY = ce2a8eb85df743e4a798f18829cd9967
   ```
5. Click **Save Changes**
6. Render farà **automatic redeploy**

### 3. Verifica Deploy
Aspetta 2-3 minuti, poi testa:
```bash
# Health check
curl https://isin-backend.onrender.com/health | jq .

# Test ENEL (DEVE tornare EUR!)
curl "https://isin-backend.onrender.com/api/financial/search?q=ENEL" | jq .
```

---

## ✅ SUCCESS CRITERIA

Il deploy è OK quando:
- ✅ Server parte senza errori
- ✅ Health check mostra `"version": "3.0.0"`
- ✅ ENEL mostra `"currency": "EUR"` (NON "USD")
- ✅ Quote ENEL mostra prezzo tipo `6.85 EUR`
- ✅ Test completo passa tutti gli 8 test

---

## ❌ TROUBLESHOOTING

### Problema: "Cannot find module './twelveData'"
**Causa:** File twelveData.js non copiato nella root
**Fix:** `cp ~/Downloads/twelveData.js .`

### Problema: ENEL mostra USD invece di EUR
**Causa:** .env non configurato correttamente
**Fix:** Verifica che .env contenga `TWELVE_DATA_API_KEY=ce2a8eb85df743e4a798f18829cd9967`

### Problema: "TWELVE_DATA_API_KEY: MISSING ✗"
**Causa:** .env non caricato
**Fix:** 
1. Verifica che .env sia nella root
2. Riavvia il server: `npm start`

### Problema: Server parte ma API non risponde
**Causa:** Porta già in uso
**Fix:** 
```bash
killall node
npm start
```

---

## 📊 DIFFERENZE v2.0 → v3.0

| Feature | v2.0 | v3.0 |
|---------|------|------|
| ENEL pricing | ❌ USD 6.85 | ✅ EUR 6.85 |
| Primary source EU | Yahoo | TwelveData |
| Italian stocks | Limited | Excellent |
| Rate limit | 60/min | 800/day |
| European exchanges | Basic | Full support |

---

## 📞 SUPPORTO

- Backend: https://isin-backend.onrender.com
- GitHub: https://github.com/cavazz73/isin-backend-render
- Email: info@mutna.it

**Problema non risolto? Contattami con screenshot dell'errore!**
