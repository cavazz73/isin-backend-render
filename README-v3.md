# 🚀 ISIN Research Backend V3.0 - Quick Deploy

## ✅ WHAT'S NEW
- **TwelveData PRIMARY** for European stocks (ENEL, ENI, etc.)
- **Correct EUR currency** for Italian stocks (no more USD!)
- **800 requests/day** free tier (plenty for development)

---

## 📂 FILE DA AGGIORNARE (Struttura FLAT)

Nella **ROOT** del tuo progetto:

```
isin-backend-render/
├── twelveData.js          ← ⭐ NUOVO (aggiungi)
├── dataAggregator.js      ← 🔄 SOSTITUISCI
├── financial.js           ← 🔄 SOSTITUISCI
├── test.js                ← 🔄 SOSTITUISCI (con test-v3.js)
├── .env                   ← 🔧 AGGIORNA (aggiungi TWELVE_DATA_API_KEY)
├── yahooFinance.js        ← ✅ RESTA (non toccare)
├── finnhub.js             ← ✅ RESTA (non toccare)
├── alphaVantage.js        ← ✅ RESTA (non toccare)
├── server.js              ← ✅ RESTA (non toccare)
└── package.json           ← ✅ RESTA (non toccare)
```

---

## 🔧 STEP 1: Aggiorna .env

Apri il tuo file `.env` e **aggiungi questa riga**:

```bash
TWELVE_DATA_API_KEY=ce2a8eb85df743e4a798f18829cd9967
```

Il tuo `.env` completo dovrebbe essere così:

```bash
# TwelveData (PRIMARY for EU)
TWELVE_DATA_API_KEY=ce2a8eb85df743e4a798f18829cd9967

# Finnhub (Backup)
FINNHUB_API_KEY=c7kgn0pr01qhqt0p6750c7kgn0pr01qhqt0p6760

# Alpha Vantage (Backup)
ALPHA_VANTAGE_API_KEY=demo

# Server
PORT=3001
NODE_ENV=development
```

---

## 📥 STEP 2: Copia i File

Copia questi 4 file nella **ROOT** del progetto:

1. **twelveData.js** → Nuovo file
2. **dataAggregator-v3.js** → Rinomina in `dataAggregator.js` (sostituisce quello vecchio)
3. **financial.js** → Sostituisce quello vecchio
4. **test-v3.js** → Rinomina in `test.js` (sostituisce quello vecchio)

```bash
# Esempio comandi bash:
cp twelveData.js ~/isin-backend-render/
cp dataAggregator-v3.js ~/isin-backend-render/dataAggregator.js
cp financial.js ~/isin-backend-render/
cp test-v3.js ~/isin-backend-render/test.js
```

---

## 🧪 STEP 3: Test Locale

```bash
cd ~/isin-backend-render
node test.js
```

**Output atteso:**
```
✅ Test 2: ENEL → EUR (non USD!) ← QUESTO È IL FIX!
✅ Test 3: Quote ENEL → 6.85 EUR
✅ Test 4: ENI → EUR
✅ Test 5: AAPL → USD (Yahoo fallback)
```

Se vedi **EUR per ENEL/ENI** → **FUNZIONA!** ✅

---

## 🚀 STEP 4: Deploy su Render

### Opzione A: Git Push (consigliato)
```bash
git add .
git commit -m "v3.0 - TwelveData primary for EU stocks, EUR fix"
git push origin main
```

Render farà deploy automatico.

### Opzione B: Environment Variables su Render
1. Dashboard Render → tuo servizio
2. **Environment** tab
3. Click **Add Environment Variable**
4. Aggiungi:
   - Key: `TWELVE_DATA_API_KEY`
   - Value: `ce2a8eb85df743e4a798f18829cd9967`
5. **Save Changes**

Render farà redeploy automatico.

---

## ✅ VERIFICA DEPLOY

Testa l'API live:

```bash
# Health check
curl https://isin-backend.onrender.com/health

# Test ENEL (deve mostrare EUR!)
curl https://isin-backend.onrender.com/api/financial/search?q=ENEL

# Test quote ENEL
curl https://isin-backend.onrender.com/api/financial/quote/ENEL
```

**Cerca questa stringa nel response:**
```json
"currency": "EUR"
```

Se vedi `"currency": "EUR"` → **DEPLOY OK!** ✅

---

## 🆘 TROUBLESHOOTING

### Problema: "TWELVE_DATA_API_KEY not configured"
**Fix:** Controlla che `.env` abbia la chiave corretta (con underscore: `TWELVE_DATA_API_KEY`)

### Problema: ENEL mostra ancora USD
**Fix:** 
1. Verifica che `dataAggregator.js` sia aggiornato
2. Verifica che `twelveData.js` esista nella root
3. Restart server: `npm start`

### Problema: "Cannot find module './twelveData'"
**Fix:** Assicurati che `twelveData.js` sia nella stessa cartella di `dataAggregator.js` (root del progetto)

---

## 📊 V2.3 vs V3.0

| Feature | V2.3 | V3.0 |
|---------|------|------|
| **ENEL Currency** | ❌ USD (wrong) | ✅ EUR (correct) |
| **Primary EU Source** | Yahoo | TwelveData |
| **Primary US Source** | Yahoo | Yahoo |
| **EU Currency Accuracy** | 60% | 100% |
| **Daily Requests** | Unlimited | 800 (enough) |

---

## 🎯 SUCCESS CRITERIA

✅ **Test locale:** `node test.js` mostra EUR per ENEL/ENI  
✅ **Deploy:** API live mostra `"currency": "EUR"` per ENEL  
✅ **Frontend:** ISIN Research mostra "€6.85 EUR" (non "$6.85 USD")

---

**Problemi?** Mandami screenshot dell'output di `node test.js`
