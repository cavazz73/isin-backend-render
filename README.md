# 🚀 ISIN Research Backend v2.1

## ✅ NUOVA VERSIONE CON SUPPORTO BORSA ITALIANA

### Problema Risolto
La versione precedente non riusciva a caricare i prezzi real-time per i titoli italiani (ENEL, ENI, etc.) perché Yahoo Finance restituiva 404 sulle quote.

### Soluzione: Twelve Data API
Abbiamo integrato **Twelve Data** come fonte primaria per i mercati europei:
- ✅ Supporta Borsa Italiana (MIL)
- ✅ Quote real-time per ENEL, ENI, etc.
- ✅ 800 richieste/giorno gratis
- ✅ Valuta EUR corretta

---

## 📋 COME IMPLEMENTARE

### STEP 1: Ottieni API Key Twelve Data (2 minuti)

1. Vai su: https://twelvedata.com/pricing
2. Clicca **"Start for free"**
3. Registrati con email
4. Copia la tua API key dalla dashboard

### STEP 2: Configura Variabili d'Ambiente su Render

1. Vai su: https://dashboard.render.com
2. Seleziona il tuo servizio **isin-backend**
3. **Environment** → **Add Environment Variable**
4. Aggiungi:
   ```
   TWELVE_DATA_API_KEY = tua_api_key_qui
   ```
5. Clicca **Save Changes**
6. Il servizio si riavvierà automaticamente

### STEP 3: Carica i Nuovi File

**Opzione A: GitHub (Consigliato)**
1. Scarica il pacchetto `isin-backend-v2.1`
2. Sostituisci i file nel tuo repository:
   - `twelveData.js` (NUOVO)
   - `dataAggregator.js` (AGGIORNATO)
   - `routes/financial.js` (AGGIORNATO)
   - `.env.example` (AGGIORNATO)
3. Commit e push
4. Render deploy automatico

**Opzione B: Upload Manuale**
1. Copia i file direttamente nel tuo progetto
2. Redeploy su Render

### STEP 4: Testa

Dopo il deploy, testa:
```
https://isin-backend.onrender.com/api/financial/test-italian
```

Dovresti vedere:
```json
{
  "success": true,
  "results": {
    "ENEL": {
      "success": true,
      "price": 8.98,
      "currency": "EUR",
      "source": "twelvedata"
    },
    "ENI": {
      "success": true,
      "price": 13.45,
      "currency": "EUR",
      "source": "twelvedata"
    }
  }
}
```

---

## 📊 ARCHITETTURA DATA SOURCES

### Priorità per Tipo di Mercato

**Azioni Italiane/Europee (ENEL, ENI, etc.):**
1. **Twelve Data** (primario) - Quote real-time EUR
2. Yahoo Finance (fallback) - Solo dati storici

**Azioni US (AAPL, MSFT, etc.):**
1. **Yahoo Finance** (primario) - Illimitato
2. Twelve Data (fallback)
3. Finnhub (fallback)
4. Alpha Vantage (ultimo resort)

**Dati Storici:**
1. Yahoo Finance (primario) - Migliore copertura
2. Twelve Data (fallback)
3. Alpha Vantage (ultimo resort)

### Rate Limits

| Fonte | Limite | Note |
|-------|--------|------|
| Twelve Data | 800/giorno, 8/min | Primario EU |
| Yahoo Finance | Illimitato | Primario US |
| Finnhub | 60/min | Backup |
| Alpha Vantage | 25/giorno | Ultimo resort |

---

## 📁 STRUTTURA FILE

```
isin-backend-v2.1/
├── server.js              # Server Express
├── dataAggregator.js      # Orchestratore multi-source (AGGIORNATO)
├── twelveData.js          # Client Twelve Data (NUOVO)
├── yahooFinance.js        # Client Yahoo Finance
├── finnhub.js             # Client Finnhub
├── alphaVantage.js        # Client Alpha Vantage
├── routes/
│   └── financial.js       # API Routes (AGGIORNATO)
├── .env.example           # Variabili ambiente
└── package.json           # Dipendenze
```

---

## 🔧 API ENDPOINTS

### Search
```
GET /api/financial/search?q=ENEL
```

### Quote Real-time
```
GET /api/financial/quote/ENEL.MI
GET /api/financial/quote/AAPL
```

### Historical Data
```
GET /api/financial/historical/ENEL.MI?period=1M
```

### Test Italian Stocks
```
GET /api/financial/test-italian
```

### API Usage
```
GET /api/financial/usage
```

---

## 🐛 TROUBLESHOOTING

### "Quote not found" per titoli italiani
- Verifica che `TWELVE_DATA_API_KEY` sia configurata su Render
- Controlla i log su Render Dashboard
- Testa con `/api/financial/test-italian`

### Rate limit exceeded
- Twelve Data ha limite 800 req/giorno
- Controlla usage con `/api/financial/usage`
- Il sistema fa fallback automatico su altre fonti

### Valuta sbagliata (USD invece di EUR)
- Se vedi USD per ENEL, Twelve Data non sta funzionando
- Verifica API key
- Controlla logs per errori

---

## 📞 SUPPORTO

- Email: info@mutna.it
- GitHub Issues

---

## 📜 CHANGELOG

### v2.1 (Nov 2025)
- ✅ Aggiunto Twelve Data per mercati europei
- ✅ Fix quote real-time titoli italiani
- ✅ Fix valuta EUR per Borsa Italiana
- ✅ Routing intelligente per mercato
- ✅ Nuovo endpoint `/api/financial/test-italian`
- ✅ Endpoint usage statistics

### v2.0
- Multi-source architecture
- Yahoo + Finnhub + Alpha Vantage

---

**Copyright © 2024-2025 Mutna S.R.L.S. - Tutti i diritti riservati**
