# 🚀 INSTALLAZIONE BOND - STRUTTURA FLAT

## ✅ Quick Start (5 minuti)

### Step 1: Installa Dipendenze

```bash
cd isin-backend-render
npm install axios cheerio
```

### Step 2: Copia File nella Root

Copia questi 3 file **nella root** del progetto (stesso livello di `server.js`):

```
isin-backend-render/
├── server.js                     ← SOSTITUISCI con server-v3.1.js
├── financial.js                  (esistente)
├── dataAggregator.js            (esistente)
├── finnhub.js                   (esistente)
├── yahooFinance.js              (esistente)
├── borsaItalianaScraper.js      ← NUOVO (copia questo)
├── bonds.js                     ← NUOVO (copia questo)
└── package.json
```

**File da copiare:**
1. `borsaItalianaScraper.js` → nella root
2. `bonds.js` → nella root  
3. `server-v3.1.js` → rinominalo in `server.js` (o copia il contenuto)

### Step 3: Test Locale

```bash
npm start
```

**Test nel browser o con curl:**

```bash
# Test 1: Health check
curl http://localhost:3001/health

# Test 2: Cerca BTP
curl http://localhost:3001/api/bonds/search?category=btp&limit=5

# Test 3: Test endpoint
curl http://localhost:3001/api/bonds/test
```

**Output atteso Test 2:**
```json
{
  "success": true,
  "category": "BTP",
  "count": 5,
  "bonds": [
    {
      "name": "BTP 01/12/30 3,45%",
      "isin": "IT0005436693",
      "price": 102.45,
      "change": 0.15,
      "yield": 3.2,
      "currency": "EUR",
      "source": "Borsa Italiana"
    }
  ]
}
```

### Step 4: Deploy su Render

```bash
git add .
git commit -m "Add bond integration v3.1"
git push origin main
```

Render farà automaticamente:
- `npm install` (con axios e cheerio)
- Deploy del nuovo codice

---

## 📋 Modifiche al server.js

Se preferisci modificare manualmente invece di sostituire, aggiungi solo queste 2 righe:

**Riga 7 (dopo `const financialRoutes`)**
```javascript
const bondsRoutes = require('./bonds'); // ← AGGIUNGI
```

**Riga 186 (dopo `app.use('/api/financial')`)**
```javascript
app.use('/api/bonds', bondsRoutes); // ← AGGIUNGI
```

Opzionale: aggiorna versione e console.log (linee 202-213).

---

## 🔧 API Endpoints Disponibili

### 1. Cerca Bond per Categoria
```
GET /api/bonds/search?category=btp&limit=20
```

Categorie valide: `btp`, `bot`, `cct`, `ctz`

### 2. Dettagli Bond Specifico
```
GET /api/bonds/details/IT0005436693?category=btp
```

### 3. Quick Access BTP
```
GET /api/bonds/btp?limit=10
```

### 4. Quick Access BOT
```
GET /api/bonds/bot?limit=10
```

### 5. Test
```
GET /api/bonds/test
```

### 6. Cache Stats
```
GET /api/bonds/cache-stats
```

---

## ✅ Checklist Post-Installazione

- [ ] `npm install axios cheerio` completato
- [ ] File copiati nella root (no cartelle)
- [ ] `server.js` modificato o sostituito
- [ ] Test locale OK (`npm start`)
- [ ] `/api/bonds/test` restituisce successo
- [ ] Git commit + push
- [ ] Deploy Render completato
- [ ] Test produzione OK

---

## 🎯 Frontend Integration (Esempio)

```javascript
// Cerca BTP
async function searchBTP() {
  const response = await fetch(
    'https://isin-backend.onrender.com/api/bonds/search?category=btp&limit=10'
  );
  const data = await response.json();
  
  if (data.success) {
    data.bonds.forEach(bond => {
      console.log(`${bond.name}: €${bond.price} (${bond.yield}%)`);
    });
  }
}

// Mostra disclaimer
function showDisclaimer() {
  return "⚠️ Dati Borsa Italiana - Delay 15min. Prezzi indicativi.";
}
```

---

## ⚠️ Disclaimer Obbligatorio

**Nel frontend, mostra sempre:**

```html
<div class="bond-disclaimer">
  ⚠️ I dati obbligazionari sono forniti da Borsa Italiana 
  con delay di 15 minuti. Verificare sempre presso fonti 
  ufficiali prima di decisioni di investimento.
</div>
```

---

## 🐛 Troubleshooting

### Errore: "Cannot find module './borsaItalianaScraper'"

**Causa:** File non nella root

**Soluzione:**
```bash
# Verifica file sia nella root
ls -la borsaItalianaScraper.js

# Deve essere nello stesso livello di server.js
```

### Errore: "axios is not defined"

**Causa:** npm install non eseguito

**Soluzione:**
```bash
npm install axios cheerio
npm start
```

### Nessun bond trovato

**Causa:** Sito Borsa Italiana temporaneamente non disponibile o cambio struttura HTML

**Soluzione:**
- Riprova dopo qualche minuto
- Verifica manualmente su borsaitaliana.it se il sito è up
- Controlla log Render per errori specifici

### Rate limit / Ban

**Causa:** Troppe richieste troppo veloci

**Soluzione:**
- Lo scraper ha rate limiting automatico (1 req/2 sec)
- Cache di 1 ora riduce carico
- Se problemi persistono, aumenta delay nel file

---

## 📊 Performance

- **Rate Limiting:** 1 richiesta ogni 2 secondi
- **Cache:** 1 ora per categoria
- **Timeout:** 10 secondi per richiesta
- **Max risultati:** 100 per chiamata

---

## 🎉 Tutto Pronto!

Una volta completati gli step sopra, il tuo backend supporta:

✅ Ricerca strumenti finanziari (esistente)  
✅ Quote real-time (esistente)  
✅ Dati storici (esistente)  
✅ **Ricerca bond italiani (NUOVO)** 🎉  
✅ **Dettagli obbligazioni (NUOVO)** 🎉  

---

**Versione:** 3.1.0  
**Data:** Dicembre 2024  
**Copyright:** Mutna S.R.L.S. (P.IVA: 04219740364)
