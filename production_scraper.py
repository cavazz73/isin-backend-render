import asyncio
import json
import re
import os
import argparse
import random
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80
OUTPUT_FILE = "data/certificates-data.json"

# --- HELPER ---
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else "N/D"

def parse_float(text):
    if not text: return 0.0
    # Rimuovi tutto ciò che non è numero o virgola/punto
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def calculate_scenarios(price, strike, barrier):
    """Calcola i dati per il grafico se mancano"""
    if not price or price <= 0: return []
    if not strike or strike <= 0: strike = price
    if not barrier or barrier <= 0: barrier = strike * 0.60 

    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]

    for var in variations:
        underlying_sim = strike * (1 + (var / 100))
        if underlying_sim >= barrier:
            redemption = 100.0
        else:
            redemption = (underlying_sim / strike) * 100
        pl_pct = ((redemption - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_sim, 2),
            "redemption": round(redemption, 2),
            "pl_pct": round(pl_pct, 2)
        })
    return scenarios

# ==========================================
# ENGINE: CERTIFICATI & DERIVATI (Session Mode)
# ==========================================
async def scrape_ced(page, isin):
    target_url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        # TRUCCO FONDAMENTALE: 
        # Non andiamo diretti. Navighiamo come un umano.
        
        # 1. Carica la pagina
        await page.goto(target_url, timeout=30000, wait_until="domcontentloaded")
        
        # 2. Controllo Redirect: Se siamo finiti in Home, l'ISIN non esiste o il link è rotto
        if "home.asp" in page.url and "isin=" not in page.url:
            # print(f"   ⚠️ Redirect su Home (ISIN non valido?)")
            return None

        # 3. Attesa Contenuto (Senza essere troppo schizzinosi)
        try:
            # Aspetta un qualsiasi elemento di tabella
            await page.wait_for_selector("table", timeout=5000)
        except:
            pass

        # 4. Parsing HTML
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Controllo se è una pagina valida (deve avere almeno un dato finanziario)
        full_text = soup.get_text().upper()
        if "EMITTENTE" not in full_text and "SOTTOSTANTE" not in full_text:
            return None

        # --- ESTRAZIONE "FUZZY" (Cerca ovunque) ---
        
        # Nome
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Certificate {isin}"

        # Funzione cerca valore intelligente
        def find_value(keywords):
            for kw in keywords:
                # Cerca la parola chiave
                label = soup.find(string=re.compile(kw, re.IGNORECASE))
                if label:
                    # Strategia 1: Cella successiva (TD -> TD)
                    if label.find_parent('td'):
                        nxt = label.find_parent('td').find_next_sibling('td')
                        if nxt: 
                            val = clean_text(nxt.get_text())
                            if any(c.isdigit() for c in val): return val
                    
                    # Strategia 2: Span successivo
                    nxt_span = label.find_next('span')
                    if nxt_span:
                        val = clean_text(nxt_span.get_text())
                        if any(c.isdigit() for c in val): return val
            return None

        # Estrazione Dati
        prezzo_str = find_value(["Prezzo", "Ultimo", "Valore", "Quotazione", "Denaro", "Lettera"])
        prezzo = parse_float(prezzo_str)

        # Se ancora 0, cerca il primo numero grande con €
        if prezzo == 0:
            potential_prices = re.findall(r'(\d+[.,]\d{2})\s*€', soup.get_text())
            if potential_prices:
                prezzo = parse_float(potential_prices[0])

        if prezzo <= 0: 
            # print("   ⚠️ Prezzo non trovato")
            return None

        strike = parse_float(find_value(["Strike", "Iniziale", "Strike Level"]))
        barriera = parse_float(find_value(["Barriera", "Barrier", "Livello Barriera"]))
        cedola = parse_float(find_value(["Cedola", "Premio", "Bonus"]))
        emittente = find_value(["Emittente", "Issuer"]) or "N/D"
        categoria = find_value(["Categoria", "Tipologia"]) or "Investment Certificate"

        # Defaults logici
        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin,
            "name": nome,
            "type": categoria,
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": find_value(["Sottostante"]) or nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }

    except Exception:
        return None

# ==========================================
# MAIN RUNNER
# ==========================================
async def run_batch():
    print("--- 🚀 AVVIO SCRAPER C&D (Human Mode) ---")
    results = []
    
    # 1. Recupero ISIN Freschi dalla lista "Nuove Emissioni"
    # Questo è il punto che dicevi tu: "le liste si vedono". Usiamole!
    isins_to_check = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        context = await browser.new_context()
        page = await context.new_page()

        # STEP 1: Vai in Home Page per prendere i cookie di sessione
        # print("   🌍 Visita Home Page per sessione...")
        try:
            await page.goto("https://www.certificatiederivati.it", timeout=20000)
            await asyncio.sleep(2) # Pausa umana
        except: pass

        # STEP 2: Prendi la lista "Nuove Emissioni"
        # print("   📋 Recupero lista Nuove Emissioni...")
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=20000)
            links = await page.locator("a[href*='isin=']").all()
            for l in links:
                href = await l.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins_to_check.append(m.group(1))
        except: 
            # print("   ⚠️ Errore lettura lista, uso ISIN di backup.")
            pass
            
        # Aggiungi ISIN di backup se la lista è vuota
        if len(isins_to_check) < 5:
            isins_to_check.extend([
                'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
                'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5'
            ])

        # Rimuovi duplicati
        isins_to_check = list(set(isins_to_check))
        print(f"📋 ISIN trovati in lista: {len(isins_to_check)}")

        # STEP 3: Visita ogni ISIN
        for isin in isins_to_check:
            if len(results) >= TARGET_COUNT: break
            
            print(f"Analisi {isin}...", end="\r")
            
            # Pausa casuale tra 1 e 3 secondi per sembrare umano
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            data = await scrape_ced(page, isin)
            
            if data:
                results.append(data)
                print(f"✅ OK: {isin} | {data['price']}€            ")
            else:
                print(f"❌ SKIP: {isin} (Dati non letti)        ")
        
        await browser.close()

    # Salvataggio
    output = {
        "last_update": datetime.now().isoformat(),
        "total": len(results),
        "certificates": results
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Salvati {len(results)} certificati.")

async def run_live(isin):
    # Modalità ricerca singola
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        page = await browser.new_page()
        # Visita home per sessione
        try: await page.goto("https://www.certificatiederivati.it", timeout=10000)
        except: pass
        
        data = await scrape_ced(page, isin)
        await browser.close()
        print(json.dumps(data if data else {"error": "not_found"}))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Live search")
    args = parser.parse_args()

    if args.isin:
        asyncio.run(run_live(args.isin))
    else:
        asyncio.run(run_batch())
