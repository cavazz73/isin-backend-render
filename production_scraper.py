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
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def calculate_scenarios(price, strike, barrier):
    if not price or price <= 0: return []
    if not strike or strike <= 0: strike = price
    if not barrier or barrier <= 0: barrier = strike * 0.60 
    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]
    for var in variations:
        underlying_sim = strike * (1 + (var / 100))
        if underlying_sim >= barrier: redemption = 100.0
        else: redemption = (underlying_sim / strike) * 100
        pl_pct = ((redemption - price) / price) * 100
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_sim, 2),
            "redemption": round(redemption, 2),
            "pl_pct": round(pl_pct, 2)
        })
    return scenarios

# ==========================================
# MOTORE DI SCRAPING CON DIAGNOSTICA
# ==========================================
async def scrape_ced_diagnostic(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    print(f"   🕵️  [DIAGNOSTICA] Navigo su: {url}")
    
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        
        # STAMPA COSA VEDE IL BOT (Titolo Pagina)
        title = await page.title()
        print(f"   👀 [VISTA BOT] Titolo Pagina: '{title}'")
        
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # STAMPA LE PRIME 100 LETTERE DI TESTO
        text_preview = clean_text(soup.get_text())[:150]
        print(f"   👀 [VISTA BOT] Testo Iniziale: '{text_preview}...'")

        # Controlli specifici
        if "home.asp" in page.url:
            print(f"   ⚠️ [FAIL] Redirect rilevato verso Home Page.")
            return None

        has_emittente = soup.find(string=re.compile("Emittente", re.IGNORECASE))
        if not has_emittente:
            print(f"   ⚠️ [FAIL] Parola 'Emittente' NON trovata nella pagina.")
            return None

        # Prova estrazione prezzo
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Certificate {isin}"
        
        # Cerca prezzo
        raw_price_labels = soup.find_all(string=re.compile("Prezzo|Ultimo|Valore"))
        print(f"   🔎 [DEBUG PREZZO] Trovate {len(raw_price_labels)} etichette 'Prezzo'.")
        
        prezzo = 0.0
        # Logica di estrazione semplice
        for label in raw_price_labels:
            if label.find_parent('td'):
                nxt = label.find_parent('td').find_next_sibling('td')
                if nxt:
                    val = clean_text(nxt.get_text())
                    print(f"      -> Etichetta '{label}' ha valore vicino: '{val}'")
                    p = parse_float(val)
                    if p > 0: 
                        prezzo = p
                        break
        
        if prezzo <= 0:
            print(f"   ❌ [FAIL] Prezzo finale è 0.")
            return None
            
        print(f"   ✅ [SUCCESS] Prezzo trovato: {prezzo}")

        # Dati base (usiamo default se mancano per test)
        strike = prezzo
        barriera = prezzo * 0.60
        
        return {
            "isin": isin,
            "name": nome,
            "type": "Certificate",
            "issuer": "N/D",
            "market": "SeDeX",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "annual_coupon_yield": 0,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }

    except Exception as e:
        print(f"   🔥 [ERRORE CRITICO] {e}")
        return None

# ==========================================
# RUNNER
# ==========================================
async def run_batch():
    print("--- 🚀 AVVIO SCRAPER SPIA (DIAGNOSTICA) ---")
    results = []
    
    # Usiamo solo 3 ISIN per il test rapido, così non aspetti 20 minuti
    isins = ['IT0006771510', 'DE000HD8SXZ1', 'XS2470031936']
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        page = await browser.new_page()

        # STEP 1: Visita Home
        print("   🌍 Visita Home Page...")
        try: await page.goto("https://www.certificatiederivati.it", timeout=15000)
        except: pass

        for isin in isins:
            print(f"\nAnalisi {isin}...")
            data = await scrape_ced_diagnostic(page, isin)
            if data:
                results.append(data)
                print(f"✅ SALVATO: {isin}")
            else:
                print(f"❌ SCARTATO: {isin}")
            await asyncio.sleep(2)
        
        await browser.close()

    # Output
    output = {
        "last_update": datetime.now().isoformat(),
        "total": len(results),
        "certificates": results
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    asyncio.run(run_batch())
