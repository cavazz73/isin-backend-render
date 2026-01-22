import asyncio
import json
import re
import os
import argparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 60
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
    """Genera scenari matematici per popolare i grafici (CedLab style)"""
    if not price or price <= 0: return []
    
    # Se mancano strike/barriera (Teleborsa a volte non li ha), usiamo default logici
    # per evitare che il frontend esploda.
    if not strike or strike <= 0: strike = price
    if not barrier or barrier <= 0: barrier = strike * 0.60 # Ipotizziamo barriera al 60%

    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]

    for var in variations:
        underlying_sim = strike * (1 + (var / 100))
        
        # Logica Rimborso Semplificata
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

# --- MOTORE TELEBORSA ---
async def scrape_teleborsa(page, isin):
    # URL di ricerca diretta
    url = f"https://www.teleborsa.it/Ricerca?q={isin}"
    
    try:
        await page.goto(url, timeout=15000)
        
        # Gestione Redirect: A volte la ricerca porta a una lista, a volte alla scheda
        if "Ricerca" in page.url:
            # Siamo nella lista, proviamo a cliccare il primo risultato
            try:
                await page.click(".search-results a", timeout=3000)
                await page.wait_for_load_state("networkidle")
            except:
                pass # Magari non c'era la lista, proviamo a leggere

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # 1. Estrazione Nome (H1 o Title)
        h1 = soup.find('h1')
        if not h1: return None # Pagina non valida
        nome = clean_text(h1.get_text())

        # 2. Estrazione Prezzo
        # Teleborsa usa spesso classi come 't-text-xxl' per il prezzo grande
        prezzo_tag = soup.select_one(".t-text-xxl") or soup.select_one("[itemprop='price']")
        prezzo = parse_float(prezzo_tag.get_text()) if prezzo_tag else 0.0

        if prezzo <= 0: return None # Niente prezzo, niente certificato

        # 3. Dati Tecnici (Spesso non espliciti su Teleborsa, usiamo stime per ora)
        # Cerchiamo di dedurre l'emittente dal nome
        emittente = "N/D"
        for bank in ["Vontobel", "BNP", "Unicredit", "Intesa", "Societe Generale", "Leoneteq", "Citi"]:
            if bank.upper() in nome.upper():
                emittente = bank
                break

        # Defaults per il grafico
        strike = prezzo 
        barriera = prezzo * 0.60
        cedola = 0.0

        # Tentativo di trovare valori reali nella pagina (se presenti in tabella)
        # Teleborsa a volte ha tabelle con "Dati Finanziari"
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    lbl = clean_text(cols[0].get_text()).upper()
                    val = clean_text(cols[1].get_text())
                    if "CEDOLA" in lbl: cedola = parse_float(val)
                    if "STRIKE" in lbl: strike = parse_float(val)
                    if "BARRIERA" in lbl: barriera = parse_float(val)

        # Scenari
        scenarios = calculate_scenarios(prezzo, strike, barriera)

        return {
            "isin": isin,
            "name": nome,
            "type": "Investment Certificate",
            "issuer": emittente,
            "market": "SeDeX/Cert-X",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "strike": strike,
            "barrier_down": barriera,
            "annual_coupon_yield": cedola,
            "maturity_date": (datetime.now() + timedelta(days=365*2)).isoformat(), # Default 2 anni
            "scenario_analysis": scenarios,
            "source": "Teleborsa"
        }

    except Exception as e:
        # print(f"Errore su {isin}: {e}")
        return None

# --- BATCH RUNNER ---
async def run_batch():
    print("--- 🚀 AVVIO BATCH SCRAPING (Engine: Teleborsa) ---")
    results = []
    
    # Lista ISIN Mista (Certificati vari)
    isins_to_check = [
        'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
        'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5',
        'CH1423921183', 'XS2662146856', 'IT0005653594', 'DE000VM4X559',
        'IT0005542997', 'IT0005546949' 
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Discovery (opzionale, per ora usiamo la lista fissa per stabilità)
        # Se volessimo discovery su Teleborsa è più complesso, meglio lista fissa + manuale
        
        count = 0
        for isin in isins_to_check:
            if count >= TARGET_COUNT: break
            
            print(f"Analisi {isin}...", end="\r")
            try:
                data = await scrape_teleborsa(page, isin)
                if data:
                    results.append(data)
                    count += 1
                    print(f"✅ OK: {isin} | {data['price']}€            ")
                else:
                    print(f"❌ SKIP: {isin} (Non trovato)          ")
            except:
                print(f"⚠️ ERR: {isin}                          ")

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

# --- LIVE RUNNER ---
async def run_live(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        data = await scrape_teleborsa(page, isin)
        await browser.close()
        if data:
            print(json.dumps(data))
        else:
            print(json.dumps({"error": "not_found"}))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Live search ISIN")
    args = parser.parse_args()

    if args.isin:
        asyncio.run(run_live(args.isin))
    else:
        asyncio.run(run_batch())
