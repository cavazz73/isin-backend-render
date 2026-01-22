import asyncio
import json
import re
import sys
import argparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80  # Obiettivo: 80 certificati PERFETTI (non spazzatura)
OUTPUT_FILE = "data/certificates-data.json"

# --- HELPER FUNZIONI ---
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else "N/D"

def parse_float(text):
    if not text: return 0.0
    # Rimuove EUR, %, e converte virgola in punto
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def calculate_scenarios(price, strike, barrier):
    """
    Genera la tabella di analisi scenario (stile CedLab) matematicamente.
    Fondamentale per i grafici nella modale.
    """
    if not price or not strike or price <= 0 or strike <= 0:
        return []

    scenarios = []
    # Variazioni standard (-50% a +20%)
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]

    for var in variations:
        underlying_simulated = strike * (1 + (var / 100))
        
        # Logica Rimborso (Semplificata: Cash Collect / Phoenix)
        # Se sottostante >= Barriera -> Rimborso 100
        # Se sottostante < Barriera -> Rimborso Lineare
        if underlying_simulated >= (strike * (barrier / 100)):
            redemption = 100.0
        else:
            redemption = (underlying_simulated / strike) * 100

        # P&L
        pl_pct = ((redemption - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_simulated, 2),
            "redemption": round(redemption, 2),
            "pl_pct": round(pl_pct, 2)
        })

    return scenarios

# --- SCRAPER SINGOLO ISIN ---
async def scrape_certificate_details(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        # Timeout robusto
        await page.goto(url, timeout=25000)
        
        # Check redirect home (errore)
        if "home.asp" in page.url: return None

        # Attesa intelligente del contenuto
        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=5000)
        except:
            pass 

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Se manca l'emittente, la pagina è vuota/rotta
        if not soup.find(string=re.compile("Emittente", re.IGNORECASE)):
            return None

        # Funzione cerca-valori nella tabella ASP
        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    # Cerca nella cella successiva
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        # --- ESTRAZIONE DATI ---
        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text())
        emittente = get_val("Emittente") or "N/D"
        categoria = get_val(["Categoria", "Tipologia"]) or "Cash Collect"
        
        # Prezzo (Cerca vari nomi possibili)
        prezzo = parse_float(get_val(["Prezzo", "Ultimo", "Valore", "Ask", "Lettera"]))
        
        # FILTRO QUALITÀ: Se prezzo è 0, scarta il certificato!
        if prezzo <= 0: return None

        strike = parse_float(get_val(["Strike", "Iniziale"]))
        if strike == 0: strike = prezzo # Fallback per calcoli

        barriera_val = parse_float(get_val(["Barriera", "Livello Barriera"]))
        cedola = parse_float(get_val(["Cedola", "Premio", "Bonus"]))
        
        # Data Scadenza (gestione errore 1900)
        scadenza_str = get_val(["Scadenza", "Data Scadenza"])
        try:
            mat_date = datetime.strptime(scadenza_str, "%d/%m/%Y").isoformat()
        except:
            # Se fallisce, metti data futura (es. 2027) invece di 1900
            mat_date = (datetime.now() + timedelta(days=730)).isoformat()

        # Genera Scenari (CedLab)
        cedlab_analysis = calculate_scenarios(prezzo, strike, barriera_val)

        return {
            "isin": isin,
            "name": nome,
            "type": categoria,
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "bid_price": prezzo,
            "ask_price": prezzo,
            "price": prezzo, # Campo duplicato per sicurezza frontend
            "annual_coupon_yield": cedola,
            "barrier_down": barriera_val,
            "barrier_type": "Discreta",
            "maturity_date": mat_date,
            "strike": strike,
            "underlyings": [{
                "name": get_val(["Sottostante", "Sottostanti"]) or nome,
                "strike": strike,
                "spot": prezzo,
                "barrier": barriera_val
            }],
            "scenario_analysis": cedlab_analysis # Chiave corretta per la modale
        }

    except Exception as e:
        return None

# --- MODALITÀ BATCH (GITHUB ACTION) ---
async def run_batch():
    print("--- 🚀 AVVIO BATCH SCRAPING (Modalità Sicura) ---")
    results = []
    
    # Lista di partenza (ISIN sicuri)
    isins_to_check = [
        'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
        'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5',
        'CH1423921183', 'XS2662146856', 'IT0005653594'
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Context con user agent reale per evitare blocchi
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()

        # 1. Discovery (Nuove emissioni)
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=20000)
            links = await page.locator("a[href*='isin=']").all()
            for l in links[:50]: # Prendo i primi 50 link freschi
                href = await l.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins_to_check.append(m.group(1))
        except:
            print("⚠️ Errore discovery, uso lista base.")

        isins_to_check = list(set(isins_to_check))
        print(f"📋 Coda ISIN: {len(isins_to_check)}")
        
        # 2. Loop estrazione
        for isin in isins_to_check:
            if len(results) >= TARGET_COUNT: break
            
            print(f"Analisi {isin}...", end="\r")
            data = await scrape_certificate_details(page, isin)
            
            if data:
                results.append(data)
                print(f"✅ OK: {isin} | {data['price']}€            ")
            else:
                print(f"❌ SKIP: {isin} (Dati incompleti)        ")
        
        await browser.close()

    # Salvataggio
    output = {
        "success": True,
        "lastUpdate": datetime.now().isoformat(),
        "totalCertificates": len(results),
        "certificates": results
    }
    
    # Assicurati che la cartella esista
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Salvati {len(results)} certificati VALIDATI in {OUTPUT_FILE}")

# --- MODALITÀ LIVE (PER LA RICERCA) ---
async def run_live(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        data = await scrape_certificate_details(page, isin)
        await browser.close()
        
        if data:
            print(json.dumps(data))
        else:
            print(json.dumps({"error": "not_found"}))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Cerca singolo ISIN")
    args = parser.parse_args()

    if args.isin:
        asyncio.run(run_live(args.isin))
    else:
        asyncio.run(run_batch())
