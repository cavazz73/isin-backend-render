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
    """Genera scenari matematici per popolare i grafici (CedLab style)"""
    if not price or price <= 0: return []
    
    # Defaults intelligenti se mancano i dati tecnici
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
# ENGINE 1: TELEBORSA (Veloce e Stabile)
# ==========================================
async def scrape_teleborsa(page, isin):
    url = f"https://www.teleborsa.it/Ricerca?q={isin}"
    try:
        # 1. Cerca
        await page.goto(url, timeout=15000)
        
        # 2. Gestione Lista Risultati (Il punto dove falliva prima)
        if "Ricerca" in page.url:
            # Cerca un link che contenga l'ISIN nell'URL o nel testo
            try:
                # Selettore intelligente: link che ha l'ISIN nell'href
                link_selector = f"a[href*='{isin.lower()}'], a[href*='{isin.upper()}']"
                await page.wait_for_selector(link_selector, timeout=3000)
                await page.click(link_selector)
                await page.wait_for_load_state("domcontentloaded")
            except:
                # Fallback: clicca il primo risultato della tabella se esiste
                try:
                    await page.click(".search-results a, table a", timeout=2000)
                except:
                    return None # Non trovato nemmeno nella lista

        # 3. Parsing Scheda
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Nome
        h1 = soup.find('h1')
        if not h1: return None
        nome = clean_text(h1.get_text())

        # Prezzo
        prezzo_tag = soup.select_one(".t-text-xxl") or soup.select_one("[itemprop='price']") or soup.find(string=re.compile("Prezzo")).find_next("span")
        prezzo = parse_float(prezzo_tag.get_text()) if prezzo_tag else 0.0

        if prezzo <= 0: return None # Prezzo non trovato

        # Dati Tecnici (Tentativo estrazione tabella)
        strike = 0.0
        barriera = 0.0
        cedola = 0.0
        
        # Scansiona tutte le celle per trovare keyword
        for td in soup.find_all("td"):
            txt = clean_text(td.get_text()).upper()
            val_tag = td.find_next_sibling("td")
            if val_tag:
                val = parse_float(val_tag.get_text())
                if "CEDOLA" in txt: cedola = val
                if "STRIKE" in txt or "LIVELLO INIZIALE" in txt: strike = val
                if "BARRIERA" in txt: barriera = val

        # Fallback se non trovati
        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin,
            "name": nome,
            "type": "Investment Certificate",
            "issuer": "N/D", # Teleborsa spesso non lo dice esplicitamente in testo semplice
            "market": "SeDeX/EuroTLX",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Teleborsa"
        }

    except Exception as e:
        # print(f"Teleborsa Error {isin}: {e}")
        return None

# ==========================================
# ENGINE 2: CERTIFICATI & DERIVATI (Dettagliato)
# ==========================================
async def scrape_ced(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        await page.goto(url, timeout=15000)
        if "home.asp" in page.url: return None # Redirect home = non trovato

        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=4000)
        except: pass

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Controllo validità
        if not soup.find(string=re.compile("Emittente|Sottostante", re.IGNORECASE)):
            return None

        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text())
        prezzo = parse_float(get_val(["Prezzo", "Ultimo", "Valore", "Quotazione"]))
        
        if prezzo <= 0: return None

        strike = parse_float(get_val(["Strike", "Iniziale"]))
        barriera = parse_float(get_val(["Barriera", "Livello Barriera"]))
        cedola = parse_float(get_val(["Cedola", "Premio"]))
        emittente = get_val("Emittente") or "N/D"

        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin,
            "name": nome,
            "type": get_val("Categoria") or "Certificate",
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
            "underlyings": [{"name": get_val("Sottostante") or nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }

    except Exception:
        return None

# ==========================================
# ORCHESTRATOR (Proviamo tutto!)
# ==========================================
async def process_isin(page, isin):
    # Tentativo 1: Teleborsa
    data = await scrape_teleborsa(page, isin)
    if data: return data
    
    # Tentativo 2: Certificati & Derivati
    # print(f"   -> Fallback su CED per {isin}...")
    data = await scrape_ced(page, isin)
    if data: return data

    return None

async def run_batch():
    print("--- 🚀 AVVIO HYBRID SCRAPER (Teleborsa + CED) ---")
    results = []
    
    # Lista ISIN da monitorare
    isins = [
        'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
        'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5',
        'CH1423921183', 'XS2662146856', 'IT0005653594', 'DE000VM4X559',
        'IT0005542997', 'IT0005546949', 'XS2377640135', 'IT0006769746'
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # User Agent Reale per evitare blocchi
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()

        # Discovery Rapida (Opzionale, aggiunge ISIN freschi)
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=10000)
            links = await page.locator("a[href*='isin=']").all()
            for l in links[:30]:
                href = await l.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins.append(m.group(1))
        except: pass

        isins = list(set(isins))
        print(f"📋 Coda Analisi: {len(isins)} ISIN")
        
        for isin in isins:
            if len(results) >= TARGET_COUNT: break
            
            print(f"Analisi {isin}...", end="\r")
            data = await process_isin(page, isin)
            
            if data:
                results.append(data)
                print(f"✅ OK: {isin} | {data['price']}€ ({data['source']})          ")
            else:
                print(f"❌ SKIP: {isin} (Non trovato su nessuna fonte)      ")
        
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
    
    print(f"\n💾 Salvataggio completato: {len(results)} certificati.")

async def run_live(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()
        data = await process_isin(page, isin)
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
