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
ERROR_DIR = "debug_screenshots" # Cartella dove salverà le foto degli errori

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

async def handle_cookies(page):
    """Tentativo aggressivo di chiudere i cookie"""
    try:
        # Iubenda (Teleborsa)
        if await page.is_visible("#iubenda-cs-banner", timeout=2000):
            await page.click(".iubenda-cs-accept-btn", timeout=1000)
            await asyncio.sleep(0.5)
            return

        # Generico
        common_selectors = [
            "text=Accetta tutto", "text=Acconsento", "text=Accept All", 
            "button[class*='cookie']", "#cookie-banner button"
        ]
        for sel in common_selectors:
            if await page.is_visible(sel, timeout=500):
                await page.click(sel)
                return
    except: pass

# ==========================================
# ENGINE 1: TELEBORSA (Refined)
# ==========================================
async def scrape_teleborsa(page, isin):
    url = f"https://www.teleborsa.it/Ricerca?q={isin}"
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        await handle_cookies(page)

        # Se siamo nella lista di ricerca
        if "Ricerca" in page.url:
            try:
                # Cerca link con ISIN
                link_selector = f"a[href*='{isin.lower()}'], a[href*='{isin.upper()}']"
                await page.wait_for_selector(link_selector, timeout=5000)
                await page.click(link_selector)
                await page.wait_for_load_state("domcontentloaded")
            except:
                # Screenshot debug se fallisce il click (spesso colpa dei cookie)
                await page.screenshot(path=f"{ERROR_DIR}/fail_search_{isin}.png")
                return None

        # Attesa esplicita del prezzo
        try:
            # Teleborsa price classes: .t-text-xxl, or itemprop="price"
            await page.wait_for_selector(".t-text-xxl, [itemprop='price']", timeout=5000)
        except:
            # Se timeout, forse pagina bianca o blocco
            await page.screenshot(path=f"{ERROR_DIR}/fail_load_{isin}.png")
            return None

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Nome
        h1 = soup.find('h1')
        nome = clean_text(h1.get_text()) if h1 else "N/D"

        # Prezzo
        prezzo = 0.0
        p_tag = soup.select_one(".t-text-xxl") or soup.select_one("[itemprop='price']")
        if p_tag: prezzo = parse_float(p_tag.get_text())

        if prezzo <= 0: 
            # Ultimo tentativo: cerca "Prezzo" nel testo
            x = soup.find(string=re.compile("Prezzo|Ultimo"))
            if x: 
                v = x.find_next("span")
                if v: prezzo = parse_float(v.get_text())

        if prezzo <= 0: return None

        # Dati Tecnici
        strike = 0.0
        barriera = 0.0
        cedola = 0.0
        
        # Scansiona tabelle
        for td in soup.find_all("td"):
            txt = clean_text(td.get_text()).upper()
            val_tag = td.find_next_sibling("td")
            if val_tag:
                val = parse_float(val_tag.get_text())
                if "CEDOLA" in txt: cedola = val
                if "STRIKE" in txt or "LIVELLO INIZIALE" in txt: strike = val
                if "BARRIERA" in txt: barriera = val

        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

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
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Teleborsa"
        }

    except Exception as e:
        return None

# ==========================================
# ENGINE 2: CED (Refined)
# ==========================================
async def scrape_ced(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        
        if "home.asp" in page.url: return None 

        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=5000)
        except: pass

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        if not soup.find(string=re.compile("Emittente", re.IGNORECASE)):
            await page.screenshot(path=f"{ERROR_DIR}/fail_ced_{isin}.png")
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
        # Aggiunti Denaro/Lettera/Quotazione per robustezza
        prezzo = parse_float(get_val(["Prezzo", "Ultimo", "Valore", "Quotazione", "Denaro", "Lettera"]))
        
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
# ORCHESTRATOR
# ==========================================
async def process_isin(page, isin):
    # Teleborsa
    data = await scrape_teleborsa(page, isin)
    if data: return data
    
    # CED
    data = await scrape_ced(page, isin)
    if data: return data

    return None

async def run_batch():
    print("--- 🚀 AVVIO DIAGNOSTIC SCRAPER (With Screenshots) ---")
    
    if not os.path.exists(ERROR_DIR):
        os.makedirs(ERROR_DIR)
        
    results = []
    
    # Lista ISIN
    isins = [
        'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
        'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5',
        'CH1423921183', 'XS2662146856', 'IT0005653594', 'DE000VM4X559',
        'IT0005542997', 'IT0005546949', 'XS2377640135', 'IT0006769746'
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            locale='it-IT'
        )
        
        # Trucco per nascondere il driver al sito
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        page = await context.new_page()

        # Discovery
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=15000)
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
                print(f"❌ SKIP: {isin} (Vedi screenshot in {ERROR_DIR})          ")
        
        await browser.close()

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
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        context = await browser.new_context()
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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
