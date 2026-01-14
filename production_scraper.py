import asyncio
import json
import os
import re
import argparse
import sys
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80  # Abbassiamo leggermente per garantire il successo
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

# --- MOTORE DI SCRAPING (CERTIFICATI & DERIVATI) ---
async def scrape_ced_details(page, isin):
    """Estrae dati da Certificati e Derivati con timeout aggressivi"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        # Timeout ridotto a 15s per evitare blocchi infiniti
        await page.goto(url, timeout=15000)
        
        # Se siamo finiti in home o pagina errore
        if "home.asp" in page.url: return None

        # Aspetta un elemento chiave (massimo 3 secondi)
        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=3000)
        except:
            pass # Proviamo a parsare lo stesso

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Verifica validità pagina
        if not soup.find(string=re.compile("Emittente", re.IGNORECASE)):
            return None

        # Estrazione Helper
        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        # Dati
        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text())
        prezzo = parse_float(get_val(["Prezzo", "Ultimo", "Valore"]))
        strike = parse_float(get_val("Strike") or get_val("Iniziale"))
        barriera = parse_float(get_val("Barriera"))
        cedola = parse_float(get_val(["Cedola", "Premio"]))
        
        # Costruzione Oggetto Standardizzato
        return {
            "isin": isin,
            "name": nome,
            "type": "CERTIFICATE",
            "symbol": isin, # Per compatibilità frontend
            "issuer": get_val("Emittente") or "N/D",
            "market": "SeDeX/Cert-X",
            "currency": "EUR",
            "price": prezzo,
            "strike": strike,
            "barrier": barriera,
            "coupon": cedola,
            "source": "Certificati&Derivati",
            "details": { # Dettagli extra per la modale
                "strike": strike,
                "barrier": barriera,
                "coupon": cedola,
                "category": get_val(["Categoria", "Tipologia"])
            }
        }
    except Exception:
        return None

# --- MOTORE BATCH (PER IL WORKFLOW NOTTURNO) ---
async def run_batch_scraping():
    print("--- 🚀 AVVIO BATCH SCRAPING (Certificati) ---")
    results = []
    
    # Lista Seed (Sicuri) + Tentativo Discovery
    isins_to_check = [
        'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
        'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5'
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1. Discovery Rapida (Nuove Emissioni)
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=20000)
            links = await page.locator("a[href*='isin=']").all()
            for link in links:
                href = await link.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins_to_check.append(m.group(1))
        except:
            print("⚠️ Discovery fallita, uso solo lista seed.")

        # Rimuovi duplicati
        isins_to_check = list(set(isins_to_check))
        print(f"📋 ISIN in coda: {len(isins_to_check)}")

        # 2. Scraping Sequenziale
        for i, isin in enumerate(isins_to_check):
            if len(results) >= TARGET_COUNT: break
            
            print(f"[{i+1}/{len(isins_to_check)}] Analisi {isin}...", end="\r")
            data = await scrape_ced_details(page, isin)
            if data:
                results.append(data)
                print(f"✅ PRESO: {isin} | {data['price']}€            ")
            else:
                print(f"❌ SKIP: {isin}                        ")

        await browser.close()

    # Salvataggio
    output = {
        "last_update": datetime.now().isoformat(),
        "count": len(results),
        "certificates": results
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    print(f"\n💾 Salvati {len(results)} certificati.")

# --- MOTORE LIVE (PER LA RICERCA UTENTE) ---
async def run_live_lookup(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Qui potremmo aggiungere anche logica per cercare Bond su Teleborsa
        # se volessimo un fallback python anche per i bond.
        # Per ora cerchiamo certificati.
        
        result = await scrape_ced_details(page, isin)
        await browser.close()
        
        if result:
            print(json.dumps(result))
        else:
            # Output JSON vuoto valido per non rompere il parser Node
            print(json.dumps({"error": "not_found"}))

# --- MAIN ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Cerca un singolo ISIN (Live Mode)")
    args = parser.parse_args()

    if args.isin:
        # Modo Live (chiamato da Node.js)
        asyncio.run(run_live_lookup(args.isin))
    else:
        # Modo Batch (Github Actions)
        asyncio.run(run_batch_scraping())
