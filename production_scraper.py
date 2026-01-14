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

# ==========================================
# FONTE 1: CERTIFICATI (Certificati&Derivati)
# ==========================================
async def scrape_certificate_live(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        await page.goto(url, timeout=10000) # Timeout breve per live search
        if "home.asp" in page.url: return None # Redirect alla home = non trovato

        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=3000)
        except:
            pass

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        if not soup.find(string=re.compile("Emittente", re.IGNORECASE)):
            return None

        # Helper estrazione tabella
        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text())
        prezzo = parse_float(get_val(["Prezzo", "Ultimo", "Valore"]))
        
        return {
            "isin": isin,
            "name": nome,
            "type": "CERTIFICATE",
            "symbol": isin,
            "issuer": get_val("Emittente") or "N/D",
            "market": "SeDeX/Cert-X",
            "currency": "EUR",
            "price": prezzo,
            "source": "Certificati&Derivati (Live)"
        }
    except:
        return None

# ==========================================
# FONTE 2: BOND / BTP (Teleborsa - Fonte Pubblica)
# ==========================================
async def scrape_bond_live(page, isin):
    # Teleborsa è ottima per lookup rapidi su Bond e BTP se non li abbiamo nel DB
    url = f"https://www.teleborsa.it/Ricerca?q={isin}"
    try:
        await page.goto(url, timeout=10000)
        
        # Se siamo in una lista di ricerca, clicchiamo il primo risultato
        if "Ricerca?" in page.url:
            try:
                await page.click(".search-results a", timeout=2000)
            except:
                return None # Nessun risultato

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Controllo se è pagina valida
        if not soup.find("h1"): return None
        
        nome = clean_text(soup.find("h1").get_text())
        
        # Estrazione Prezzo (Generica per Teleborsa)
        prezzo = 0.0
        # Cerchiamo il prezzo grande in alto
        price_tag = soup.select_one(".price") or soup.select_one("[itemprop='price']")
        if price_tag:
            prezzo = parse_float(price_tag.get_text())

        # Capiamo se è un BTP/Bond
        tipo = "BOND"
        if "BTP" in nome.upper() or "REPUBBLICA" in nome.upper():
            tipo = "BTP/GOV"
        
        return {
            "isin": isin,
            "name": nome,
            "type": tipo,
            "symbol": isin,
            "market": "MOT/TLX",
            "currency": "EUR",
            "price": prezzo,
            "source": "Teleborsa (Live)"
        }
    except:
        return None

# ==========================================
# GESTORE DELLA RICERCA (ORCHESTRATOR)
# ==========================================
async def hunt_isin(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        result = None
        
        # 1. Prova Certificati
        # print(f"DEBUG: Cerco {isin} su Certificati...", file=sys.stderr)
        result = await scrape_certificate_live(page, isin)
        
        # 2. Se non trovato, Prova Bond
        if not result:
            # print(f"DEBUG: Cerco {isin} su Bond...", file=sys.stderr)
            result = await scrape_bond_live(page, isin)

        await browser.close()
        
        # OUTPUT JSON PURO (Fondamentale per Node.js)
        if result:
            print(json.dumps(result))
        else:
            print(json.dumps({"error": "not_found"}))

# --- LOGICA BATCH (Mantenuta per l'aggiornamento notturno) ---
async def run_batch_scraping():
    # ... (Il codice batch di prima rimane qui identico, omesso per brevità ma va incluso)
    # Se copi questo file, assicurati di includere la logica batch se vuoi che funzioni anche di notte
    # Per ora concentriamoci sulla ricerca live:
    print("Batch scraping mode not implemented in this snippet (Use previous version logic here)")

# --- ENTRY POINT ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Cerca un singolo ISIN in real-time")
    args = parser.parse_args()

    if args.isin:
        asyncio.run(hunt_isin(args.isin))
    else:
        # Qui andrebbe il batch
        print("Usa --isin [CODICE] per cercare.")
