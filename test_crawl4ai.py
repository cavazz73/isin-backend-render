"""
Crawl4AI Certificate Scraper
Extracts REAL data from CertificatieDerivati.it
"""

import asyncio
import json
from datetime import datetime
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

async def scrape_certificate(isin):
    """Extract data for single certificate"""
    
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    # Configure browser
    browser_config = BrowserConfig(
        headless=True,
        verbose=False
    )
    
    # Configure crawler
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for_images=False,
        page_timeout=15000
    )
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(
            url=url,
            config=run_config
        )
        
        if result.success:
            # Get clean markdown
            markdown = result.markdown
            
            # Parse data from markdown
            cert = {
                'isin': isin,
                'url': url,
                'markdown_content': markdown[:500],  # First 500 chars for debug
                'scraped': True,
                'timestamp': datetime.now().isoformat()
            }
            
            # Try to extract specific fields from markdown
            text = result.cleaned_text if hasattr(result, 'cleaned_text') else markdown
            
            cert['extracted_text'] = text[:500]  # First 500 chars
            
            return cert
        else:
            return {
                'isin': isin,
                'error': result.error_message if hasattr(result, 'error_message') else 'Unknown error',
                'scraped': False
            }

async def scrape_certificates():
    """Scrape multiple certificates"""
    
    # Test ISINs (verified real)
    test_isins = [
        'IT0006771510',
        'DE000HD8SXZ1', 
        'XS2470031936',
        'CH1390857220'
    ]
    
    print(f"Testing Crawl4AI on {len(test_isins)} certificates...")
    print("")
    
    results = []
    
    for i, isin in enumerate(test_isins, 1):
        print(f"[{i}/{len(test_isins)}] Scraping {isin}...")
        
        try:
            cert = await scrape_certificate(isin)
            results.append(cert)
            
            if cert.get('scraped'):
                print(f"  ✓ Success")
            else:
                print(f"  ✗ Failed: {cert.get('error', 'Unknown')}")
                
        except Exception as e:
            print(f"  ✗ Exception: {str(e)}")
            results.append({
                'isin': isin,
                'error': str(e),
                'scraped': False
            })
        
        # Small delay between requests
        await asyncio.sleep(1)
    
    print("")
    print("=" * 60)
    print(f"RESULTS: {sum(1 for r in results if r.get('scraped'))} / {len(results)} successful")
    print("=" * 60)
    print("")
    
    # Save results
    output = {
        'success': True,
        'timestamp': datetime.now().isoformat(),
        'test_run': True,
        'total_tested': len(test_isins),
        'successful': sum(1 for r in results if r.get('scraped')),
        'failed': sum(1 for r in results if not r.get('scraped')),
        'certificates': results
    }
    
    with open('crawl4ai_test_results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("Saved results to: crawl4ai_test_results.json")
    print("")
    
    # Print sample
    if results and results[0].get('scraped'):
        print("SAMPLE OUTPUT (first certificate):")
        print("-" * 60)
        print(f"ISIN: {results[0]['isin']}")
        print(f"Markdown preview:")
        print(results[0].get('markdown_content', 'N/A'))
        print("-" * 60)
    
    return output

if __name__ == "__main__":
    asyncio.run(scrape_certificates())
