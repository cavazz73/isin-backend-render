name: Scrape CED Certificates
on:
  workflow_dispatch:
  schedule:
    - cron: '0 9 * * 1-5'
  push:
    paths:
      - 'certificates-scraper-ced-v15.py'
jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python 3.11
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install Python dependencies
      run: |
        pip install playwright pandas beautifulsoup4 lxml
    
    - name: Install Playwright Chromium
      run: |
        playwright install --with-deps chromium
    
    - name: Run CED scraper
      env:
        RECENT_DAYS: 30
        MAX_DETAIL_ISIN: 100
      run: |
        echo "🚀 Avvio scraper CED (recenti ultimi ${RECENT_DAYS}gg)"
        ls -lh certificates-scraper-ced-v*.py
        python certificates-scraper-ced-v15.py
    
    - name: Verify output files
      run: |
        if [ ! -f "certificates-data.json" ]; then
          echo "❌ ERRORE: certificates-data.json NON generato!"
          exit 1
        fi
        echo "✅ File generato correttamente"
        COUNT=$(jq '.count' certificates-data.json)
        echo "📊 Totale certificati validi: ${COUNT}"
        jq '.metadata' certificates-data.json
    
    - name: Commit and push results
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action Bot"
        git add certificates-data.json certificates-recenti.json certificates-recenti.csv
        git diff --staged --quiet || git commit -m "🤖 Update certificates data $(date +%Y-%m-%d-%H%M)"
        git push
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
