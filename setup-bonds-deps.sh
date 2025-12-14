#!/bin/bash

# Update package.json for bonds scraper dependencies

echo "Updating package.json with bonds scraper dependencies..."

# Check if package.json exists
if [ ! -f "package.json" ]; then
    echo "Error: package.json not found"
    exit 1
fi

# Add axios and cheerio if not present
npm install --save axios cheerio

echo "✓ Dependencies installed"
echo ""
echo "Installed packages:"
echo "  - axios (HTTP client for scraping)"
echo "  - cheerio (HTML parsing)"
echo ""
echo "Next steps:"
echo "  1. Test scraper: node test-bonds-scraper.js"
echo "  2. Run manual scrape: node bonds-scraper-simpletoolsforinvestors.js"
echo "  3. Commit and push to trigger GitHub Action"
