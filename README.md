# wikimedia-metadata-scraper
Short project inline with our thesis project || Image + Metadata Scraper Script
By Seedlign 😁😁😁

A small Python CLI tool to collect **open-license images** (Wikimedia Commons) and save **per-image license + attribution metadata** for dataset building.

This scraper is designed for license compliance:
- Every image gets recorded license info (`license_name`, `license_url`)
- Includes attribution text (TASL-style: Title, Author, Source, License)
- Skips unclear licenses and **NoDerivatives (ND)** licenses (if your script enforces this)

---

## Requirements

- Python 3.10+ (works on Python 3.13)
- Dependencies:
  - `requests`
  - `tqdm`
  - `python-dotenv` (optional)

Install:
```bash
pip install requests tqdm python-dotenv
```

**Important: Wikimedia User-Agent**

Wikimedia expects a proper User-Agent.
Edit scrape_open_images.py and set:

DEFAULT_UA = "YourProjectName/1.0 (contact: your_email@domain.com)"


Use a real email (not a placeholder).


**Usage**
1) Wikimedia Commons — Category mode

Collect 100 images into transport/:
```bash
python scrape_open_images.py --out transport --min 100 wikimedia \
  --category "Jeepneys in Manila"
```

Multiple categories:
```bash
python scrape_open_images.py --out transport --min 300 wikimedia \
  --category "Jeepneys in Manila" \
  --category "Tricycles in the Philippines"
```

Limit per category (if enabled in your script):
```bash
python scrape_open_images.py --out transport --min 300 wikimedia --max-per-category 50 \
  --category "Jeepneys in Manila" \
  --category "Tricycles in the Philippines"
```
2) Wikimedia Commons — Search mode (Commons search bar style)

Collect using search keywords (File namespace):
```bash
python scrape_open_images.py --out festival --min 200 wikimedia-search \
  --query "sinulog festival" \
  --query "ati-atihan festival"
```

Limit per query (if enabled in your script):
```bash
python scrape_open_images.py --out festival --min 300 wikimedia-search --max-per-query 60 \
  --query "sinulog festival" \
  --query "ati-atihan festival" \
  --query "dinagyang festival"
```
3) Resume mode (checkpoint)
```bash
python scrape_open_images.py --out transport --min 300 --resume wikimedia \
  --category "Jeepneys in Manila"
```

To reset resume state, delete checkpoint files inside: _checkpoints_

4) Dry-run (metadata only)
```bash
python scrape_open_images.py --out test --min 20 --dry-run wikimedia-search \
  --query "bangka cebu"
```
