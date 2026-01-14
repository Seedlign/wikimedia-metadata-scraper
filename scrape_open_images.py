from __future__ import annotations

import argparse
import csv
from fileinput import filename
import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
from tqdm import tqdm

try:
    import aiohttp
    import asyncio
    AIOHTTP_AVAILABLE = True
except Exception:
    AIOHTTP_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# -----------------------------
# Config + License allowlist
# -----------------------------

WIKIMEDIA_API = "https://commons.wikimedia.org/w/api.php"
FLICKR_API = "https://www.flickr.com/services/rest/"

# Your project rule: prefer commercial+derivatives
# Wikimedia extmetadata LicenseShortName examples:
# - "CC BY-SA 4.0", "CC BY 3.0", "CC0 1.0", "Public domain"
ALLOWED_LICENSE_PATTERNS = [
    r"\bcc\s*by\b",         # CC BY
    r"\bcc\s*by[-\s]*sa\b", # CC BY-SA
    r"\bcc0\b",             # CC0
    r"\bpublic\s*domain\b", # Public domain
]
DISALLOWED_PATTERNS = [
    r"\bnd\b",              # NoDerivatives marker
    r"no\s*derivatives",
    r"no\s*derivs",
]

# Flickr license IDs vary; we resolve with flickr.photos.licenses.getInfo()
# We'll allow only licenses whose name contains these:
FLICKR_ALLOWED_NAME_PATTERNS = [
    r"attribution",         # CC BY
    r"sharealike",          # CC BY-SA
    r"cc0",
    r"public\s*domain",
]
FLICKR_DISALLOWED_NAME_PATTERNS = [
    r"no\s*derivatives",
    r"\bnd\b",
    r"noncommercial",       # exclude NC to match your preference
]

DEFAULT_UA = "KitaKo-OpenLicenseScraper/1.0 (contact: ricbarrios45@gmail.com)"

SESSION = requests.Session()

DOWNLOAD_HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Api-User-Agent": DEFAULT_UA,           # harmless + sometimes helpful
    "Referer": "https://commons.wikimedia.org/",
    "Accept": "image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


_retry = Retry(
    total=5,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    raise_on_status=False,
)

SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.headers.update({
    "User-Agent": DEFAULT_UA,
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://commons.wikimedia.org/",
})

@dataclass
class MetaRecord:
    image_id: str
    category: str
    source_url: str
    license_name: str
    license_url: str
    attribution_text: str
    has_readable_text: bool
    notes: str = ""

# -----------------------------
# Utility helpers
# -----------------------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def safe_filename(s: str, max_len: int = 140) -> str:
    s = re.sub(r"[^\w\-\. ]+", "_", s, flags=re.UNICODE).strip().replace(" ", "_")
    return s[:max_len] if len(s) > max_len else s

def is_license_allowed(license_name: str, license_url: str = "") -> Tuple[bool, str]:
    """Return (allowed, reason). Enforces your thesis rules."""
    text = f"{license_name} {license_url}".lower()

    for pat in DISALLOWED_PATTERNS:
        if re.search(pat, text):
            return False, f"disallowed_license_pattern:{pat}"

    for pat in ALLOWED_LICENSE_PATTERNS:
        if re.search(pat, text):
            # CC BY/SA/CC0/PD generally allow commercial use and derivatives (CC0/PD yes, BY/SA yes)
            return True, "allowed"
    return False, "unknown_or_not_in_allowlist"

def write_jsonl(path: Path, obj: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def append_skipped(skipped_csv: Path, row: Dict[str, Any], header: List[str]) -> None:
    file_exists = skipped_csv.exists()
    with skipped_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 30) -> requests.Response:
    # Use download headers for upload.wikimedia.org, otherwise default session headers
    final_headers = headers
    if "upload.wikimedia.org" in url:
        final_headers = {**DOWNLOAD_HEADERS, **(headers or {})}

    r = SESSION.get(
        url,
        params=params,
        headers=final_headers,   # can be None; requests will then use session headers
        timeout=timeout,
        allow_redirects=True
    )
    r.raise_for_status()
    return r


def download_bytes(url: str, timeout: int = 60) -> bytes:
    r = http_get(url, timeout=timeout)
    # TEMP DEBUG: print once
    if getattr(download_bytes, "_printed", False) is False:
        print("[DEBUG] download status", r.status_code)
        print("[DEBUG] sent UA", r.request.headers.get("User-Agent"))
        print("[DEBUG] sent Referer", r.request.headers.get("Referer"))
        download_bytes._printed = True
    return r.content


def checkpoint_save(path: Path, data: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def checkpoint_load(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))

def load_existing_dedupe_sets(meta_path: Path):
    existing_urls = set()
    existing_hashes = set()

    if not meta_path.exists():
        return existing_urls, existing_hashes

    with meta_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            url = obj.get("source_url")
            if url:
                existing_urls.add(url)

            h = obj.get("content_sha256")  # only if you store it
            if h:
                existing_hashes.add(h)

    return existing_urls, existing_hashes

def next_id_start(meta_path: Path, prefix: str) -> int:
    """
    Scans an existing JSONL metadata file and returns the next integer ID start
    for a given prefix (e.g., 'wikimedia_', 'flickr_').
    """
    if not meta_path.exists():
        return 0

    max_n = -1
    with meta_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            image_id = str(obj.get("image_id", ""))
            if not image_id.startswith(prefix):
                continue

            # Expect format like: wikimedia_00042
            tail = image_id[len(prefix):]
            if tail.isdigit():
                max_n = max(max_n, int(tail))

    return max_n + 1


# -----------------------------
# Wikimedia Commons scraper
# -----------------------------

def wikimedia_category_members(category: str, limit: int = 50, continue_token: Optional[dict] = None) -> dict:
    """
    Fetch files from a Wikimedia Commons category with imageinfo + extmetadata.
    """
    params = {
        "action": "query",
        "format": "json",
        "generator": "categorymembers",
        "gcmtitle": f"Category:{category}",
        "gcmtype": "file",
        "gcmlimit": str(limit),
        "prop": "imageinfo",
        "iiprop": "url|extmetadata",
        "iiurlwidth": "1024",
    }
    if continue_token:
        params.update(continue_token)
    return http_get(WIKIMEDIA_API, params=params).json()

def wikimedia_search_members(search_query: str, limit: int = 50, continue_token: Optional[dict] = None) -> dict:
    """
    Use MediaWiki API generator=search restricted to File namespace (ns=6),
    so results behave like Commons search but return file pages.
    """
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrsearch": search_query,
        "gsrnamespace": 6,          # File namespace
        "gsrlimit": min(limit, 50),
        "prop": "imageinfo",
        "iiprop": "url|extmetadata",
        "iiurlwidth": "1024",
    }
    if continue_token:
        # MediaWiki returns dict like {"continue":"...","gsroffset":...}
        params.update(continue_token)

    r = http_get("https://commons.wikimedia.org/w/api.php", params=params, timeout=30)
    return r.json()

def parse_wikimedia_item(page: dict) -> Optional[dict]:
    """
    Extract what we need from a single query page entry.
    """
    imageinfo = (page.get("imageinfo") or [])
    if not imageinfo:
        return None
    ii = imageinfo[0]
    ext = ii.get("extmetadata") or {}

    # extmetadata fields often have {"value": "..."} shape
    def extv(key: str) -> str:
        v = ext.get(key, {})
        if isinstance(v, dict):
            return str(v.get("value", "")).strip()
        return str(v or "").strip()

    title = page.get("title", "")
    thumb_url = ii.get("thumburl", "")
    orig_url = ii.get("url", "")
    # Page URL (source) is the commons File page
    # Example title: "File:Something.jpg"
    file_title = title.replace(" ", "_")
    source_url = f"https://commons.wikimedia.org/wiki/{file_title}"
    license_name = extv("LicenseShortName") or extv("UsageTerms")
    license_url = extv("LicenseUrl")
    author = extv("Artist") or extv("Credit") or "Unknown"
    # Artist might be HTML; keep as-is but you can strip tags if you want.
    attribution_text = f"{title} — {author}. Source: {source_url}. License: {license_name} ({license_url})"

    # Determine allow/deny
    allowed, reason = is_license_allowed(license_name, license_url)
    if not allowed:
        return {"ok": False, "reason": reason, "title": title, "source_url": source_url, "direct_url": direct_url, "license_name": license_name, "license_url": license_url}

    # crude: BY/SA/CC0/PD generally allow derivatives & commercial (your preference)
    derivatives_allowed = True
    commercial_allowed = True
    
    return {
        "ok": True,
        "title": title,
        "author": author,
        "source_url": source_url,
        "thumb_url": thumb_url,
        "orig_url": orig_url,
        "license_name": license_name,
        "license_url": license_url,
        "attribution_text": attribution_text,
        "derivatives_allowed": True,
        "commercial_use_allowed": True,
    }
    
def scrape_wikimedia_search(
    queries: List[str],
    out_dir: Path,
    min_images: int,
    per_request: int,
    max_per_query: int,
    dry_run: bool,
    resume: bool
) -> None:
    images_dir = out_dir / "images" / "wikimedia"
    ensure_dir(images_dir)
    meta_path = out_dir / "metadata.jsonl"
    skipped_csv = out_dir / "skipped.csv"
    cp_path = out_dir / "checkpoints" / "wikimedia_search_checkpoint.json"

    skipped_header = ["source_platform", "title", "source_url", "direct_url", "license_name", "license_url", "reason"]

    # ---- DEDUPE SETS PERSIST ACROSS RUNS (from metadata.jsonl) ----
    existing_urls, existing_hashes = load_existing_dedupe_sets(meta_path)  # existing_hashes optional

    # ---- CONTINUE ID NUMBERING ACROSS RUNS ----
    # Use same prefix as category mode so Wikimedia IDs are globally unique across both modes
    id_counter = next_id_start(meta_path, "wikimedia_")

    # ---- CHECKPOINT / RESUME ----
    checkpoint = checkpoint_load(cp_path) if resume else None
    start_q_idx = checkpoint.get("query_index", 0) if checkpoint else 0
    continue_token = checkpoint.get("continue_token") if checkpoint else None
    collected = checkpoint.get("collected", 0) if checkpoint else 0

    seen_hashes = set(checkpoint.get("seen_hashes", [])) if checkpoint else set()
    # If you store hashes in metadata, merge them here:
    seen_hashes |= set(existing_hashes) if existing_hashes else set()

    pbar = tqdm(total=min_images, initial=collected, desc="Wikimedia(search) collected", unit="img")

    for qi in range(start_q_idx, len(queries)):
        q = queries[qi]
        collected_in_query = 0

        while collected < min_images and (max_per_query <= 0 or collected_in_query < max_per_query):
            data = wikimedia_search_members(q, limit=per_request, continue_token=continue_token)

            pages_obj = (data.get("query", {}).get("pages", {}) or {})
            if not pages_obj:
                # no more results for this query
                break

            for _, page in pages_obj.items():
                parsed = parse_wikimedia_item(page)
                if not parsed:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": page.get("title", ""),
                        "source_url": "",
                        "direct_url": "",
                        "license_name": "",
                        "license_url": "",
                        "reason": "parse_returned_none_missing_imageinfo",
                    }, skipped_header)
                    continue

                if not parsed.get("ok", False):
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": parsed.get("title", ""),
                        "source_url": parsed.get("source_url", ""),
                        "direct_url": "",
                        "license_name": parsed.get("license_name", ""),
                        "license_url": parsed.get("license_url", ""),
                        "reason": parsed.get("reason", "unknown"),
                    }, skipped_header)
                    continue

                title = parsed["title"]

                # ---- FAST DEDUPE: skip if source_url already in metadata ----
                if parsed["source_url"] in existing_urls:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": title,
                        "source_url": parsed["source_url"],
                        "direct_url": "",
                        "license_name": parsed["license_name"],
                        "license_url": parsed["license_url"],
                        "reason": "duplicate_source_url",
                    }, skipped_header)
                    continue

                # Download candidates (thumb first, then original)
                candidates = [parsed.get("thumb_url"), parsed.get("orig_url")]
                candidates = [u for u in candidates if u]

                if not candidates:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": title,
                        "source_url": parsed["source_url"],
                        "direct_url": "",
                        "license_name": parsed["license_name"],
                        "license_url": parsed["license_url"],
                        "reason": "missing_thumb_and_orig_url",
                    }, skipped_header)
                    continue

                if dry_run:
                    direct_url = candidates[0]
                    fake_hash = "DRY_RUN"
                    local_path = ""
                else:
                    try:
                        content = None
                        direct_url = None
                        last_err = None

                        for u in candidates:
                            try:
                                content = download_bytes(u)
                                direct_url = u
                                break
                            except Exception as e:
                                last_err = e

                        if content is None:
                            raise last_err

                        h = sha256_bytes(content)

                        # ---- HASH DEDUPE (same bytes) ----
                        if h in seen_hashes:
                            append_skipped(skipped_csv, {
                                "source_platform": "wikimedia",
                                "title": title,
                                "source_url": parsed["source_url"],
                                "direct_url": direct_url or candidates[0],
                                "license_name": parsed["license_name"],
                                "license_url": parsed["license_url"],
                                "reason": "duplicate_content_hash",
                            }, skipped_header)
                            continue

                        seen_hashes.add(h)

                        ext = Path(direct_url.split("?")[0]).suffix or ".jpg"
                        fname = safe_filename(title.replace("File:", ""), 160)
                        local_path = str((images_dir / f"{fname}_{h[:10]}{ext}").resolve())
                        Path(local_path).write_bytes(content)
                        fake_hash = h

                    except Exception as e:
                        status = ""
                        resp = getattr(e, "response", None)
                        if resp is not None:
                            status = str(getattr(resp, "status_code", ""))

                        append_skipped(skipped_csv, {
                            "source_platform": "wikimedia",
                            "title": title,
                            "source_url": parsed["source_url"],
                            "direct_url": candidates[0],
                            "license_name": parsed["license_name"],
                            "license_url": parsed["license_url"],
                            "reason": f"download_error:{type(e).__name__}:status={status}",
                        }, skipped_header)
                        continue

                # ---- WRITE YOUR TRIMMED METADATA SCHEMA ----
                record = {
                    "image_id": f"wikimedia_{id_counter:05d}",
                    "category": q,  # search bucket label (query text)
                    "source_url": parsed["source_url"],
                    "license_name": parsed["license_name"],
                    "license_url": parsed["license_url"],
                    "attribution_text": parsed.get("attribution_text", ""),
                    "has_readable_text": False,
                    "notes": f"query={q}",
                    # Optional (recommended internal fields):
                    # "content_sha256": fake_hash,
                    # "file_name": Path(local_path).name if local_path else "",
                }
                write_jsonl(meta_path, record)

                # update sets/counters
                existing_urls.add(parsed["source_url"])
                id_counter += 1
                collected += 1
                collected_in_query += 1
                pbar.update(1)

                time.sleep(0.3)

                if collected >= min_images:
                    break
                if max_per_query > 0 and collected_in_query >= max_per_query:
                    break

            # pagination
            continue_token = data.get("continue")
            checkpoint_save(cp_path, {
                "query_index": qi,
                "continue_token": continue_token,
                "collected": collected,
                "seen_hashes": list(seen_hashes)[:20000],
            })

            if not continue_token:
                break

        # move to next query bucket
        continue_token = None
        checkpoint_save(cp_path, {
            "query_index": qi + 1,
            "continue_token": None,
            "collected": collected,
            "seen_hashes": list(seen_hashes)[:20000],
        })

        if collected >= min_images:
            break

    pbar.close()
    print(f"Done. Wikimedia(search) collected: {collected}. Output: {out_dir}")


def scrape_wikimedia(categories: List[str], out_dir: Path, min_images: int, per_request: int, max_per_category: int, dry_run: bool, resume: bool) -> None:
    images_dir = out_dir / "images" / "wikimedia"
    ensure_dir(images_dir)
    meta_path = out_dir / "metadata.jsonl"
    existing_urls, existing_hashes = load_existing_dedupe_sets(meta_path)
    id_counter = next_id_start(meta_path, "wikimedia_") #CHANGE THE NAME PER CATEGORY
    skipped_csv = out_dir / "skipped.csv"
    cp_path = out_dir / "checkpoints" / "wikimedia_checkpoint.json"

    skipped_header = ["source_platform", "title", "source_url", "direct_url", "license_name", "license_url", "reason"]

    seen_hashes = set()
    collected = 0

    checkpoint = checkpoint_load(cp_path) if resume else None
    start_cat_idx = checkpoint.get("category_index", 0) if checkpoint else 0
    continue_token = checkpoint.get("continue_token") if checkpoint else None
    collected = checkpoint.get("collected", 0) if checkpoint else 0
    seen_hashes = set(checkpoint.get("seen_hashes", [])) if checkpoint else set()

    pbar = tqdm(total=min_images, initial=collected, desc="Wikimedia collected", unit="img")

    for ci in range(start_cat_idx, len(categories)):
        category = categories[ci]
        collected_in_cat = 0
        while collected < min_images and (max_per_category <= 0 or collected_in_cat < max_per_category):
            data = wikimedia_category_members(category, limit=per_request, continue_token=continue_token)

            import json

            if "error" in data:
                print("API ERROR:", data["error"])
            if "warnings" in data:
                print("API WARNINGS:", data["warnings"])

            pages_obj = data.get("query", {}).get("pages")
            print(f"[DEBUG] category={category} keys={list(data.keys())} has_query={'query' in data} pages_type={type(pages_obj).__name__} pages_len={len(pages_obj) if isinstance(pages_obj, dict) else (len(pages_obj) if isinstance(pages_obj, list) else 0)}")

            if not pages_obj:
                print("[DEBUG] Response head:", json.dumps(data)[:500])
                break

            pages = (data.get("query", {}).get("pages", {}) or {})
            # parse items
            for _, page in pages.items():
                parsed = parse_wikimedia_item(page)
                if not parsed:
                    continue
                if not parsed["ok"]:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": parsed.get("title", ""),
                        "source_url": parsed.get("source_url", ""),
                        "direct_url": parsed.get("direct_url", ""),
                        "license_name": parsed.get("license_name", ""),
                        "license_url": parsed.get("license_url", ""),
                        "reason": parsed.get("reason", "unknown"),
                    }, skipped_header)
                    continue

                title = parsed["title"]
                # Fast dedupe: skip if we've already recorded this source_url
                if parsed["source_url"] in existing_urls:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": title,
                        "source_url": parsed["source_url"],
                        "direct_url": "",
                        "license_name": parsed["license_name"],
                        "license_url": parsed["license_url"],
                        "reason": "duplicate_source_url",
                    }, skipped_header)
                    continue
                filename = title.replace("File:", "").replace(" ", "_")
                file_path_url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{filename}"
                #where starts tab
                candidates = [file_path_url, parsed.get("thumb_url"), parsed.get("orig_url")]
                candidates = [u for u in candidates if u]

                if not candidates:
                    append_skipped(skipped_csv, {
                        "source_platform": "wikimedia",
                        "title": title,
                        "source_url": parsed["source_url"],
                        "direct_url": "",
                        "license_name": parsed["license_name"],
                        "license_url": parsed["license_url"],
                        "reason": "missing_thumb_and_orig_url",
                    }, skipped_header)
                    continue

                if dry_run:
                    fake_hash = "DRY_RUN"
                    local_path = ""
                    direct_url = candidates[0]  # just record something for metadata
                else:
                    try:
                        content = None
                        direct_url = None
                        last_err = None

                        for u in candidates:
                            try:
                                content = download_bytes(u)
                                direct_url = u
                                break
                            except Exception as e:
                                last_err = e

                        if content is None:
                            # none of the candidates worked
                            raise last_err

                        h = sha256_bytes(content)
                        # Robust dedupe: same bytes already seen
                        if h in seen_hashes:
                            append_skipped(skipped_csv, {
                                "source_platform": "wikimedia",
                                "title": title,
                                "source_url": parsed["source_url"],
                                "direct_url": direct_url,
                                "license_name": parsed["license_name"],
                                "license_url": parsed["license_url"],
                                "reason": "duplicate_content_hash",
                            }, skipped_header)
                            continue

                        seen_hashes.add(h)
                        existing_hashes.add(h)
                        existing_urls.add(parsed["source_url"])

                        ext = Path(direct_url.split("?")[0]).suffix or ".jpg"
                        fname = safe_filename(title.replace("File:", ""), 160)
                        local_path = str((images_dir / f"{fname}_{h[:10]}{ext}").resolve())
                        Path(local_path).write_bytes(content)
                        fake_hash = h
                    #start tab
                    except Exception as e:
                        status = ""
                        try:
                            resp = getattr(e, "response", None)
                            if resp is not None:
                                status = str(resp.status_code)
                        except Exception:
                            pass

                        append_skipped(skipped_csv, {
                            "source_platform": "wikimedia",
                            "title": title,
                            "source_url": parsed["source_url"],
                            "direct_url": direct_url or (candidates[0] if candidates else ""),
                            "license_name": parsed["license_name"],
                            "license_url": parsed["license_url"],
                            "reason": f"download_error:{type(e).__name__}:status={status}",
                        }, skipped_header)
                        continue

                #stop here
                record = MetaRecord(
                    image_id=f"wikimedia_{id_counter:05d}", #CHANGE THE NAME PER CATEGORY
                    category=category,
                    source_url=parsed["source_url"],
                    license_name=parsed["license_name"],
                    license_url=parsed["license_url"],
                    attribution_text=parsed.get("attribution_text", ""),
                    has_readable_text=False,
                    notes=f"category={category}",
                )
                write_jsonl(meta_path, asdict(record))
                id_counter += 1
                collected += 1
                collected_in_cat += 1
                pbar.update(1)

                if max_per_category > 0 and collected_in_cat >= max_per_category:
                    # stop this category early
                    continue_token = None
                    break
                
                if collected >= min_images:
                    break

            # pagination
            continue_token = data.get("continue")
            # save checkpoint after each page
            checkpoint_save(cp_path, {
                "category_index": ci,
                "continue_token": continue_token,
                "collected": collected,
                "seen_hashes": list(seen_hashes)[:20000],  # cap for safety
            })

            if not continue_token:
                # move to next category
                break

        # reset continue token when moving categories
        continue_token = None
        checkpoint_save(cp_path, {
            "category_index": ci + 1,
            "continue_token": None,
            "collected": collected,
            "seen_hashes": list(seen_hashes)[:20000],
        })

        if collected >= min_images:
            break

    pbar.close()
    print(f"Done. Wikimedia collected: {collected}. Output: {out_dir}")


# -----------------------------
# Flickr scraper (DISABLED)
# -----------------------------
# The Flickr scraper has been commented out. To re-enable, remove the leading
# '#' characters from the following block. It was disabled to avoid running
# against Flickr's API or requiring an API key during normal usage.
#
# def flickr_call(api_key: str, method: str, extra: dict) -> dict:
#     params = {
#         "method": method,
#         "api_key": api_key,
#         "format": "json",
#         "nojsoncallback": 1,
#         **extra,
#     }
#     return http_get(FLICKR_API, params=params).json()
#
# def flickr_license_map(api_key: str) -> Dict[str, Dict[str, str]]:
#     """Return license_id -> {name, url}"""
#     data = flickr_call(api_key, "flickr.photos.licenses.getInfo", {})
#     licenses = data.get("licenses", {}).get("license", []) or []
#     out = {}
#     for lic in licenses:
#         out[str(lic.get("id"))] = {"name": lic.get("name", ""), "url": lic.get("url", "")}
#     return out
#
# def flickr_license_allowed(name: str, url: str) -> Tuple[bool, str]:
#     text = f"{name} {url}".lower()
#     for pat in FLICKR_DISALLOWED_NAME_PATTERNS:
#         if re.search(pat, text):
#             return False, f"disallowed_license_pattern:{pat}"
#     for pat in FLICKR_ALLOWED_NAME_PATTERNS:
#         if re.search(pat, text):
#             return True, "allowed"
#     return False, "unknown_or_not_in_allowlist"
#
# def flickr_build_direct_url(photo: dict) -> Optional[str]:
#     """
#     Requires these fields: farm, server, id, secret
#     https://www.flickr.com/services/api/misc.urls.html
#     """
#     try:
#         return f"https://farm{photo['farm']}.staticflickr.com/{photo['server']}/{photo['id']}_{photo['secret']}_b.jpg"
#     except Exception:
#         return None
#
# def scrape_flickr(query: str, out_dir: Path, min_images: int, per_page: int, max_pages: int, dry_run: bool, resume: bool) -> None:
#     api_key = os.getenv("FLICKR_API_KEY", "").strip()
#     if not api_key:
#         raise SystemExit("Missing FLICKR_API_KEY env var. Set it in your environment or .env file.")
#
#     images_dir = out_dir / "images" / "flickr"
#     ensure_dir(images_dir)
#     meta_path = out_dir / "metadata.jsonl"
#     skipped_csv = out_dir / "skipped.csv"
#     cp_path = out_dir / "checkpoints" / "flickr_checkpoint.json"
#
#     skipped_header = ["source_platform", "title", "source_url", "direct_url", "license_name", "license_url", "reason"]
#
#     lic_map = flickr_license_map(api_key)
#
#     seen_hashes = set()
#     collected = 0
#     checkpoint = checkpoint_load(cp_path) if resume else None
#     start_page = checkpoint.get("page", 1) if checkpoint else 1
#     collected = checkpoint.get("collected", 0) if checkpoint else 0
#     seen_hashes = set(checkpoint.get("seen_hashes", [])) if checkpoint else set()
#
#     pbar = tqdm(total=min_images, initial=collected, desc="Flickr collected", unit="img")
#
#     for page in range(start_page, max_pages + 1):
#         if collected >= min_images:
#             break
#
#         data = flickr_call(api_key, "flickr.photos.search", {
#             "text": query,
#             "content_type": 1,
#             "media": "photos",
#             "safe_search": 1,
#             "per_page": per_page,
#             "page": page,
#             "sort": "relevance",
#             "extras": "license,owner_name,original_format",
#         })
#
#         photos = data.get("photos", {}).get("photo", []) or []
#         if not photos:
#             break
#
#         for ph in photos:
#             if collected >= min_images:
#                 break
#
#             license_id = str(ph.get("license", ""))
#             lic = lic_map.get(license_id, {"name": "", "url": ""})
#             allowed, reason = flickr_license_allowed(lic.get("name", ""), lic.get("url", ""))
#             if not allowed:
#                 append_skipped(skipped_csv, {
#                     "source_platform": "flickr",
#                     "title": ph.get("title", ""),
#                     "source_url": f"https://www.flickr.com/photos/{ph.get('owner', '')}/{ph.get('id', '')}",
#                     "direct_url": "",
#                     "license_name": lic.get("name", ""),
#                     "license_url": lic.get("url", ""),
#                     "reason": reason,
#                 }, skipped_header)
#                 continue
#
#             title = ph.get("title", "") or f"flickr_{ph.get('id', '')}"
#             author = ph.get("ownername", "") or ph.get("owner", "") or "Unknown"
#             source_url = f"https://www.flickr.com/photos/{ph.get('owner', '')}/{ph.get('id', '')}"
#             direct_url = flickr_build_direct_url(ph)
#             if not direct_url:
#                 append_skipped(skipped_csv, {
#                     "source_platform": "flickr",
#                     "title": title,
#                     "source_url": source_url,
#                     "direct_url": "",
#                     "license_name": lic.get("name", ""),
#                     "license_url": lic.get("url", ""),
#                     "reason": "missing_direct_url",
#                 }, skipped_header)
#                 continue
#
#             if dry_run:
#                 h = "DRY_RUN"
#                 local_path = ""
#             else:
#                 try:
#                     content = download_bytes(direct_url)
#                     h = sha256_bytes(content)
#                     if h in seen_hashes:
#                         continue
#                     seen_hashes.add(h)
#
#                     ext = Path(direct_url.split("?")[0]).suffix or ".jpg"
#                     fname = safe_filename(title, 160)
#                     local_path = str((images_dir / f"{fname}_{h[:10]}{ext}").resolve())
#                     Path(local_path).write_bytes(content)
#                 except Exception as e:
#                     append_skipped(skipped_csv, {
#                         "source_platform": "flickr",
#                         "title": title,
#                         "source_url": source_url,
#                         "direct_url": direct_url,
#                         "license_name": lic.get("name", ""),
#                         "license_url": lic.get("url", ""),
#                         "reason": f"download_error:{type(e).__name__}",
#                     }, skipped_header)
#                     continue
#
#             derivatives_allowed = True
#             commercial_allowed = True
#
#             record = TASLRecord(
#                 image_id=f"flickr_{collected:05d}",
#                 source_platform="flickr",
#                 title=title,
#                 author=author,
#                 source_url=source_url,
#                 direct_url=direct_url,
#                 license_name=lic.get("name", ""),
#                 license_url=lic.get("url", ""),
#                 derivatives_allowed=derivatives_allowed,
#                 commercial_use_allowed=commercial_allowed,
#                 sha256=h,
#                 local_path=local_path,
#                 collected_at_unix=int(time.time()),
#                 notes=f"query={query};license_id={license_id}",
#             )
#             write_jsonl(meta_path, asdict(record))
#             collected += 1
#             pbar.update(1)
#
#         checkpoint_save(cp_path, {
#             "page": page + 1,
#             "collected": collected,
#             "seen_hashes": list(seen_hashes)[:20000],
#         })
#
#         time.sleep(0.5)
#
#     pbar.close()
#     print(f"Done. Flickr collected: {collected}. Output: {out_dir}")


# -----------------------------
# Optional: faster async downloading (drop-in)
# -----------------------------
# If you want to speed up Wikimedia/Flickr downloads, you can replace download_bytes()
# calls with this batch async downloader. Left here as a template.

async def _fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.read()

async def download_many(urls: List[str], concurrency: int = 16) -> List[bytes]:
    if not AIOHTTP_AVAILABLE:
        raise RuntimeError("aiohttp not installed. pip install aiohttp")
    connector = aiohttp.TCPConnector(limit=concurrency)
    headers = {"User-Agent": DEFAULT_UA}
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [asyncio.create_task(_fetch(session, u)) for u in urls]
        return await asyncio.gather(*tasks)


# -----------------------------
# CLI
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="source", required=True)

    ap.add_argument("--out", default="out", help="Output directory")
    ap.add_argument("--min", type=int, default=400, help="Minimum number of images to collect")
    ap.add_argument("--dry-run", action="store_true", help="Collect metadata only, do not download")
    ap.add_argument("--resume", action="store_true", help="Resume from checkpoint")

    w = sub.add_parser("wikimedia", help="Scrape Wikimedia Commons by categories.")
    w.add_argument("--category", action="append", required=True)
    w.add_argument("--per-request", type=int, default=50)
    w.add_argument("--max-per-category", type=int, default=0, help="Maximum images to collect per category (0 = unlimited).")

    wsearch = sub.add_parser("wikimedia-search", help="Scrape Wikimedia Commons by search keywords (File namespace).")
    wsearch.add_argument("--query", action="append", required=True, help="Search keyword(s). Repeatable.")
    wsearch.add_argument("--per-request", type=int, default=50)
    wsearch.add_argument("--max-per-query", type=int, default=0)


    # NOTE: if you want 500, you should also consider API etiquette + rate limiting.

    # Flickr CLI entry disabled
    # f = sub.add_parser("flickr", help="Scrape Flickr with CC-only filtering (requires API key)")
    # f.add_argument("--query", required=True, help="Search query text")
    # f.add_argument("--per-page", type=int, default=100, help="Results per page (max 500)")
    # f.add_argument("--max-pages", type=int, default=50, help="Max pages to scan")

    args = ap.parse_args()
    out_dir = Path(args.out)
    ensure_dir(out_dir / "checkpoints")

    if args.source == "wikimedia":
        scrape_wikimedia(
            categories=args.category,
            out_dir=out_dir,
            min_images=args.min,
            per_request=args.per_request,
            max_per_category=args.max_per_category,
            dry_run=args.dry_run,
            resume=args.resume,
        )
    elif args.source == "wikimedia-search":
        scrape_wikimedia_search(
            queries=args.query,
            out_dir=out_dir,
            min_images=args.min,
            per_request=getattr(args, "per_request", 50),
            max_per_query=getattr(args, "max_per_query", 0),
            dry_run=args.dry_run,
            resume=args.resume,
        )

    # Flickr option disabled
    # elif args.source == "flickr":
    #     scrape_flickr(
    #         query=args.query,
    #         out_dir=out_dir,
    #         min_images=args.min,
    #         per_page=args.per_page,
    #         max_pages=args.max_pages,
    #         dry_run=args.dry_run,
    #         resume=args.resume,
    #     )
    else:
        raise SystemExit("Unknown source")

if __name__ == "__main__":
    main()
