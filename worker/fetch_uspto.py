#!/usr/bin/env python3
"""GitHub Actions worker: USPTO trademark filings -> sponsor candidates.

Mirrors ops/sponsor-fetch/uspto_pull.py (same classifier, same domain
resolution) but runs on a GitHub runner and writes JSON results instead of
touching the local SQLite DB. The local side ingests uspto/results-*.json
via ops/sponsor-fetch/uspto_ingest.py (pure git+sqlite, no website access).

Auth: USPTO ODP API key from the USPTO_API_KEY env var (repo secret),
sent as X-API-KEY (exact casing — the gateway rejects anything else).

Pipeline:
  1. ODP API: list product files, download newest unprocessed daily ZIP(s).
     Watermark: uspto/processed.txt in this repo.
  2. Parse XML (iterparse over <case-file>).
  3. Aggressive classifier funnel (same rules as uspto_pull.py).
  4. Domain resolution for survivors (heuristic candidates, verified by
     site <title>/H1 content, parked-page rejection).
  5. Write uspto/results-YYYY-MM-DD.json + uspto/rejects-YYYY-MM-DD.jsonl.

Usage: python worker/fetch_uspto.py [--max-files N]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

REPO_ROOT = Path(__file__).resolve().parent.parent
USPTO_DIR = REPO_ROOT / "uspto"

ODP_BASE = "https://api.uspto.gov"
PRODUCT_ID = "TRTDXFAP"

MAX_FILES_PER_RUN = 2
FILING_WINDOW_DAYS = 21
PASS_SCORE = 3
PROBE_WORKERS = 10
PROBE_TIMEOUT = 12


def api_key() -> str:
    key = (os.environ.get("USPTO_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("USPTO_API_KEY env var is not set")
    return key


# --------------------------------------------------------------------------
# Classifier tables (kept in sync with ops/sponsor-fetch/uspto_pull.py)
# --------------------------------------------------------------------------

CLASS_SCORES = {
    3: 3, 5: 3, 9: 3, 11: 3, 12: 3, 14: 3, 18: 3, 20: 3, 21: 3, 24: 3,
    25: 3, 28: 3, 29: 3, 30: 3, 31: 3, 32: 3, 42: 3,
    16: 1, 27: 1, 41: 1,
    35: -2,
    1: -3, 4: -3, 6: -3, 7: -3, 10: -3, 36: -3, 37: -3, 38: -3, 39: -3,
    40: -3, 43: -3, 44: -3, 45: -3,
}
BLOCKED_CLASSES = {13, 34}  # firearms, tobacco: hard block, no override
ALCOHOL_CLASS = 33

CLASS_TO_NICHE = {
    9: "tech", 28: "gaming", 25: "fashion", 3: "beauty", 21: "home",
    20: "home", 27: "home", 24: "home", 11: "home", 29: "food-beverage",
    30: "food-beverage", 32: "food-beverage", 5: "health-fitness",
    14: "fashion", 18: "fashion", 12: "automotive", 31: "pets",
    42: "creator-tools", 41: "entertainment", 16: "lifestyle",
}

BLOCK_KEYWORDS = {
    "cannabis", "marijuana", "thc", "cbd", "hemp",
    "tobacco", "vape", "vaping", "cigarette", "cigar",
    "firearm", "gun", "rifle", "pistol", "ammunition", "ammo",
    "gambling", "casino", "betting", "sportsbook", "lottery",
    "porn", "pornographic", "xxx", "escort", "strip club", "brothel",
}
ALCOHOL_KEYWORDS = {
    "beer", "wine", "whiskey", "whisky", "spirits", "liquor", "cocktail",
    "vodka", "tequila", "rum", "gin", "bourbon", "brewery", "winery",
    "distillery", "cider", "sake", "mead", "hard seltzer", "malt beverage",
}
KILL_KEYWORDS = {  # B2B/industrial/local-service: never sponsors
    "law firm", "attorney", "lawyer", "legal services", "law office",
    "church", "ministry", "religious", "synagogue", "mosque",
    "realty", "real estate", "insurance", "bank", "banking",
    "funeral", "cemetery", "plumbing", "hvac", "roofing",
    "landscaping", "trucking", "freight", "logistics", "warehouse",
    "staffing", "payroll", "accounting firm", "consulting firm",
    "political", "campaign", "vote",
}
BOOST_KEYWORDS = {  # consumer-brand signals
    "app", "gaming", "game", "skincare", "beauty", "cosmetics",
    "snack", "beverage", "coffee", "tea", "pet", "baby", "kids",
    "fitness", "workout", "smart home", "wearable", "toy",
    "fashion", "apparel", "jewelry", "candle", "supplement",
    "vitamin", "protein", "backpack", "luggage", "mattress",
    "pillow", "kitchen", "cookware", "plant", "garden", "outdoor",
    "camping", "bike", "skate", "surf", "fragrance", "perfume",
    "haircare",
}

PUBLIC_SUFFIXES = {
    "co.uk", "org.uk", "me.uk", "ac.uk", "com.au", "net.au", "co.jp",
    "ne.jp", "co.in", "com.br", "co.kr", "com.mx", "co.nz", "co.za",
    "com.ar", "com.co", "co.id", "com.tr", "co.th", "com.sg", "co.il",
    "com.ng", "com.ph", "com.hk", "co.hk", "com.tw", "co.tw",
}
PARKED_HINTS = {
    "parked", "godaddy", "sedo", "afternic", "buy this domain",
    "domain for sale", "this domain", "hugedomains",
}


def word_boundary_hit(text: str, keywords: set[str]) -> str | None:
    for kw in keywords:
        if re.search(r"\b" + re.escape(kw) + r"\b", text):
            return kw
    return None


def registrable_domain(host: str | None) -> str | None:
    host = (host or "").lower().strip().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if (not host or "." not in host or ".." in host
            or any(c in host for c in " /:@")):
        return None
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in PUBLIC_SUFFIXES:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


# --------------------------------------------------------------------------
# ODP API client (curl transport; key from env, never on argv)
# --------------------------------------------------------------------------

def _run_curl_cfg(cfg_lines: list[str],
                  timeout: int = 150) -> subprocess.CompletedProcess:
    cfg = "\n".join(cfg_lines) + "\n"
    return subprocess.run(["curl", "-K", "-"], input=cfg, capture_output=True,
                          text=True, timeout=timeout)


def _key_cfg() -> str:
    # Written via stdin config so the key never appears on the command line.
    return f'header = "X-API-KEY: {api_key()}"'


def odp_json(path: str, params: dict | None = None) -> dict:
    url = ODP_BASE + path
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += "?" + qs
    cfg = [
        f'url = "{url}"',
        _key_cfg(),
        'header = "Accept: application/json"',
        'header = "User-Agent: NFA-uspto-worker/1.0"',
        "max-time = 60",
        "silent",
        "show-error",
        "fail",  # non-2xx -> nonzero exit, so errors can't parse as JSON
    ]
    proc = _run_curl_cfg(cfg, timeout=90)
    if proc.returncode != 0:
        raise RuntimeError(f"ODP request failed: {proc.stderr[:200]}")
    return json.loads(proc.stdout)


def list_product_files() -> list[dict]:
    """File entries (name + download URI), newest first. Defensive about
    the response shape."""
    data = odp_json(f"/api/v1/datasets/products/{PRODUCT_ID}",
                    {"includeFiles": "true", "latest": "true",
                     "limit": "60"})
    files: list[dict] = []

    def grab(node):
        if isinstance(node, dict):
            name = (node.get("file_name") or node.get("fileName")
                    or node.get("name"))
            uri = (node.get("file_download_uri")
                   or node.get("fileDownloadUri")
                   or node.get("fileDownloadURI")
                   or node.get("download_uri"))
            if name and str(name).lower().endswith(".zip"):
                files.append({"name": str(name), "uri": uri})
            for v in node.values():
                grab(v)
        elif isinstance(node, list):
            for v in node:
                grab(v)

    grab(data)
    files.sort(key=lambda f: f["name"], reverse=True)
    seen: dict[str, dict] = {}
    for f in files:
        seen.setdefault(f["name"], f)
    return list(seen.values())


def download_file(entry: dict, dest: Path) -> None:
    url = entry.get("uri") or (
        f"{ODP_BASE}/api/v1/datasets/products/files/"
        f"{PRODUCT_ID}/{entry['name']}")
    cfg = [
        f'url = "{url}"',
        _key_cfg(),
        'header = "User-Agent: NFA-uspto-worker/1.0"',
        "location",  # follow the 302 to the signed data.uspto.gov URL
        "max-time = 600",
        "silent",
        "show-error",
        "fail",
        f'output = "{dest}"',
    ]
    proc = _run_curl_cfg(cfg, timeout=660)
    if proc.returncode != 0 or not dest.exists() or dest.stat().st_size == 0:
        raise RuntimeError(f"download failed for {entry['name']}: "
                           f"{proc.stderr[:200]}")


# --------------------------------------------------------------------------
# XML parsing (USPTO Trademark Applications DTD v2.x)
# --------------------------------------------------------------------------

def _text(elem, path: str) -> str:
    child = elem.find(path)
    return (child.text or "").strip() if child is not None else ""


def parse_case_file(elem) -> dict:
    header = elem.find("case-file-header")
    serial = _text(elem, "serial-number")
    mark = _text(header, "mark-identification") if header is not None else ""
    filing = _text(header, "filing-date") if header is not None else ""
    status = _text(header, "status-code") if header is not None else ""
    regno = _text(elem, "registration-number")

    classes: list[int] = []
    clf = elem.find("classifications")
    if clf is not None:
        for c in clf.findall("classification"):
            code = (_text(c, "international-code")
                    or _text(c, "primary-code")).strip()
            try:
                classes.append(int(code.lstrip("0") or "0"))
            except ValueError:
                pass

    owner_name, owner_country = "", ""
    owners = elem.find("case-file-owners")
    if owners is not None:
        owner = owners.find("case-file-owner")
        if owner is not None:
            owner_name = _text(owner, "party-name")
            for tag in ("country", "domicile-code"):
                val = _text(owner, tag).upper()
                if val:
                    owner_country = val
                    break
            if not owner_country:
                for desc in owner.iter():
                    if desc.tag.lower() == "country" and desc.text:
                        owner_country = desc.text.strip().upper()
                        break

    gs_texts: list[str] = []
    stmts = elem.find("case-file-statements")
    if stmts is not None:
        for st in stmts.findall("case-file-statement"):
            tcode = _text(st, "type-code")
            if tcode.startswith("GS"):
                gs_texts.extend(
                    (t.text or "") for t in st.findall("text") if t.text)
    return {
        "serial": serial,
        "mark": mark,
        "filing_date": filing,  # YYYYMMDD
        "status": status,
        "regno": regno,
        "classes": sorted(set(classes)),
        "owner": owner_name,
        "country": owner_country,
        "goods": " ".join(" ".join(gs_texts).split()),
    }


# --------------------------------------------------------------------------
# Aggressive classifier
# --------------------------------------------------------------------------

US_COUNTRY = {"US", "USA", "U.S.", "UNITED STATES",
              "UNITED STATES OF AMERICA"}


def classify(rec: dict, today_ymd: str) -> tuple[bool, int, str]:
    """Returns (keep, score, reason). Rejects are aggressive by design."""
    mark = rec["mark"]
    if not mark:
        return False, 0, "no mark text (design-only)"
    filing = rec["filing_date"]
    try:
        age_days = (datetime.strptime(today_ymd, "%Y-%m-%d")
                    - datetime.strptime(filing, "%Y%m%d")).days
    except ValueError:
        return False, 0, "bad filing date"
    if age_days < 0 or age_days > FILING_WINDOW_DAYS:
        return False, 0, f"filing not fresh ({filing})"
    # No status-code gate: for filings this fresh the status is always a
    # live prosecution code (e.g. 630 new application); the common "<600 =
    # live" heuristic misfires on the 63x/64x new-application series.
    # Missing/unknown owner-country evidence must NEVER become US:
    # only an explicit US value passes; anything else is rejected.
    if rec["country"] not in US_COUNTRY:
        return False, 0, f"non-US/unknown owner country ({rec['country']})"

    classes = rec["classes"]
    if not classes:
        return False, 0, "no Nice classes"
    if BLOCKED_CLASSES & set(classes):
        return False, 0, "blocked class (firearms/tobacco)"
    if ALCOHOL_CLASS in classes:
        return False, 0, "alcohol class (allowlist empty)"

    hay = f"{mark} {rec['goods']}".lower()
    hit = word_boundary_hit(hay, BLOCK_KEYWORDS)
    if hit:
        return False, 0, f"blocked keyword: {hit}"
    if word_boundary_hit(hay, ALCOHOL_KEYWORDS):
        return False, 0, "alcohol keyword (allowlist empty)"
    if word_boundary_hit(hay, KILL_KEYWORDS):
        return False, 0, "B2B/service keyword"

    class_score = max((CLASS_SCORES.get(c, 0) for c in classes), default=0)
    if class_score <= 0:
        return False, 0, f"no consumer class (classes {classes})"
    score = class_score
    boosts = sum(1 for kw in BOOST_KEYWORDS
                 if re.search(r"\b" + re.escape(kw) + r"\b", hay))
    score += min(boosts, 3)  # cap keyword boosts at +3
    if score < PASS_SCORE:
        return False, score, f"score {score} < {PASS_SCORE}"
    return True, score, "pass"


# --------------------------------------------------------------------------
# Domain resolution (heuristic candidates, verified by site content)
# --------------------------------------------------------------------------

def brand_root(mark: str) -> str:
    m = mark.lower()
    m = re.sub(r"\b(inc|llc|corp|corporation|co|company|ltd|llp|pllc|dba)\b\.?",
               " ", m)
    m = re.sub(r"[^a-z0-9]+", "", m)
    return m


def candidate_domains(root: str) -> list[str]:
    if not root or len(root) < 3:
        return []
    cands = [f"{root}.com", f"get{root}.com", f"try{root}.com",
             f"shop{root}.com", f"hello{root}.com", f"{root}.co"]
    return [d for d in cands if registrable_domain(d)]


def probe_domain(domain: str, tokens: list[str]) -> bool:
    """True when the site loads, is not parked, and the brand verifies in
    <title>/H1 text."""
    try:
        out = subprocess.run(
            ["curl", "-sS", "-L", "--max-time", str(PROBE_TIMEOUT),
             "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
             f"https://{domain}/"],
            capture_output=True, timeout=PROBE_TIMEOUT + 10, check=False)
    except Exception:
        return False
    if out.returncode != 0 or not out.stdout:
        return False
    html = out.stdout.decode("utf-8", "replace").lower()
    title = ""
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S)
    if m:
        title = re.sub(r"<[^>]+>", " ", m.group(1))
    h1s = " ".join(re.sub(r"<[^>]+>", " ", h)
                   for h in re.findall(r"<h1[^>]*>(.*?)</h1>", html, re.S))
    visible = f"{title} {h1s}"
    if word_boundary_hit(visible, PARKED_HINTS):
        return False
    hits = sum(1 for t in tokens if t in visible)
    if hits >= 2:
        return True
    compact = "".join(tokens)
    return bool(compact) and compact in visible.replace(" ", "")


def resolve_domain(mark: str) -> str | None:
    root = brand_root(mark)
    tokens = [t for t in re.findall(r"[a-z0-9]{4,}", mark.lower())]
    tokens = [t for t in tokens
              if t not in {"inc", "llc", "corp", "company", "ltd"}]
    if not tokens:
        return None
    for domain in candidate_domains(root):
        if probe_domain(domain, tokens):
            return domain
    return None


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-files", type=int, default=MAX_FILES_PER_RUN)
    args = ap.parse_args()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    USPTO_DIR.mkdir(parents=True, exist_ok=True)
    processed_file = USPTO_DIR / "processed.txt"
    processed = set()
    if processed_file.exists():
        processed = {l.strip() for l in processed_file.read_text().splitlines()
                     if l.strip()}

    try:
        files = list_product_files()
    except Exception as exc:
        print(f"ERROR: USPTO product listing failed: {exc}", file=sys.stderr)
        return 1
    new_files = [f for f in files
                 if f["name"] not in processed][:args.max_files]
    if not new_files:
        print(f"uspto worker {today}: no new daily files")
        return 0

    results: list[dict] = []
    rejects: list[dict] = []
    stats = {"parsed": 0, "candidates": 0, "resolved": 0}
    seen_serials: set[str] = set()
    candidates: list[tuple[dict, int]] = []
    per_niche: dict[str, int] = {}

    with tempfile.TemporaryDirectory() as tmp:
        for entry in new_files:
            dest = Path(tmp) / entry["name"]
            try:
                download_file(entry, dest)
            except Exception as exc:
                print(f"WARNING: download failed for {entry['name']}: {exc}",
                      file=sys.stderr)
                continue
            try:
                zf = zipfile.ZipFile(dest)
            except zipfile.BadZipFile as exc:
                print(f"WARNING: bad zip {entry['name']}: {exc}",
                      file=sys.stderr)
                continue
            for member in zf.namelist():
                if not member.lower().endswith(".xml"):
                    continue
                with zf.open(member) as fh:
                    for _event, elem in ET.iterparse(fh, events=("end",)):
                        if elem.tag != "case-file":
                            continue
                        try:
                            rec = parse_case_file(elem)
                        finally:
                            elem.clear()
                        if not rec["serial"] or rec["serial"] in seen_serials:
                            continue
                        seen_serials.add(rec["serial"])
                        stats["parsed"] += 1
                        keep, score, reason = classify(rec, today)
                        if keep:
                            candidates.append((rec, score))
                            stats["candidates"] += 1
                        else:
                            rejects.append({
                                "serial": rec["serial"],
                                "mark": rec["mark"][:80],
                                "classes": rec["classes"],
                                "score": score,
                                "reason": reason,
                                "filing_date": rec["filing_date"]})
            processed.add(entry["name"])

    if candidates:
        with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
            resolved = list(pool.map(lambda rc: (rc[0], rc[1],
                                                resolve_domain(rc[0]["mark"])),
                                     candidates))
        for rec, score, domain in resolved:
            if not domain:
                continue
            stats["resolved"] += 1
            top_class = max(rec["classes"],
                            key=lambda c: CLASS_SCORES.get(c, 0))
            niche = CLASS_TO_NICHE.get(top_class, "lifestyle")
            per_niche[niche] = per_niche.get(niche, 0) + 1
            results.append({
                "domain": domain,
                "mark": rec["mark"][:120],
                "website": f"https://{domain}",
                "country_code": "US",
                "niche": niche,
                "classes": rec["classes"],
                "serial": rec["serial"],
                "filing_date": rec["filing_date"],
                "owner": rec["owner"][:60],
                "score": score,
                "source": "uspto",
                "first_seen": today,
            })

    results_file = USPTO_DIR / f"results-{today}.json"
    results_file.write_text(json.dumps(results, indent=1) + "\n")
    if rejects:
        rfile = USPTO_DIR / f"rejects-{today}.jsonl"
        with rfile.open("a") as fh:
            for r in rejects:
                fh.write(json.dumps(r) + "\n")
    processed_file.write_text("\n".join(sorted(processed)) + "\n")

    print(f"uspto worker {today}: parsed={stats['parsed']} "
          f"candidates={stats['candidates']} resolved={stats['resolved']} "
          f"({per_niche}); wrote {len(results)} to {results_file.name}; "
          f"{len(rejects)} rejects logged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
