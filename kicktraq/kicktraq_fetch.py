#!/usr/bin/env python3
"""Kicktraq RSS fetch for GitHub Actions.

Stateless fetch proxy: fetches all 7 Kicktraq RSS feeds, parses campaigns,
outputs JSON. No DB, no seen-set, no ledger — local side handles dedup.

Usage: python3 kicktraq_fetch.py --out campaigns.json
"""
import argparse
import html
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import urllib.request
import urllib.error

RSS_FEEDS = [
    {"url": "https://www.kicktraq.com/categories/technology/latest.rss",
     "category": "Technology", "subcategory": None},
    {"url": "https://www.kicktraq.com/categories/technology/apps/latest.rss",
     "category": "Technology", "subcategory": "Apps"},
    {"url": "https://www.kicktraq.com/categories/technology/software/latest.rss",
     "category": "Technology", "subcategory": "Software"},
    {"url": "https://www.kicktraq.com/categories/technology/web/latest.rss",
     "category": "Technology", "subcategory": "Web"},
    {"url": "https://www.kicktraq.com/categories/technology/hardware/latest.rss",
     "category": "Technology", "subcategory": "Hardware"},
    {"url": "https://www.kicktraq.com/categories/design/product%20design/latest.rss",
     "category": "Design", "subcategory": "Product Design"},
    {"url": "https://www.kicktraq.com/categories/design/graphic%20design/latest.rss",
     "category": "Design", "subcategory": "Graphic Design"},
]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _fix_double_utf8(text):
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


def _strip_html(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _fix_double_utf8(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_item(item, feed):
    """Parse RSS item into campaign dict (mirrors intake/kicktraq.py)."""
    title = item.findtext("title") or ""
    link = item.findtext("link") or ""
    desc = item.findtext("description") or ""
    guid = item.findtext("guid") or link
    
    # Extract kicktraq URL and source_id
    kicktraq_url = link
    source_id = guid or link
    
    # Parse description for metrics
    text = _strip_html(desc)
    result = {
        "source_id": source_id,
        "title": _strip_html(title),
        "kicktraq_url": kicktraq_url,
        "campaign_category": feed["category"],
        "campaign_subcategory": feed["subcategory"],
        "description_text": text[:2000],
    }
    
    # Extract funding metrics
    m = re.search(r"\((\d+(?:,\d+)?(?:\.\d+)?)\s*%\)", text)
    if m:
        try:
            result["percent_funded"] = float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    
    return result


def fetch_feed(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        print(f"Feed fetch failed {url}: {e}", file=sys.stderr)
        return None


def main(out_path):
    campaigns = []
    seen_ids = set()
    
    for feed in RSS_FEEDS:
        print(f"Fetching {feed['url']}...", file=sys.stderr)
        data = fetch_feed(feed["url"])
        if not data:
            continue
        try:
            root = ET.fromstring(data)
        except ET.ParseError as e:
            print(f"Parse failed {feed['url']}: {e}", file=sys.stderr)
            continue
        
        channel = root.find("channel")
        items = channel.findall("item") if channel is not None else root.findall("item")
        
        for item in items:
            try:
                campaign = _parse_item(item, feed)
            except Exception as e:
                print(f"Item parse failed: {e}", file=sys.stderr)
                continue
            
            sid = campaign.get("source_id")
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            campaigns.append(campaign)
    
    out = {
        "fetched_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "count": len(campaigns),
        "campaigns": campaigns,
    }
    
    Path(out_path).write_text(json.dumps(out, indent=2))
    print(f"Fetched {len(campaigns)} campaigns -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output JSON path")
    args = parser.parse_args()
    main(args.out)
