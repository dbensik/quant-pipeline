"""
core/news.py

One Yahoo news item to a stable shape. Shared by the Research page's live
feed (api/upstream.py) and the headline archive (core/news_archive.py), so a
story reads the same in both. Moved out of api/upstream.py on 2026-10-22
unchanged: core does not import from api.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _first_url(*candidates: Any) -> Optional[str]:
    for candidate in candidates:
        if isinstance(candidate, dict):
            url = candidate.get("url")
            if url:
                return url
        elif isinstance(candidate, str) and candidate:
            return candidate
    return None


def normalise_news_item(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    One yfinance news item to a stable shape.

    HANDLES BOTH LAYOUTS. yfinance >= 1.x nests everything under `content`
    ({"id": ..., "content": {"title", "pubDate", "provider", "canonicalUrl"}}),
    while older releases returned a flat dict with title/link/publisher/
    providerPublishTime.

    The Streamlit widget only ever read the flat keys. Against yfinance 1.2.0
    that meant every item had title None and link None — so its dedupe on
    `link` collapsed ten stories into one and rendered it as "[None](None)"
    from "Unknown", dated 1970-01-01. Reading both layouts is why this
    function exists.
    """
    if not isinstance(raw, dict):
        return None

    content = raw.get("content")
    if isinstance(content, dict):
        provider = content.get("provider") or {}
        published = content.get("pubDate") or content.get("displayTime")
        return {
            "id": str(content.get("id") or raw.get("id") or ""),
            "title": content.get("title"),
            "summary": content.get("summary") or content.get("description"),
            "url": _first_url(content.get("canonicalUrl"), content.get("clickThroughUrl")),
            "publisher": provider.get("displayName") if isinstance(provider, dict) else None,
            "published_at": _parse_time(published),
            "related_tickers": [],
        }

    # Legacy flat layout, which Yahoo's search still returns.
    return {
        "id": str(raw.get("uuid") or raw.get("id") or ""),
        "title": raw.get("title"),
        "summary": raw.get("summary"),
        "url": raw.get("link"),
        "publisher": raw.get("publisher"),
        "published_at": _parse_time(raw.get("providerPublishTime")),
        # Search matches text, so a story need not be about the symbol it was
        # found under: FOX's included Fox Factory and Terry Fox. The router
        # keeps a story only when these tags name the symbol or another of
        # its share classes (GOOGL's are tagged GOOG).
        "related_tickers": [str(t) for t in raw.get("relatedTickers") or []],
    }


def _parse_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # Epoch seconds, the legacy layout's providerPublishTime.
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
