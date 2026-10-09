"""
core/sec.py

The one place this project talks to SEC EDGAR. Tier 3 phase 0 of
research/dcf-plan-2026-10-08.md.

FOUR RULES, each tested with a fake session (tests/unit/test_sec_gateway.py):

1. NO IDENTITY WITHOUT CONSENT. The User-Agent the SEC requires ("Name
   email@domain") comes only from SEC_USER_AGENT in .env. Unset, or not
   shaped like a name and an email, the gateway refuses to construct and
   names the setting. It never falls back to anything.
2. HEADER ONLY. The User-Agent travels as a header; nothing identifying goes
   in a URL (URLs land in logs).
3. STOP ON A BLOCK. An undeclared or over-eager client gets an HTML page with
   status 403, not JSON. That is raised as SECBlockedError and the run stops,
   rather than a parser choking on HTML or a loop hammering a block.
4. STAY UNDER THE LIMIT. Requests are spaced to SEC_MAX_REQUESTS_PER_SECOND
   (8, under the published 10) by a clock the tests can fake.
"""

from __future__ import annotations

import re
import time
from typing import Any, Callable, Dict, Optional

from config import settings

_UA_SHAPE = re.compile(r"^\S.*\s\S+@\S+\.\S+$")


class SECConfigError(RuntimeError):
    pass


class SECBlockedError(RuntimeError):
    pass


class SECThrottledError(RuntimeError):
    pass


def validated_user_agent(value: Optional[str]) -> str:
    if not value:
        raise SECConfigError(
            "SEC_USER_AGENT is not set. The SEC requires a declared client: add a line "
            "SEC_USER_AGENT=Your Name you@example.com to .env. Nothing is sent without it."
        )
    if not _UA_SHAPE.match(value.strip()):
        raise SECConfigError(
            f"SEC_USER_AGENT must be 'Name email@domain' (a name, a space, an email); got {value!r}."
        )
    return value.strip()


class SECGateway:
    def __init__(
        self,
        session: Any = None,
        user_agent: Optional[str] = None,
        max_per_second: float = settings.SEC_MAX_REQUESTS_PER_SECOND,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.user_agent = validated_user_agent(user_agent if user_agent is not None else settings.sec_user_agent())
        if session is None:
            import requests

            session = requests.Session()
        self.session = session
        self.min_interval = 1.0 / max_per_second
        self.clock, self.sleep = clock, sleep
        self._last: Optional[float] = None
        self.requests_made = 0

    def _throttle(self) -> None:
        now = self.clock()
        if self._last is not None:
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                self.sleep(wait)
                now = self.clock()
        self._last = now

    def get_json(self, url: str) -> Dict[str, Any]:
        self._throttle()
        response = self.session.get(
            url,
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=settings.SEC_TIMEOUT_SECONDS,
        )
        self.requests_made += 1
        ctype = (response.headers.get("Content-Type") or "").lower()
        if response.status_code == 429:
            raise SECThrottledError(f"SEC throttled the client (429) at {url}; stop and retry later")
        if response.status_code == 403 or "html" in ctype:
            title = re.search(r"<title>(.*?)</title>", response.text or "", re.S | re.I)
            raise SECBlockedError(
                f"SEC refused the request ({response.status_code}): "
                f"{title.group(1).strip() if title else 'HTML instead of JSON'}. Stopping."
            )
        if response.status_code == 404:
            raise LookupError(f"SEC has no document at {url}")
        if response.status_code != 200:
            raise RuntimeError(f"SEC returned {response.status_code} for {url}")
        return response.json()

    def company_facts(self, cik: int) -> Dict[str, Any]:
        return self.get_json(settings.URL_SEC_COMPANY_FACTS.format(cik=int(cik)))

    def submissions(self, cik: int) -> Dict[str, Any]:
        return self.get_json(settings.URL_SEC_SUBMISSIONS.format(cik=int(cik)))

    def company_tickers(self) -> Dict[str, Any]:
        return self.get_json(settings.URL_SEC_COMPANY_TICKERS)
