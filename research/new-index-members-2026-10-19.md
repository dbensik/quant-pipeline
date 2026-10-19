# Registering companies that join an index (decision 44, 2026-10-19)

Nothing adds a new index member to the asset registry. The daily job's
header says "Ingest runs FIRST so that any name which joined an index today
already has bars by the time the snapshot records it". That is not true:
ingest fetches only registered assets, and the snapshot records membership
without registering anyone. Decision 42 found 30 current S&P 500 members
with no prices. Most had been members since the first snapshot, because the
registry was built from an older list. 29 were added by hand on 2026-10-19.

## How often it happens

New members since the first snapshot (2026-08-09) up to 2026-10-19:

| index | joiners in 10 weeks | not registered today |
|---|---|---|
| sp500 | 8 (2 on 08-25, 3 on 09-21, 3 on 10-08) | 0, after today's backfill |
| dow_jones | 0 | 0 |
| top_100_crypto | about 25 | **41 of 103** members |

The S&P 500 changes 20-25 names a year, so leaving this unattended loses
about two names a month from every point-in-time screen.

## What adding the 30 names by hand showed

Registering a ticker blindly is not safe. Of the 30:

- **1 was a rename, not a new company.** SKYD is PSKY renamed: same CIK,
  identical prices. Registering it would have created a second row for one
  instrument.
- **2 carried placeholder history.** FERG had an illiquid line back to
  2010, 2,079 of its bars with zero volume. P had a zero-volume bar the day
  before its IPO. Both needed a start-date floor.
- **1 carried a shell company's history.** VRT's pre-merger SPAC bars.
  `check_reassigned` caught it the same day.
- **Crypto is worse.** A ticker is not an identifier: 22 coins once held
  another token's history. Every crypto asset has to pass the identity audit
  before it is ingested.

## Decision

44. **Recommend A.**

    - **A: a step in the daily job, after the snapshot. Equities registered
      with guards; crypto flagged only.** For each current sp500 and
      dow_jones member not in the registry, after the share-class and dated
      rename mapping decision 42 uses:
      - **Rename guard.** Look up its CIK in SEC's ticker map, which the
        06:00 SEC step already reads. If a registered asset has that CIK,
        do not register it. Flag "possible rename of X"; you re-key it, as
        SKYD was.
      - Otherwise register it (equity, yfinance), backfill insert-only from
        2010 through `core/ingest.py`, record the CIK, and mark it
        `served` only if every bar has served values.
      - **Placeholder guard.** Zero-volume bars at the start of the new
        series (the FERG and P pattern) are flagged with a suggested floor.
        Nothing is trimmed automatically; deleting bars stays your decision.
      - `top_100_crypto`: list unregistered members in the log and the
        notification, but never register one. Each needs the identity audit
        first.
      - Exit 1 with a notification when anything is flagged. A clean
        registration logs one line.
      - Also correct the job header's claim about ingest.
      - Tests: a joiner is registered and backfilled; a same-CIK joiner is
        flagged and not registered; a zero-volume prefix is flagged and not
        trimmed; a crypto joiner is never registered; each fails against
        the current job.
      - About 2.5-3.5 h of my time.
    - **B: flag only, every index.** The job lists unregistered members
      every morning, and you register them by hand, about two S&P names a
      month. Safest, more manual. About 1 h.
    - **C: as now.** Point-in-time screens lose about two members a month.

    **Separately, not proposed here:** the 41 unregistered top-100 crypto
    members are mostly stablecoins and tokenized assets (USDG, PYUSD,
    RLUSD, PAXG, XAUT, USTB, BUIDL-like funds). Whether the crypto universe
    should hold them at all is its own question.
