"""
pricing/chains.py

One archived option-chain capture -> a normalised frame with our implied
volatility, then a surface: per-expiry smiles, the ATM term structure and the
25-delta skew. Pure: the caller supplies the rate and the dividend history.

Phase 2 of research/option-pricing-plan-2026-10-08.md. Three measured facts
shape it, each recorded in the plan:

1. NO RECORDED SPOT. The capture fetches every expiry in its own request and
   reads spot once, so quotes and spot are seconds apart and the gap differs
   per expiry (AAPL 2026-10-07: parity forward 0.09% above spot for the
   same-day expiry, 0.2% by mid-November). Each expiry is priced off its own
   put-call-parity forward F, in the forward measure (Black-76, i.e. the BSM
   pricer with S=F, q=r). The recorded spot is used for nothing.

2. AMERICAN EXERCISE IS NOT NEGLIGIBLE ON SPY. Near-the-money SPY puts a
   month or more out carry an early-exercise premium worth up to 20x the
   half-spread in vol terms. So every mid is DE-AMERICANISED: the premium
   (American minus European on one escrowed-dividend tree, at the current
   vol) is subtracted, the European formula inverted, and the premium
   re-priced at the new vol. The parity forward is biased by the same
   premium (puts are richer than parity assumes), so F and the premium are
   solved together until F stops moving.

3. DIVIDENDS ARE DISCRETE. A continuous trailing yield misprices the SPY put
   premium by 4.5-16x the half-spread around an ex-date, so projected
   discrete dividends (pricing/dividends.py) go into the tree.

QUALITY FLAGS (a flagged row keeps its vendor fields but is not in the surface)
    zero_bid         bid < OPTIONS_MIN_BID: the mid is not a price
    crossed          ask < bid
    locked           ask == bid (an infinite IV/half-spread ratio otherwise)
    zero_dte         under OPTIONS_EXCLUDE_DTE_BELOW days to expiry
    stale_last_trade informational only: the IV uses the quote, not the trade
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.stats import norm

from config.settings import (
    BINOMIAL_STEPS,
    OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS,
    OPTIONS_EXCLUDE_DTE_BELOW,
    OPTIONS_FORWARD_MAX_PASSES,
    OPTIONS_FORWARD_PAIRS,
    OPTIONS_FORWARD_TOL,
    OPTIONS_MIN_BID,
    OPTIONS_STALE_TRADE_DAYS,
)
from pricing.binomial import escrowed_prices
from pricing.black_scholes import implied_vol_black76_many
from pricing.dividends import Dividend, dividends_before, near_ex_date, pv_dividends

ET = ZoneInfo("America/New_York")
SECONDS_PER_YEAR = 365.0 * 86400.0
FLAG_COLUMNS = ("zero_bid", "crossed", "locked", "zero_dte")


def expiry_time(expiry: str) -> datetime:
    """Options stop trading at 16:00 New York time on the expiry date."""
    return datetime.combine(date.fromisoformat(expiry), time(16, 0), tzinfo=ET)


def normalise(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Vendor rows -> one row per contract with mid, T (ACT/365 seconds from the
    capture to 16:00 ET on expiry) and quality flags. Vendor IV kept beside.
    """
    if frame.empty:
        raise ValueError("empty chain")
    captured = pd.Timestamp(frame["captured_at_utc"].iloc[0]).to_pydatetime()
    out = pd.DataFrame(
        {
            "expiry": frame["expiry"].astype(str),
            "is_call": frame["right"].map({"C": True, "P": False}),
            "strike": frame["strike"].astype(float),
            "bid": frame["bid"].astype(float),
            "ask": frame["ask"].astype(float),
            "vendor_iv": frame["impliedVolatility"].astype(float),
            "open_interest": frame.get("openInterest", pd.Series(np.nan, index=frame.index)),
        }
    )
    if out["is_call"].isna().any():
        raise ValueError("right must be 'C' or 'P'")
    expiries = {e: (expiry_time(e) - captured).total_seconds() / SECONDS_PER_YEAR for e in out["expiry"].unique()}
    out["T"] = out["expiry"].map(expiries)
    out["mid"] = 0.5 * (out["bid"] + out["ask"])
    out["zero_bid"] = ~(out["bid"] >= OPTIONS_MIN_BID)
    out["crossed"] = out["ask"] < out["bid"]
    out["locked"] = out["ask"] == out["bid"]
    out["zero_dte"] = out["T"] < OPTIONS_EXCLUDE_DTE_BELOW / 365.0
    if "lastTradeDate" in frame:
        age = (pd.Timestamp(captured) - pd.to_datetime(frame["lastTradeDate"], utc=True)).dt.total_seconds() / 86400
        out["stale_last_trade"] = (age > OPTIONS_STALE_TRADE_DAYS).fillna(True).to_numpy()
    else:
        out["stale_last_trade"] = False
    out["usable"] = ~out[list(FLAG_COLUMNS)].any(axis=1)
    out.attrs["captured_at"] = captured
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# De-americanisation and the joint forward solve
# ---------------------------------------------------------------------------

def _schedules(rows: pd.DataFrame, dividends: Sequence[Dividend], valuation: date) -> List[List[Tuple[float, float]]]:
    cache: Dict[str, List[Tuple[float, float]]] = {}
    out = []
    for e in rows["expiry"]:
        if e not in cache:
            cache[e] = dividends_before(dividends, valuation, date.fromisoformat(e))
        out.append(cache[e])
    return out


def deamericanise(
    mids, F, K, T, r: float, is_call, schedules, passes: int = 2, steps: int = BINOMIAL_STEPS
):
    """
    European-equivalent prices: mid minus the early-exercise premium, the
    premium priced at the vol the corrected price implies. Returns
    (european_price, premium, sigma, reason).
    """
    mids, F, K, T = (np.asarray(x, dtype=float) for x in (mids, F, K, T))
    is_call = np.asarray(is_call, dtype=bool)
    # Escrowed tree on the risky part: F = S_risky * e^{rT} by definition.
    s_risky = F * np.exp(-r * T)
    sigma, reason = implied_vol_black76_many(mids, F, K, T, r, is_call)
    premium = np.zeros_like(mids)
    # The escrowed tree has no yield, so its probability stays inside (0, 1)
    # only while sigma sqrt(dt) > r dt. A row below that floor cannot be
    # priced on the lattice; it gets its own reason rather than raising for
    # the whole batch (one bad row would otherwise fail a ticker-day).
    floor = 1.001 * abs(r) * np.sqrt(T / steps)
    for _ in range(passes):
        sub_floor = (reason == "") & ~(sigma > floor)
        reason = np.where(sub_floor, "below_tree_floor", reason)
        ok = reason == ""
        if not ok.any():
            break
        idx = np.flatnonzero(ok)
        am, eu = escrowed_prices(
            s_risky[idx], K[idx], T[idx], r, sigma[idx], is_call[idx],
            divs=[schedules[i] for i in idx], steps=steps,
        )
        premium[idx] = np.maximum(am - eu, 0.0)
        floored = reason == "below_tree_floor"
        sigma, reason = implied_vol_black76_many(mids - premium, F, K, T, r, is_call)
        reason = np.where(floored, "below_tree_floor", reason)
        sigma = np.where(floored, np.nan, sigma)
    sub_floor = (reason == "") & ~(sigma > floor)
    reason = np.where(sub_floor, "below_tree_floor", reason)
    sigma = np.where(reason == "", sigma, np.nan)
    return mids - premium, premium, sigma, reason


def _parity_forward(calls: pd.DataFrame, puts: pd.DataFrame, T: float, r: float, around: Optional[float]) -> Optional[float]:
    """
    Median of K + e^{rT}(C - P) over the OPTIONS_FORWARD_PAIRS usable pairs
    nearest the money: nearest `around` if given, else where |C - P| is
    smallest (which is the money, with no spot needed).
    """
    pairs = calls.join(puts, lsuffix="_c", rsuffix="_p", how="inner")
    if pairs.empty:
        return None
    if around is None:
        distance = (pairs["price_c"] - pairs["price_p"]).abs()
    else:
        distance = (pairs.index.to_series() - around).abs()
    near = pairs.loc[distance.nsmallest(OPTIONS_FORWARD_PAIRS).index]
    estimates = near.index.to_numpy() + np.exp(r * T) * (near["price_c"] - near["price_p"]).to_numpy()
    return float(np.median(estimates))


@dataclass
class ForwardSolve:
    forward: float
    passes: int
    raw_forward: float
    converged: bool


def solve_forward(rows: pd.DataFrame, r: float, schedules, steps: int = BINOMIAL_STEPS) -> Optional[ForwardSolve]:
    """
    One expiry's usable rows -> its parity forward, solved jointly with the
    early-exercise premium of the pairs it is read from.
    """
    T = float(rows["T"].iloc[0])
    calls = rows[rows["is_call"]].set_index("strike")
    puts = rows[~rows["is_call"]].set_index("strike")
    common = calls.index.intersection(puts.index)
    if len(common) == 0:
        return None
    calls, puts = calls.loc[common].copy(), puts.loc[common].copy()
    calls["price"], puts["price"] = calls["mid"], puts["mid"]
    raw = _parity_forward(calls[["price"]], puts[["price"]], T, r, None)
    if raw is None:
        return None
    forward = raw
    sched = {"c": schedules, "p": schedules}  # one expiry: one schedule
    for n in range(1, OPTIONS_FORWARD_MAX_PASSES + 1):
        near = (pd.Series(common, index=common) - forward).abs().nsmallest(2 * OPTIONS_FORWARD_PAIRS).index
        k = np.asarray(near, dtype=float)
        mids = np.concatenate([calls.loc[near, "mid"].to_numpy(), puts.loc[near, "mid"].to_numpy()])
        flags = np.concatenate([np.ones(k.size, bool), np.zeros(k.size, bool)])
        eu, _, _, why = deamericanise(
            mids, np.full(2 * k.size, forward), np.concatenate([k, k]), np.full(2 * k.size, T), r,
            flags, [sched["c"]] * (2 * k.size), steps=steps,
        )
        c_eu = pd.DataFrame({"price": eu[: k.size]}, index=near)[why[: k.size] == ""]
        p_eu = pd.DataFrame({"price": eu[k.size :]}, index=near)[why[k.size :] == ""]
        new = _parity_forward(c_eu, p_eu, T, r, forward)
        if new is None:
            return ForwardSolve(forward, n, raw, False)
        if abs(new - forward) <= OPTIONS_FORWARD_TOL * forward:
            return ForwardSolve(new, n, raw, True)
        forward = new
    return ForwardSolve(forward, OPTIONS_FORWARD_MAX_PASSES, raw, False)


# ---------------------------------------------------------------------------
# The whole capture
# ---------------------------------------------------------------------------

@dataclass
class ChainResult:
    rows: pd.DataFrame
    expiries: pd.DataFrame
    quality: Dict[str, int] = field(default_factory=dict)


def price_chain(
    frame: pd.DataFrame,
    r: float,
    dividends: Sequence[Dividend] = (),
    steps: int = BINOMIAL_STEPS,
) -> ChainResult:
    """
    Normalise, solve each expiry's forward, de-americanise every usable row,
    and attach IV, forward delta and log-moneyness. `r` is the continuously
    compounded risk-free rate for the capture date.
    """
    rows = normalise(frame)
    valuation = rows.attrs["captured_at"].astimezone(ET).date()
    rows["F"] = np.nan
    expiry_info = []
    for expiry, g in rows[rows["usable"]].groupby("expiry", sort=True):
        schedule = dividends_before(dividends, valuation, date.fromisoformat(expiry))
        solve = solve_forward(g, r, schedule, steps=steps)
        if solve is None:
            continue
        rows.loc[rows["expiry"] == expiry, "F"] = solve.forward
        T = float(g["T"].iloc[0])
        expiry_info.append(
            {
                "expiry": expiry,
                "T": T,
                "forward": solve.forward,
                "raw_parity_forward": solve.raw_forward,
                "forward_passes": solve.passes,
                "forward_converged": solve.converged,
                # The spot each expiry's quotes imply: F e^{-rT} + PV(dividends).
                "implied_spot": solve.forward * np.exp(-r * T) + pv_dividends(schedule, r),
                "dividends_before_expiry": len(schedule),
                "dividend_date_uncertain": near_ex_date(
                    dividends, date.fromisoformat(expiry), OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS
                ),
            }
        )
    expiries = pd.DataFrame(expiry_info)

    priced = rows["usable"] & rows["F"].notna()
    idx = np.flatnonzero(priced.to_numpy())
    rows["premium"] = np.nan
    rows["iv"] = np.nan
    rows["iv_reason"] = np.where(rows["usable"], "no_forward", "flagged")
    if idx.size:
        sub = rows.iloc[idx]
        eu, premium, sigma, reason = deamericanise(
            sub["mid"], sub["F"], sub["strike"], sub["T"], r, sub["is_call"],
            _schedules(sub, dividends, valuation), steps=steps,
        )
        rows.loc[rows.index[idx], "premium"] = premium
        rows.loc[rows.index[idx], "iv"] = sigma
        rows.loc[rows.index[idx], "iv_reason"] = np.where(reason == "", "", reason)
    rows["log_moneyness"] = np.log(rows["strike"] / rows["F"])
    rows["otm"] = np.where(rows["is_call"], rows["strike"] >= rows["F"], rows["strike"] < rows["F"])
    d1 = (-rows["log_moneyness"] + 0.5 * rows["iv"] ** 2 * rows["T"]) / (rows["iv"] * np.sqrt(rows["T"]))
    rows["forward_delta"] = np.where(rows["is_call"], norm.cdf(d1), norm.cdf(d1) - 1.0)

    quality = {flag: int(rows[flag].sum()) for flag in (*FLAG_COLUMNS, "stale_last_trade")}
    quality.update(
        rows=int(len(rows)),
        priced=int((rows["iv_reason"] == "").sum()),
        no_iv=int(((rows["iv_reason"] != "") & (rows["iv_reason"] != "flagged")).sum()),
    )
    if not expiries.empty:
        quality["implied_spot_spread_bp"] = float(
            1e4 * (expiries["implied_spot"].max() - expiries["implied_spot"].min()) / expiries["implied_spot"].median()
        )
    return ChainResult(rows=rows, expiries=expiries, quality=quality)


# ---------------------------------------------------------------------------
# Surface
# ---------------------------------------------------------------------------

def smile(rows: pd.DataFrame, expiry: str) -> pd.DataFrame:
    """The surface points for one expiry: out-of-the-money, priced, by strike."""
    g = rows[(rows["expiry"] == expiry) & rows["otm"] & (rows["iv_reason"] == "")]
    return g.sort_values("strike")[["strike", "is_call", "log_moneyness", "forward_delta", "iv", "vendor_iv", "bid", "ask", "premium"]]


def _interp(x: np.ndarray, y: np.ndarray, at: float) -> float:
    order = np.argsort(x)
    x, y = x[order], y[order]
    if x.size < 2 or not (x[0] <= at <= x[-1]):
        return float("nan")
    return float(np.interp(at, x, y))


def term_structure(result: ChainResult) -> pd.DataFrame:
    """
    Per expiry: ATM vol (interpolated at log-moneyness 0 across the OTM
    smile), the 25-delta put and call vols, skew = put25 - call25, points.
    Expiries flagged zero_dte are absent: their rows are not usable.
    """
    out = []
    for _, e in result.expiries.iterrows():
        s = smile(result.rows, e["expiry"])
        if len(s) < 3:
            continue
        atm = _interp(s["log_moneyness"].to_numpy(), s["iv"].to_numpy(), 0.0)
        puts, calls = s[~s["is_call"]], s[s["is_call"]]
        put25 = _interp(puts["forward_delta"].to_numpy(), puts["iv"].to_numpy(), -0.25)
        call25 = _interp(calls["forward_delta"].to_numpy(), calls["iv"].to_numpy(), 0.25)
        out.append(
            {
                "expiry": e["expiry"],
                "T": e["T"],
                "forward": e["forward"],
                "atm_iv": atm,
                "put25_iv": put25,
                "call25_iv": call25,
                "skew_25d": put25 - call25,
                "points": len(s),
                "dividend_date_uncertain": bool(e["dividend_date_uncertain"]),
            }
        )
    return pd.DataFrame(out)
