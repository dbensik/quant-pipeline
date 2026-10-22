"""
core/svi_report.py on a synthetic surface payload: which expiries are fitted,
the share within the spread, the parameter flags, the calendar pairs and the
in-process cache.
"""

import numpy as np
import pytest
from scipy.stats import norm

from core.svi_report import ReportCache, build_report
from pricing.svi import SVIParams

TRUE = SVIParams(a=0.002, b=0.05, rho=-0.6, m=0.03, sigma=0.08)
RATE = 0.04
F = 100.0


def _points(T, k, iv, half_spread_vol):
    """Smile points whose bid-ask spread is `half_spread_vol` wide either side, in vol."""
    s = iv * np.sqrt(T)
    vega = np.exp(-RATE * T) * F * np.sqrt(T) * norm.pdf(-k / s + s / 2)
    out = []
    for ki, v, vg, h in zip(k, iv, vega, half_spread_vol, strict=True):
        mid = 1.0
        out.append(
            {
                "strike": float(F * np.exp(ki)),
                "is_call": bool(ki >= 0),
                "log_moneyness": float(ki),
                "iv": float(v),
                "bid": mid - h * vg,
                "ask": mid + h * vg,
            }
        )
    return out


def surface(expiries):
    """expiries: (name, T, points)"""
    return {
        "key": "k1",
        "ticker": "TEST",
        "day": "2026-10-20",
        "captured_at": "2026-10-20T19:45:00+00:00",
        "rate": {"continuous": RATE},
        "expiries": [{"expiry": e, "T": T, "forward": F} for e, T, _ in expiries],
        "smiles": {e: pts for e, _, pts in expiries},
    }


def test_short_and_thin_expiries_are_skipped_with_a_reason():
    k = np.linspace(-0.2, 0.2, 12)
    T = 30 / 365
    body = build_report(
        surface(
            [
                (
                    "2026-10-21",
                    1 / 365,
                    _points(1 / 365, k, TRUE.iv(k, 1 / 365), np.full(12, 0.01)),
                ),
                ("2026-11-06", 17 / 365, _points(T, k[:7], TRUE.iv(k[:7], T), np.full(7, 0.01))),
                ("2026-11-20", T, _points(T, k, TRUE.iv(k, T), np.full(12, 0.01))),
            ]
        )
    )
    assert [f["expiry"] for f in body["fits"]] == ["2026-11-20"]
    reasons = {s["expiry"]: s["reason"] for s in body["skipped"]}
    assert "under 2 days" in reasons["2026-10-21"]
    assert "7 smile points" in reasons["2026-11-06"]


def test_share_within_the_spread_counts_quotes_the_fit_passes_inside():
    T = 60 / 365
    k = np.linspace(-0.25, 0.2, 20)
    # Every other quote is moved 1 vol point off the true smile; the rest
    # sit on it, so the fit misses each by about half a point. Half the
    # quotes get a 2-point half-spread, half 0.4 points: inside the miss,
    # where a spread counted whole rather than halved would pass them.
    bump = np.where(np.arange(20) % 2, 0.01, 0.0)
    wide = np.where(np.arange(20) < 10, 0.02, 0.004)
    pts = _points(T, k, TRUE.iv(k, T) + bump, wide)
    [fit] = build_report(surface([("2026-12-18", T, pts)]))["fits"]
    fitted = SVIParams(**fit["params"]).iv(k, T)
    miss = np.abs(fitted - (TRUE.iv(k, T) + bump))
    expected = np.mean(miss <= wide)
    assert fit["within_spread_share"] == pytest.approx(expected)
    assert 0 < expected < 1
    assert np.mean(miss <= 2 * wide) > expected  # the halving matters here


def test_parameters_are_marked_readable_only_with_the_bottom_among_the_quotes():
    T = 60 / 365
    k_around = np.linspace(-0.2, 0.2, 15)  # TRUE.m = 0.03 lies inside
    k_left = np.linspace(-0.4, -0.1, 15)  # all below the bottom
    body = build_report(
        surface(
            [
                ("2026-12-18", T, _points(T, k_around, TRUE.iv(k_around, T), np.full(15, 0.01))),
                (
                    "2027-01-15",
                    2 * T,
                    _points(2 * T, k_left, TRUE.iv(k_left, 2 * T), np.full(15, 0.01)),
                ),
            ]
        )
    )
    first, second = body["fits"]
    assert first["bottom_quoted"] is True
    assert first["params"]["m"] == pytest.approx(TRUE.m, abs=1e-5)
    assert second["params"]["m"] > -0.1  # past the last quote
    assert second["bottom_quoted"] is False
    assert first["wings_within_lee"] is True
    assert first["atm_iv"] == pytest.approx(float(TRUE.iv(0.0, T)), abs=1e-6)
    assert len(first["curve"]) == 101
    assert first["curve"][0]["log_moneyness"] == pytest.approx(-0.3)


def test_calendar_pairs_are_adjacent_in_time_whatever_the_payload_order():
    k = np.linspace(-0.2, 0.2, 12)
    rows = [
        (e, T, _points(T, k, TRUE.iv(k, 0.08), np.full(12, 0.01)))
        for e, T in [("2027-01-15", 0.24), ("2026-11-20", 0.08), ("2026-12-18", 0.16)]
    ]
    body = build_report(surface(rows))
    assert [(c["near"], c["far"]) for c in body["calendar"]] == [
        ("2026-11-20", "2026-12-18"),
        ("2026-12-18", "2027-01-15"),
    ]
    # The same vols at a longer T: total variance rises, nothing crosses.
    assert body["calendar_summary"] == {"none": 2, "both": 0, "fit_only": 0, "quotes_only": 0}


def test_reports_are_cached_by_surface_key():
    k = np.linspace(-0.2, 0.2, 12)
    T = 0.1
    payload = surface([("2026-11-27", T, _points(T, k, TRUE.iv(k, T), np.full(12, 0.01)))])
    cache = ReportCache(size=1)
    first = cache.get_or_build(payload)
    assert cache.get_or_build(payload) is first
    other = {**payload, "key": "k2"}
    assert cache.get_or_build(other) is not first
    assert cache.get_or_build(payload) is not first  # evicted at size 1
