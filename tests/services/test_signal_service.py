"""
services/grpc_service — what a signed signal's value and confidence mean.

A strategy's signal is an EVENT (1 enter, -1 exit, 0 no change). Until
2026-10-14 the service signed the latest event, so a strategy long for weeks
was served as 0 ("flat") on every bar but its entry; and its confidence read
a z_score column mean_reversion never returned, so it was always 0.5.
Checked failing against both.

Bars come from TimescaleDB through services/grpc_service/bars.py (until
2026-10-14 straight from yfinance by the bare symbol: "BTC" was not
Bitcoin but the Grayscale Bitcoin Mini Trust ETF), on or before the request's as_of, which had been ignored. The
reader is injected here: no network, no database.
"""

import json
from datetime import date
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from services.crypto.audit_log import AuditLog
from services.crypto.signer import Signer
from services.grpc_service import server
from services.proto import signals_pb2


def held_long_frame() -> pd.DataFrame:
    """Flat, then a drop below -1 sd, then still below the mean on the last bar."""
    rng = np.random.default_rng(0)
    close = list(100 + rng.normal(0, 0.5, 40)) + [96.0, 96.5, 97.0, 97.5]
    idx = pd.bdate_range("2026-01-02", periods=len(close))
    c = pd.Series(close, index=idx)
    return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1e6})


class FakeBars:
    def __init__(self, frames):
        self.frames, self.calls = frames, []

    def frame(self, symbol, as_of=None):
        self.calls.append((symbol, as_of))
        return self.frames.get(
            symbol, pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        )


def service(tmp_path, bars):
    return server.SignalService(
        Signer.from_config(None), AuditLog(file_path=str(tmp_path / "audit.json")), bars
    )


def test_the_signed_value_is_the_position_and_confidence_reads_the_z_score(tmp_path):
    df = held_long_frame()
    from alpha_models.mean_reversion import MeanReversionStrategy

    strategy = MeanReversionStrategy(window=20, threshold=1.0)
    events = strategy.generate_signals(df)["signal"]
    assert events.iloc[-1] == 0 and events.sum() == 1, "fixture: entered earlier, still long"
    z = strategy.z_score(df["Close"]).iloc[-1]

    bars = FakeBars({"X": df})
    resp = service(tmp_path, bars).GetSignal(
        signals_pb2.SignalRequest(symbol="X", as_of="2026-03-01"), mock.Mock()
    )
    assert bars.calls == [("X", date(2026, 3, 1))], "as_of must reach the read"

    assert resp.value == 1.0
    assert resp.confidence == min(0.99, max(0.5, abs(z) / 2.0)) and resp.confidence != 0.5
    assert json.loads(resp.payload_json)["value"] == 1.0


def test_the_audit_entry_carries_the_signature_the_caller_got(tmp_path):
    """Since 2026-10-15 the log keeps the seal, so it can prove who issued an entry."""
    svc = service(tmp_path, FakeBars({"X": held_long_frame()}))
    resp = svc.GetSignal(signals_pb2.SignalRequest(symbol="X", as_of="2026-03-01"), mock.Mock())
    entry = AuditLog(file_path=str(tmp_path / "audit.json")).get_log()[-1]
    assert entry["payload_hash"] == resp.features_hash
    assert entry["signature"] == resp.signature
    assert entry["public_key_id"] == resp.public_key_id
    assert Signer.verify(
        svc.signer.get_public_key_pem(), resp.features_hash.encode(), entry["signature"]
    )


def test_a_symbol_the_registry_does_not_hold_is_not_found(tmp_path):
    context = mock.Mock()
    resp = service(tmp_path, FakeBars({})).GetSignal(
        signals_pb2.SignalRequest(symbol="BTC", as_of="2023-01-01"), context
    )
    context.set_code.assert_called_once_with(server.grpc.StatusCode.NOT_FOUND)
    assert resp.signature == ""


def test_a_malformed_as_of_is_refused(tmp_path):
    context = mock.Mock()
    bars = FakeBars({"X": held_long_frame()})
    service(tmp_path, bars).GetSignal(
        signals_pb2.SignalRequest(symbol="X", as_of="01/03/2026"), context
    )
    context.set_code.assert_called_once_with(server.grpc.StatusCode.INVALID_ARGUMENT)
    assert bars.calls == []


@pytest.mark.integration
def test_the_reader_serves_stored_bars_on_or_before_as_of():
    # Real TimescaleDB: SPY's bars end on or before as_of; a symbol the
    # registry does not hold ("BTC", which Yahoo serves as another security)
    # has none; the reader's own loop serves repeated calls.
    from services.grpc_service.bars import BarReader

    try:
        reader = BarReader()
        spy = reader.frame("SPY", date(2026, 10, 1))
    except OSError as exc:  # pragma: no cover - database down
        pytest.skip(f"TimescaleDB unreachable: {exc}")
    assert not spy.empty and spy.index.max().date() <= date(2026, 10, 1)
    assert (spy.index.max() - spy.index.min()).days >= 150
    assert reader.frame("BTC", date(2023, 1, 1)).empty
    assert not reader.frame("BTC-USD", date(2023, 1, 1)).empty
