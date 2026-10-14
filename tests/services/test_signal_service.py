"""
services/grpc_service — what a signed signal's value and confidence mean.

A strategy's signal is an EVENT (1 enter, -1 exit, 0 no change). Until
2026-10-14 the service signed the latest event, so a strategy long for weeks
was served as 0 ("flat") on every bar but its entry; and its confidence read
a z_score column mean_reversion never returned, so it was always 0.5.
Checked failing against both. yfinance is stubbed: no network.
"""

import json
from unittest import mock

import numpy as np
import pandas as pd

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


def test_the_signed_value_is_the_position_and_confidence_reads_the_z_score(tmp_path):
    df = held_long_frame()
    from alpha_models.mean_reversion import MeanReversionStrategy

    strategy = MeanReversionStrategy(window=20, threshold=1.0)
    events = strategy.generate_signals(df)["signal"]
    assert events.iloc[-1] == 0 and events.sum() == 1, "fixture: entered earlier, still long"
    z = strategy.z_score(df["Close"]).iloc[-1]

    svc = server.SignalService(
        Signer.from_config(None), AuditLog(file_path=str(tmp_path / "audit.json"))
    )
    context = mock.Mock()
    with mock.patch.object(server.yf, "download", return_value=df):
        resp = svc.GetSignal(signals_pb2.SignalRequest(symbol="X", as_of="2026-03-01"), context)

    assert resp.value == 1.0
    assert resp.confidence == min(0.99, max(0.5, abs(z) / 2.0)) and resp.confidence != 0.5
    assert json.loads(resp.payload_json)["value"] == 1.0
