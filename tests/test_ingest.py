import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline import ingest, quality


def _write_partition(raw: Path, day: str, rows: list[dict]) -> None:
    d = raw / "transactions" / f"dt={day}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "part.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def _row(tid, amount=10.0, customer="C1", currency="EUR"):
    return {"transaction_id": tid, "event_ts": "2026-01-01T10:00:00", "customer_id": customer,
            "merchant_id": "M1", "amount": amount, "currency": currency, "channel": "POS",
            "status": "APPROVED", "is_fraud": False}


@pytest.fixture
def env(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    pd.DataFrame({"customer_id": ["C1"], "segment": ["premium"], "city": ["Lyon"],
                  "created_at": ["2024-01-01"]}).to_csv(raw / "customers.csv", index=False)
    pd.DataFrame({"merchant_id": ["M1"], "mcc": ["5411"], "city": ["Lyon"],
                  "category": ["Alimentation"]}).to_csv(raw / "merchants.csv", index=False)
    con = ingest.connect(tmp_path / "wh.duckdb")
    ingest.load_referentials(con, raw)
    return raw, con


def test_rejets_et_doublons(env):
    raw, con = env
    _write_partition(raw, "2026-01-01", [
        _row("T1"), _row("T1"),              # doublon
        _row("T2", amount=-5),               # montant négatif
        _row("T3", customer=None),           # clé manquante
        _row("T4", currency="XXX"),          # devise inconnue
        _row("T5"),
    ])
    stats = ingest.load_partition(con, raw / "transactions/dt=2026-01-01/part.jsonl")
    assert stats["rows_in"] == 6
    assert stats["rows_loaded"] == 2
    assert stats["rows_rejected"] == 3
    assert stats["rows_duplicate"] == 1
    reasons = {r[0] for r in con.execute("SELECT reason FROM raw.transactions_rejected").fetchall()}
    assert reasons == {"montant invalide", "clé étrangère manquante", "devise inconnue"}


def test_idempotence(env):
    raw, con = env
    _write_partition(raw, "2026-01-01", [_row("T1"), _row("T2")])
    for p in ingest.pending_partitions(con, raw):
        ingest.load_partition(con, p)
    assert ingest.pending_partitions(con, raw) == []          # rien à recharger
    assert con.execute("SELECT count(*) FROM raw.transactions").fetchone()[0] == 2


def test_controle_qualite_sans_marts(env):
    raw, con = env
    _write_partition(raw, "2026-01-01", [_row("T1")])
    ingest.load_partition(con, raw / "transactions/dt=2026-01-01/part.jsonl")
    checks = {c.name: c for c in quality.run_checks(con)}
    assert checks["doublons_en_bronze"].passed
    assert not checks["reconciliation_bronze_vs_faits"].passed   # dbt pas encore lancé
