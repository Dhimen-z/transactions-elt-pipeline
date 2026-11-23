"""Contrôles qualité post-chargement et rapport de run (data/quality_report.json).

Complète les tests dbt avec des contrôles « métier » : volumétrie par rapport
à l'historique, taux de rejet, fraîcheur et réconciliation bronze -> faits.
Code de sortie non nul si un contrôle bloquant échoue (utilisable en CI / Airflow).
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

DB = Path("data/warehouse.duckdb")
REPORT = Path("data/quality_report.json")


@dataclass
class Check:
    name: str
    passed: bool
    value: float
    threshold: str
    blocking: bool = True


def run_checks(con) -> list[Check]:
    checks: list[Check] = []

    rej = con.execute("SELECT sum(rows_rejected) / nullif(sum(rows_in), 0) FROM meta.load_log").fetchone()[0] or 0
    checks.append(Check("taux_de_rejet_global", rej <= 0.02, round(float(rej), 5), "<= 2 %"))

    # volumétrie du dernier jour vs médiane des 7 jours précédents
    vol = con.execute("""
        WITH d AS (SELECT partition, rows_loaded FROM meta.load_log ORDER BY partition DESC LIMIT 8)
        SELECT (SELECT rows_loaded FROM d ORDER BY partition DESC LIMIT 1)
               / nullif((SELECT median(rows_loaded) FROM (SELECT * FROM d ORDER BY partition DESC OFFSET 1)), 0)
    """).fetchone()[0]
    vol = float(vol) if vol is not None else 1.0
    checks.append(Check("volumetrie_dernier_jour_vs_mediane_7j", 0.5 <= vol <= 2.0, round(vol, 3), "entre 0,5 et 2"))

    # réconciliation : tout ce qui est en bronze doit être dans la table de faits
    try:
        gap = con.execute("""SELECT (SELECT count(*) FROM raw.transactions)
                                  - (SELECT count(*) FROM marts.fct_transactions)""").fetchone()[0]
        checks.append(Check("reconciliation_bronze_vs_faits", gap == 0, float(gap), "= 0"))
    except duckdb.CatalogException:
        checks.append(Check("reconciliation_bronze_vs_faits", False, -1, "marts absent : lancer dbt"))

    dup = con.execute("""SELECT count(*) - count(DISTINCT transaction_id) FROM raw.transactions""").fetchone()[0]
    checks.append(Check("doublons_en_bronze", dup == 0, float(dup), "= 0"))

    fraud = con.execute("SELECT avg(CASE WHEN is_fraud THEN 1.0 ELSE 0 END) FROM raw.transactions").fetchone()[0] or 0
    checks.append(Check("taux_fraude_plausible", float(fraud) < 0.05, round(float(fraud), 5), "< 5 %", blocking=False))
    return checks


def main() -> int:
    con = duckdb.connect(str(DB), read_only=True)
    checks = run_checks(con)
    con.close()
    REPORT.write_text(json.dumps([asdict(c) for c in checks], indent=2, ensure_ascii=False), encoding="utf-8")
    for c in checks:
        flag = "OK  " if c.passed else ("FAIL" if c.blocking else "WARN")
        print(f"[{flag}] {c.name:<42} valeur={c.value:<10} attendu {c.threshold}")
    failed = [c for c in checks if c.blocking and not c.passed]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
