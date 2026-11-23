"""Ingestion incrémentale et idempotente des fichiers bruts vers DuckDB (couche bronze).

- Référentiels : full refresh.
- Transactions : une partition dt=YYYY-MM-DD n'est chargée qu'une fois
  (journal meta.load_log). Relancer le script ne crée aucun doublon.
- Validation à l'entrée : les lignes invalides partent en quarantaine
  (raw.transactions_rejected) avec le motif du rejet, au lieu de casser le run.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

RAW = Path("data/raw")
DB = Path("data/warehouse.duckdb")

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id VARCHAR PRIMARY KEY,
    event_ts TIMESTAMP, customer_id VARCHAR, merchant_id VARCHAR,
    amount DECIMAL(12,2), currency VARCHAR, channel VARCHAR, status VARCHAR,
    is_fraud BOOLEAN, _partition DATE, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw.transactions_rejected (
    transaction_id VARCHAR, payload JSON, reason VARCHAR,
    _partition DATE, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS meta.load_log (
    partition DATE PRIMARY KEY, rows_in INTEGER, rows_loaded INTEGER,
    rows_rejected INTEGER, rows_duplicate INTEGER, loaded_at TIMESTAMP
);
"""

# Règle de rejet -> motif. L'ordre compte : le premier motif rencontré est retenu.
RULES = """
CASE
  WHEN transaction_id IS NULL                       THEN 'transaction_id manquant'
  WHEN customer_id IS NULL OR merchant_id IS NULL   THEN 'clé étrangère manquante'
  WHEN amount IS NULL OR amount <= 0                THEN 'montant invalide'
  WHEN currency NOT IN ('EUR')                      THEN 'devise inconnue'
  WHEN TRY_CAST(event_ts AS TIMESTAMP) IS NULL      THEN 'horodatage invalide'
END
"""


def connect(db: Path = DB) -> duckdb.DuckDBPyConnection:
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db))
    con.execute(DDL)
    return con


def load_referentials(con, raw: Path = RAW) -> None:
    for name in ("customers", "merchants"):
        con.execute(f"CREATE OR REPLACE TABLE raw.{name} AS "
                    f"SELECT *, now() AS _loaded_at FROM read_csv_auto(?, header=true, all_varchar=true)",
                    [str(raw / f"{name}.csv")])


def pending_partitions(con, raw: Path = RAW) -> list[Path]:
    done = {str(r[0]) for r in con.execute("SELECT partition FROM meta.load_log").fetchall()}
    parts = sorted((raw / "transactions").glob("dt=*/part.jsonl"))
    return [p for p in parts if p.parent.name[3:] not in done]


def load_partition(con, path: Path) -> dict:
    part = path.parent.name[3:]
    now = datetime.now()
    con.execute("BEGIN")
    try:
        con.execute("""CREATE OR REPLACE TEMP TABLE batch AS
                       SELECT * FROM read_json_auto(?, format='newline_delimited',
                              columns={transaction_id:'VARCHAR', event_ts:'VARCHAR', customer_id:'VARCHAR',
                                       merchant_id:'VARCHAR', amount:'DOUBLE', currency:'VARCHAR',
                                       channel:'VARCHAR', status:'VARCHAR', is_fraud:'BOOLEAN'})""",
                    [str(path)])
        con.execute(f"CREATE OR REPLACE TEMP TABLE checked AS SELECT *, {RULES} AS reason FROM batch")
        rows_in = con.execute("SELECT count(*) FROM batch").fetchone()[0]

        con.execute("""INSERT INTO raw.transactions_rejected
                       SELECT transaction_id, to_json(checked), reason, ?::DATE, ? FROM checked
                       WHERE reason IS NOT NULL""", [part, now])
        rejected = con.execute("SELECT count(*) FROM checked WHERE reason IS NOT NULL").fetchone()[0]

        # dédoublonnage intra-lot (QUALIFY) + anti-jointure sur ce qui est déjà chargé
        con.execute("""CREATE OR REPLACE TEMP TABLE valid AS
                       SELECT * FROM checked WHERE reason IS NULL
                       QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY event_ts) = 1""")
        con.execute("""INSERT INTO raw.transactions
                       SELECT v.transaction_id, v.event_ts::TIMESTAMP, v.customer_id, v.merchant_id,
                              v.amount::DECIMAL(12,2), v.currency, v.channel, v.status, v.is_fraud, ?::DATE, ?
                       FROM valid v ANTI JOIN raw.transactions t USING (transaction_id)""", [part, now])
        loaded = con.execute("SELECT count(*) FROM raw.transactions WHERE _partition = ?", [part]).fetchone()[0]
        stats = dict(partition=part, rows_in=rows_in, rows_loaded=loaded, rows_rejected=rejected,
                     rows_duplicate=rows_in - rejected - loaded)
        con.execute("INSERT INTO meta.load_log VALUES (?, ?, ?, ?, ?, ?)",
                    [part, rows_in, loaded, rejected, stats["rows_duplicate"], now])
        con.execute("COMMIT")
        return stats
    except Exception:
        con.execute("ROLLBACK")
        raise


def main() -> None:
    con = connect()
    load_referentials(con)
    todo = pending_partitions(con)
    if not todo:
        print("Rien à charger : toutes les partitions sont déjà ingérées.")
    for p in todo:
        s = load_partition(con, p)
        print(f"{s['partition']}  lus={s['rows_in']:>6}  chargés={s['rows_loaded']:>6}  "
              f"rejetés={s['rows_rejected']:>4}  doublons={s['rows_duplicate']:>3}")
    con.close()


if __name__ == "__main__":
    main()
