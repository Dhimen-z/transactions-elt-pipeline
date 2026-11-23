"""Point d'entrée unique : python -m pipeline.run [generate|ingest|transform|quality|all]."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from pipeline import generate, ingest, quality

ROOT = Path(__file__).resolve().parents[1]


def transform() -> None:
    dbt = ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
    env = {**os.environ, "WAREHOUSE_PATH": str(ROOT / "data" / "warehouse.duckdb")}
    subprocess.run(dbt, cwd=ROOT, check=True, env=env)


def main() -> int:
    p = argparse.ArgumentParser(description="Pipeline ELT transactions")
    p.add_argument("step", choices=["generate", "ingest", "transform", "quality", "all"])
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--days", type=int, default=30)
    a = p.parse_args()

    if a.step in ("generate", "all"):
        generate.main(a.start, a.days, 4000, 42)
    if a.step in ("ingest", "all"):
        ingest.main()
    if a.step in ("transform", "all"):
        transform()
    if a.step in ("quality", "all"):
        return quality.main()
    return 0


if __name__ == "__main__":
    sys.exit(main())
