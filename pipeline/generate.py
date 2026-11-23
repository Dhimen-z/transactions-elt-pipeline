"""Génère des données brutes synthétiques façon système de paiement.

Sorties (dans data/raw/) :
  customers.csv, merchants.csv          -> référentiels (full refresh)
  transactions/dt=YYYY-MM-DD/part.jsonl -> un fichier par jour (chargement incrémental)

Des anomalies sont injectées volontairement (doublons, montants négatifs,
champs manquants, devises inconnues) pour exercer la couche qualité.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

RAW = Path("data/raw")
MCC = {"5411": "Alimentation", "5812": "Restaurants", "5541": "Carburant",
       "5732": "Électronique", "4111": "Transport", "5999": "Divers", "7995": "Jeux"}
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Clermont-Ferrand", "Lille", "Nantes", "Perpignan"]
CHANNELS = ["POS", "ECOM", "CONTACTLESS", "ATM"]


def make_referentials(rng: np.random.Generator, n_customers: int, n_merchants: int):
    customers = pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(n_customers)],
        "segment": rng.choice(["particulier", "premium", "pro"], n_customers, p=[.75, .15, .10]),
        "city": rng.choice(CITIES, n_customers),
        "created_at": [str(date(2022, 1, 1) + timedelta(days=int(d))) for d in rng.integers(0, 1000, n_customers)],
    })
    merchants = pd.DataFrame({
        "merchant_id": [f"M{i:05d}" for i in range(n_merchants)],
        "mcc": rng.choice(list(MCC), n_merchants),
        "city": rng.choice(CITIES, n_merchants),
    })
    merchants["category"] = merchants["mcc"].map(MCC)
    return customers, merchants


def make_day(rng: np.random.Generator, day: date, customers, merchants, n: int) -> list[dict]:
    cust = rng.choice(customers["customer_id"].to_numpy(), n)
    merch = rng.choice(merchants["merchant_id"].to_numpy(), n)
    secs = np.sort(rng.integers(0, 86_400, n))
    amounts = np.round(rng.lognormal(3.3, 1.0, n), 2)
    fraud = rng.random(n) < 0.004
    amounts[fraud] *= rng.uniform(3, 15, fraud.sum())
    rows = []
    for i in range(n):
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=int(secs[i]))
        rows.append({
            "transaction_id": f"T{day:%Y%m%d}{i:07d}",
            "event_ts": ts.isoformat(),
            "customer_id": str(cust[i]),
            "merchant_id": str(merch[i]),
            "amount": float(amounts[i]),
            "currency": "EUR",
            "channel": str(rng.choice(CHANNELS, p=[.45, .25, .25, .05])),
            "status": "DECLINED" if rng.random() < 0.03 else "APPROVED",
            "is_fraud": bool(fraud[i]),
        })
    # anomalies volontaires (~0,5 %)
    for _ in range(max(1, n // 200)):
        r = dict(rows[rng.integers(0, n)])
        kind = rng.integers(0, 4)
        if kind == 0:
            rows.append(r)                       # doublon exact
        elif kind == 1:
            r["amount"] = -abs(r["amount"])     # montant négatif
            r["transaction_id"] += "N"
            rows.append(r)
        elif kind == 2:
            r["customer_id"] = None             # clé manquante
            r["transaction_id"] += "K"
            rows.append(r)
        else:
            r["currency"] = "XXX"               # devise inconnue
            r["transaction_id"] += "C"
            rows.append(r)
    random.Random(int(day.strftime("%Y%m%d"))).shuffle(rows)
    return rows


def main(start: str, days: int, per_day: int, seed: int) -> None:
    rng = np.random.default_rng(seed)
    RAW.mkdir(parents=True, exist_ok=True)
    customers, merchants = make_referentials(rng, 5_000, 800)
    customers.to_csv(RAW / "customers.csv", index=False)
    merchants.to_csv(RAW / "merchants.csv", index=False)
    d0 = date.fromisoformat(start)
    for k in range(days):
        day = d0 + timedelta(days=k)
        out = RAW / "transactions" / f"dt={day}"
        out.mkdir(parents=True, exist_ok=True)
        n = int(per_day * (1.25 if day.weekday() >= 5 else 1.0))
        with open(out / "part.jsonl", "w", encoding="utf-8") as f:
            for row in make_day(rng, day, customers, merchants, n):
                f.write(json.dumps(row) + "\n")
    print(f"{days} jour(s) générés à partir du {start} dans {RAW}/")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--per-day", type=int, default=4000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    main(a.start, a.days, a.per_day, a.seed)
