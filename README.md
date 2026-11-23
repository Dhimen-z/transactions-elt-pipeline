# Pipeline ELT de transactions bancaires — DuckDB · dbt · Airflow

Pipeline de données de bout en bout qui transforme des fichiers bruts de paiements en un **modèle en étoile prêt pour la BI**, avec ingestion incrémentale, quarantaine des lignes invalides, tests dbt et contrôles qualité bloquants.

Le contexte (flux de transactions carte, détection de fraude) reprend le domaine sur lequel j'ai travaillé en stage chez un éditeur de logiciels bancaires. Les données sont **synthétiques** et générées localement : le projet tourne sans compte cloud, en une commande.

```
fichiers bruts (CSV / JSONL partitionnés par jour)
        │  ingest.py  — incrémental, idempotent, validation à l'entrée
        ▼
  raw (bronze)  ──►  raw.transactions_rejected  (quarantaine + motif)
        │  dbt build — staging (vues) → marts (tables, faits incrémentaux) + 31 tests
        ▼
  marts : fct_transactions · dim_customer · dim_merchant · dim_date · agg_daily_merchant
        │  quality.py — volumétrie, taux de rejet, réconciliation, doublons
        ▼
  data/quality_report.json  (code retour ≠ 0 si un contrôle bloquant échoue)
```

## Démarrage rapide

```bash
pip install -r requirements.txt
python -m pipeline.run all --days 30     # génère 30 jours, ingère, dbt build, contrôles
python -m pytest -q                      # tests unitaires de l'ingestion
```

Sortie type :

```
2026-01-31  lus=  5025  chargés=  5000  rejetés=  19  doublons=  6
...
Done. PASS=31 WARN=0 ERROR=0
[OK  ] taux_de_rejet_global                   valeur=0.00382  attendu <= 2 %
[OK  ] volumetrie_dernier_jour_vs_mediane_7j  valeur=1.0      attendu entre 0,5 et 2
[OK  ] reconciliation_bronze_vs_faits         valeur=0.0      attendu = 0
```

Ou avec Docker : `docker build -t elt . && docker run --rm elt`

## Ce que montre le projet

| Sujet | Mise en œuvre |
|---|---|
| **Chargement incrémental** | Journal `meta.load_log` par partition `dt=YYYY-MM-DD` ; relancer l'ingestion ne recharge rien et ne crée aucun doublon. Côté dbt, `fct_transactions` est un modèle `incremental` (`delete+insert` sur `transaction_id`, filtré sur `_loaded_at`). |
| **Qualité à l'entrée** | Règles SQL de rejet (clé manquante, montant ≤ 0, devise inconnue, horodatage invalide). Les lignes rejetées vont en quarantaine avec leur motif et le payload JSON, au lieu de faire échouer le run. |
| **Dédoublonnage** | `QUALIFY row_number()` dans le lot + `ANTI JOIN` sur l'existant. |
| **Transactions** | Chaque partition est chargée dans une transaction : en cas d'erreur, `ROLLBACK` et la partition reste « à charger ». |
| **Modélisation** | Schéma en étoile (1 table de faits au grain transaction, 3 dimensions dont un calendrier) + un agrégat journalier par catégorie de marchand pour le reporting. |
| **Tests** | 31 tests dbt (unicité, non-nullité, intégrité référentielle, valeurs acceptées, test singulier sur les montants) + `source freshness` + tests `pytest`. |
| **Observabilité** | Rapport JSON des contrôles qualité ; contrôle de volumétrie par rapport à la médiane des 7 jours précédents. |
| **Orchestration** | DAG Airflow `dags/transactions_dag.py` : `ingest_bronze → dbt_build → quality_checks`, retries, planification quotidienne. |
| **CI** | GitHub Actions : tests + exécution complète du pipeline à chaque push. |

## Structure

```
pipeline/
  generate.py   données synthétiques (anomalies injectées volontairement)
  ingest.py     bronze incrémental + quarantaine
  quality.py    contrôles post-chargement, rapport JSON
  run.py        CLI : generate | ingest | transform | quality | all
dbt/
  models/staging/   vues de nettoyage + sources (freshness)
  models/marts/     faits, dimensions, agrégats + tests
  tests/            tests singuliers
dags/               DAG Airflow
tests/              pytest
.github/workflows/  CI
```

## Interroger l'entrepôt

```python
import duckdb
con = duckdb.connect("data/warehouse.duckdb", read_only=True)
con.sql("""
  select category, sum(volume_approuve) as volume, avg(taux_acceptation) as acceptation,
         sum(nb_fraudes) as fraudes
  from marts.agg_daily_merchant group by 1 order by 2 desc
""").show()
```

Les tables `marts.*` peuvent être branchées directement sur Power BI, Metabase ou Superset (connecteur DuckDB / export Parquet).

## Passer à l'échelle

DuckDB a été choisi pour que le projet tourne partout sans infrastructure. Le même découpage se transpose tel quel sur un entrepôt cloud : changer l'adaptateur dbt (`dbt-snowflake`, `dbt-bigquery`, `dbt-postgres`), remplacer la lecture JSONL locale par un stockage objet (S3/ADLS) et confier l'ingestion à l'orchestrateur.

## Projet lié

L'analyse et le tableau de bord construits sur ce type de données : [payments-kpi-analytics](https://github.com/Dhimen-z/payments-kpi-analytics).

## Auteur

Zidane DHIMEN — élève ingénieur ISIMA (IA & optimisation), Master ICS. [LinkedIn](https://www.linkedin.com/in/zidane-dhimen-bb7416373)
