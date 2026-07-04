# Real-Time Fraud Detection Pipeline for Mobile Money Transactions

**Author:** Brayan Hawald Ngowi
**GitHub:** github.com/masterbry
**Status:** In Development

---

## 1. Project Overview

### 1.1 Problem Statement
Mobile money services (M-Pesa, Tigo Pesa, Airtel Money) process millions of transactions daily across East Africa. Fraudulent transactions — account takeovers, rapid cash-out chains, and unusual transfer patterns — need to be detected in real time, not hours later in a batch report.

This project builds an end-to-end data engineering pipeline that ingests a live stream of mobile money transactions, computes real-time fraud-risk features, validates data quality at every stage, and serves fraud scores through an API and live dashboard — while also maintaining a batch layer for historical analytics and model retraining.

### 1.2 Goals
- Demonstrate production-grade streaming data engineering (not just batch/ETL)
- Show orchestration, data quality, and observability practices used in real DE teams
- Build something directly relevant to the East African fintech ecosystem
- Produce a portfolio piece that maps cleanly onto Data Engineer / Platform Engineer job descriptions

### 1.3 Non-Goals
- This is not a fraud-detection ML research project. The model (Logistic Regression / Gradient Boosting) is intentionally simple — the focus is the **pipeline**, not model accuracy.

---

## 2. Architecture

```
                    ┌─────────────────────┐
                    │ Transaction Producer │  (Python + PaySim replay)
                    └──────────┬───────────┘
                               │ produces
                               ▼
                    ┌─────────────────────┐
                    │   Kafka (topic:      │
                    │   transactions.raw)  │
                    └──────────┬───────────┘
                               │ consumes
              ┌────────────────┴────────────────┐
              ▼                                  ▼
   ┌─────────────────────┐          ┌─────────────────────────┐
   │ Spark Structured     │          │  Raw sink (bronze layer)│
   │ Streaming            │          │  -> Postgres / Parquet  │
   │ (feature engineering,│          └─────────────────────────┘
   │  windowed velocity   │
   │  aggregates, fraud   │
   │  scoring)            │
   └──────────┬───────────┘
              │ writes
              ▼
   ┌─────────────────────┐
   │ Silver layer         │ <- Great Expectations validation
   │ (Postgres/Delta)     │
   └──────────┬───────────┘
              │
      ┌───────┴────────┐
      ▼                ▼
┌───────────┐   ┌─────────────────┐
│ dbt models │   │ Airflow DAGs     │
│ (gold      │   │ - daily agg      │
│  layer,    │   │ - model retrain  │
│  marts)    │   │ - GE validation  │
└─────┬──────┘   └─────────────────┘
      │
      ▼
┌─────────────┐        ┌───────────────────┐
│ FastAPI      │        │ Streamlit/Grafana  │
│ (fraud score │        │ (live monitoring   │
│  endpoint)   │        │  dashboard)        │
└─────────────┘        └───────────────────┘
```

### 2.1 Medallion Layers
| Layer | Storage | Purpose |
|---|---|---|
| **Bronze** | Postgres raw table / Parquet | Raw transactions exactly as received from Kafka, no transformation |
| **Silver** | Postgres validated table | Cleaned, deduplicated, schema-validated (Great Expectations), enriched with computed features |
| **Gold** | dbt models / marts | Business-ready aggregates: daily fraud rate by region, user risk scores, flagged transaction feed |

---

## 3. Tech Stack

| Component | Tool | Reason |
|---|---|---|
| Message broker | Apache Kafka | Industry-standard event streaming |
| Stream processing | Spark Structured Streaming | Windowed aggregations, stateful fraud features |
| Orchestration | Apache Airflow | Batch DAGs, retraining schedule, GE checks |
| Transformation | dbt | SQL-based, version-controlled gold layer |
| Data quality | Great Expectations | Automated validation between layers |
| Storage | PostgreSQL | Simple, free, sufficient for project scale |
| Serving | FastAPI | Lightweight scoring endpoint |
| Dashboard | Streamlit | Fast to build, good for live views |
| Containerization | Docker Compose | Full local reproducibility |
| ML | Scikit-learn | Simple fraud classifier (not the focus) |

---

## 4. Dataset

### 4.1 Primary Dataset: PaySim
PaySim is a synthetic dataset that simulates mobile money transactions based on a sample of real transactions extracted from one month of financial logs from a mobile money service implemented in an African country. It contains roughly 6.3 million transactions across a 30-day simulated period, with fraud making up only about 0.13% of records — a realistic imbalance for fraud-detection work.

**Where to get it:**
- Kaggle: search **"PaySim1"** or go to `kaggle.com/datasets/ealaxi/paysim1` (requires free Kaggle account + `kaggle.json` API token)
- GitHub mirror (no account needed): `github.com/BBQtime/Synthetic-Financial-Datasets-For-Fraud-Detection`

**Columns:**
| Column | Description |
|---|---|
| step | Time unit (1 step = 1 hour, 744 steps = 30 days) |
| type | CASH-IN, CASH-OUT, DEBIT, PAYMENT, TRANSFER |
| amount | Transaction amount (local currency) |
| nameOrig | Sender account ID |
| oldbalanceOrg / newbalanceOrig | Sender balance before/after |
| nameDest | Receiver account ID |
| oldbalanceDest / newbalanceDest | Receiver balance before/after |
| isFraud | Ground truth fraud label |
| isFlaggedFraud | Flagged by the simple business rule (transfer > 200,000) |

### 4.2 How to download (Kaggle CLI)
```bash
pip install kaggle --break-system-packages
# Place kaggle.json (from kaggle.com/settings) in ~/.kaggle/
kaggle datasets download -d ealaxi/paysim1
unzip paysim1.zip -d data/raw/
```

### 4.3 Using PaySim for streaming simulation
PaySim is a static CSV, so to simulate a **live** stream:
1. Sort by the `step` column (already chronological)
2. Write a Python producer that reads rows sequentially and publishes each to Kafka
3. Add a small `time.sleep()` between rows (or compress time — e.g., 1 step = 1 second instead of 1 hour) so the pipeline behaves like a real feed
4. Optionally inject additional synthetic fraud patterns (rapid-fire transfers, geo-anomalies) on top of PaySim's existing labels, to make your feature engineering layer do real work

### 4.4 Supplementary/alternative datasets
- **IEEE-CIS Fraud Detection** (Kaggle) — real e-commerce fraud data, more features, good if you want a second dataset for model comparison
- **Synthetic Financial Payment System dataset** (Kaggle, via Synthesized.io) — ~594K transactions, 4,112 users, alternative schema if you want variety

---

## 5. Build Roadmap

- [ ] **Phase 1 — Foundation**: Docker Compose (Kafka, Zookeeper, Postgres, Airflow); repo structure
- [ ] **Phase 2 — Producer**: PaySim replay script publishing to `transactions.raw` topic
- [ ] **Phase 3 — Streaming**: Spark Structured Streaming job computing rolling features (transaction velocity, amount z-score per user) and writing to Silver
- [ ] **Phase 4 — Data quality**: Great Expectations suite validating Bronze → Silver transition
- [ ] **Phase 5 — Transformation**: dbt models building Gold layer marts (daily fraud rate, risk scores)
- [ ] **Phase 6 — Orchestration**: Airflow DAGs for batch aggregation + scheduled model retraining
- [ ] **Phase 7 — Serving**: FastAPI endpoint returning fraud score for a transaction ID
- [ ] **Phase 8 — Monitoring**: Streamlit dashboard showing live transaction flow + flagged fraud feed
- [ ] **Phase 9 — Deploy**: Docker deployment to a free-tier VM; README + architecture diagram polish

---

## 6. Repository Structure (proposed)
```
fraud-detection-pipeline/
├── docker-compose.yml
├── producer/
│   └── paysim_replay.py
├── streaming/
│   └── spark_fraud_features.py
├── dags/
│   ├── daily_aggregation_dag.py
│   └── model_retrain_dag.py
├── dbt_project/
│   └── models/
│       ├── staging/
│       └── marts/
├── great_expectations/
│   └── expectations/
├── api/
│   └── main.py
├── dashboard/
│   └── app.py
├── models/
│   └── fraud_classifier.pkl
├── data/
│   └── raw/  (PaySim CSV, gitignored)
└── docs/
    └── PROJECT_DOCUMENTATION.md
```

---

## 7. Resume/Portfolio Framing
> Built a real-time fraud detection pipeline processing simulated mobile-money transactions using Kafka and Spark Structured Streaming, with Airflow-orchestrated batch retraining, dbt-modeled analytics layer, and automated data quality validation via Great Expectations — deployed via Docker.

---

## 8. References
- PaySim paper: Lopez-Rojas, E. A., Elmir, A., & Axelsson, S. (2016). *PaySim: A financial mobile money simulator for fraud detection.* 28th European Modeling and Simulation Symposium, Larnaca, Cyprus.
- Kaggle dataset: kaggle.com/datasets/ealaxi/paysim1
