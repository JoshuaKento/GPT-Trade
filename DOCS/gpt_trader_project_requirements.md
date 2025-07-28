---
title: GPT Trader – Project Specs (Concise)
created: 2025-07-28
tags: ["#gpt_trader", "#spec"]
---

# 1. Purpose
Automate collection, storage, and analysis of corporate filings, market data, and social signals to generate investor‑ready reports and actionable trading signals.

# 2. Functional Requirements
1. **ETL Pipelines** – Ingest SEC EDGAR (10‑K/10‑Q/8‑K), EDINET, market price APIs, and curated SNS/news; store raw files in S3 and metadata in Postgres.
2. **Vector & Feature Store** – Embed cleaned text (pgvector) and tabular features (Feast).
3. **Retrieval‑Augmented Generation** – Query, summarise, and benchmark using GPT‑4o / o3 with RAG.
4. **Backtesting & Live Trading** – Generate signals; test with Backtrader; optional live execution via broker REST API.
5. **Reporting** – Output PDF/HTML dashboards covering fundamentals, sentiment, and signal performance.

# 3. Non‑Functional Requirements
- **Daily ETL SLA:** ≤ 30 min for 50 tickers.
- **Cost Cap:** < $500/month cloud + LLM.
- **Compliance:** SEC usage limits, GDPR; anonymise sensitive data.
- **Observability:** Structured logs, Prometheus‑based metrics, alerting.

# 4. Technology Stack
| Layer | Primary Tool |
| --- | --- |
| Orchestration | Docker Compose → Airflow |
| Data Lake | S3 / MinIO |
| Relational | Postgres 16 |
| Vector DB | pgvector (→ Milvus as scale grows) |
| Feature Store | Feast |
| LLM Orchestration | LangChain / LlamaIndex |
| Backtesting | Backtrader / VectorBT |
| CI/CD | GitHub Actions, pre‑commit |

# 5. Milestones
| Month | Goal |
| --- | --- |
| 1 | PoC – single‑ticker ETL + notebook backtest |
| 3 | MVP – 50 tickers, RAG reports, Streamlit UI |
| 6 | Beta – role‑based API & governance, live trading loop |

# 6. Out‑of‑Scope (v1)
- Options data
- High‑frequency intraday execution
- Mobile app GUI

