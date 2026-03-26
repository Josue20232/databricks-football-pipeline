# 🏟️ Databricks Football Pipeline

End-to-end PySpark data pipeline on Databricks implementing the **Medallion Architecture** (Raw → Bronze → Silver → Gold) using Bundesliga football match data.

## 🏗️ Architecture
```
Raw (CSV files)
  → Bronze (Delta Lake ingestion)
    → Silver (cleaning & transformation)
      → Gold (aggregations & analytics)
```

## 📓 Notebooks

| Notebook | Description |
|----------|-------------|
| `01_Raw_To_Bronze_Transformations` | Ingest CSV files into Delta tables |
| `02_Bronze_To_Silver_Transformations` | Clean, type cast and rename columns |
| `03_Silver_To_Gold_Transformations` | Aggregate stats, compute rankings |
| `04_Exploratory_Data_Analysis` | Champions, titles, best attacks per season |

## 🛠️ Tech Stack

- **Apache Spark / PySpark**
- **Databricks** (Serverless)
- **Delta Lake**
- **Unity Catalog**
- **Databricks Jobs** (orchestration & scheduling)

## 📊 Dataset

Bundesliga football matches from 1993 to 2016 — 7,650 rows after filtering Division 1.

## 👤 Author

LUABA Josué — MSc Data Engineering, Aivancity 2025-2026
