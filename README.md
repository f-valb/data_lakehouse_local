# 🏔️ Local Lakehouse Demo — Medallion Architecture

A fully local lakehouse that streams fake e-commerce events through the **Bronze → Silver → Gold** medallion layers using Kafka, Spark Structured Streaming, Delta Lake on MinIO, and Trino for SQL querying.

## Stack

| Component | Purpose | URL |
|-----------|---------|-----|
| MinIO | S3-compatible object storage | http://localhost:9001 (minioadmin/minioadmin) |
| Kafka | Event streaming | localhost:9092 |
| Spark (stream) | Kafka → Bronze Delta tables | — |
| Spark (batch) | Bronze → Silver → Gold (every 60s) | — |
| Trino | SQL over Delta tables (3 catalogs) | http://localhost:8080 |
| Jupyter | PySpark exploration + ML | http://localhost:8888 |
| Grafana | Dashboards over Trino | http://localhost:3000 (admin/admin) |

## Quick Start

```bash
docker compose up --build -d
```

Wait ~2 minutes for all services to initialise, then:

```bash
# Query bronze layer
docker compose exec trino trino --execute "SELECT count(*) FROM bronze.default.orders LIMIT 1"

# Query gold layer
docker compose exec trino trino --execute "SELECT * FROM gold.default.daily_revenue LIMIT 10"
```

Open Jupyter at http://localhost:8888 and run the notebooks in `work/`.

## Medallion Layers

### 🥉 Bronze
- Raw events written by Spark Structured Streaming directly from Kafka
- Tables: `bronze.default.orders`, `bronze.default.clicks`
- No transformations — exact Kafka payload preserved

### 🥈 Silver
- Cleaned, deduplicated, validated, typed
- Tables: `silver.default.orders`, `silver.default.clicks`
- Runs every 60 seconds as a batch job

### 🥇 Gold
- Business-level aggregations
- Tables: `gold.default.daily_revenue`, `gold.default.product_metrics`, `gold.default.user_metrics`, `gold.default.funnel`

## Trino Catalogs

Each layer is a separate Trino catalog backed by the Delta Lake connector pointing to a different MinIO bucket:

```sql
-- any layer is directly queryable
SELECT * FROM bronze.default.orders LIMIT 5;
SELECT * FROM silver.default.orders LIMIT 5;
SELECT * FROM gold.default.daily_revenue LIMIT 5;
```

## Time Travel (Delta Lake)

```sql
-- Trino Delta time travel
SELECT * FROM bronze.default.orders FOR VERSION AS OF 0;
```

Or from PySpark in Jupyter:

```python
df = spark.read.format("delta").option("versionAsOf", 0).load("s3a://bronze/orders")
```

## Notebooks

| Notebook | Description |
|----------|-------------|
| `01_explore.ipynb` | Delta table exploration, schema inspection, time travel |
| `02_ml_training.ipynb` | Feature engineering from gold.user_metrics, purchase propensity model |

## Architecture Diagram

```
Event Generator
      │  orders / clicks (~2/s)
      ▼
   Kafka
      │
      ▼  Spark Structured Streaming
  🥉 Bronze (MinIO s3a://bronze/)
      │
      ▼  Spark Batch (every 60s)
  🥈 Silver (MinIO s3a://silver/)
      │
      ▼  Spark Batch (every 60s)
  🥇 Gold  (MinIO s3a://gold/)
      │
      ▼
   Trino (bronze / silver / gold catalogs)
   ├── Jupyter (exploration + ML)
   └── Grafana (dashboards)
```
