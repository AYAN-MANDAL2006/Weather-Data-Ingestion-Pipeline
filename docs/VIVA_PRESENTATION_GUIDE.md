# College Presentation & Viva Voce Guide

## Project: Automated Weather Data Ingestion & Analytics Pipeline

This guide is prepared to help you confidently present your project to professors, external examiners, and interviewers. It provides clear, high-scoring answers to the most common questions.

---

### 1. 60-Second Elevator Pitch
> *"Our project is an automated, end-to-end data engineering pipeline that ingests, validates, stores, and analyzes real-time and historical weather observations for Faridabad, Haryana, India. It leverages the modern data stack: Open-Meteo REST API for live weather data, Pydantic for data validation, dlt (data load tool) for declarative ingestion and deduplication, DuckDB for high-performance in-process columnar SQL analytics, and Dagster for software-defined asset orchestration and automated hourly scheduling. The system is resilient to network failures, handles schema evolution automatically, and guarantees idempotent data loading."*

---

### 2. Tech Stack Justification: "Why Did You Choose X?"

| Technology | Role | Why We Chose It (Viva Talking Point) |
|:---|:---|:---|
| **Open-Meteo REST API** | Data Source | Free, open-access, and requires no API keys for educational use. It provides high-accuracy, 15-minute to hourly meteorological forecasts and observations worldwide. |
| **Pydantic** | Validation | Guarantees data integrity before insertion. Catches corrupted or unrealistic physical readings (e.g., negative wind speeds or extreme temperatures) at the application boundary. |
| **dlt (data load tool)** | ELT Ingestion | A lightweight, modern Python library that eliminates boilerplate data loading code. Automatically infers schemas, creates tables, and handles upserts/merges without manual schema management. |
| **DuckDB** | Storage & OLAP | Known as the "SQLite for Analytics". It is an embedded, columnar database running directly in Python memory and local disk. It executes analytical queries (aggregations, window functions) 10x-100x faster than traditional row-oriented databases like SQLite without needing an external server like PostgreSQL. |
| **SQL** | Analytics | Industry-standard query language. We use advanced window functions (`AVG() OVER`, `LAG()`) to compute 6-hour moving averages and identify warming vs cooling trends. |
| **Dagster** | Orchestration | Modern orchestrator centered on **Software-Defined Assets (SDAs)**. Unlike older task-based engines (Airflow), Dagster focuses on what data should exist, providing built-in data lineage, asset materialization metadata, and clean local testing. |

---

### 3. Top 10 Viva & Interview Questions with Model Answers

#### Q1: What is idempotency, and how does your pipeline achieve it?
**Answer:**
> *An idempotent pipeline produces the exact same result in the database regardless of whether it is run once or ten times with the same input. In our pipeline, we achieve this through `dlt` with `write_disposition="merge"` on a composite primary key consisting of `(location, timestamp)`. If the pipeline runs every hour, re-reading the last 2 days of data will not duplicate rows; existing rows are updated in-place and only new hourly slots are inserted.*

#### Q2: What is the difference between OLTP and OLAP, and why did you use DuckDB?
**Answer:**
> *OLTP (Online Transaction Processing) like PostgreSQL or SQLite stores data row-by-row and is optimized for frequent single-row inserts and updates. OLAP (Online Analytical Processing) like DuckDB or Snowflake stores data column-by-column. Because our analytical queries compute metrics like `AVG(temperature_c)` across thousands of observations, DuckDB only reads the temperature column into CPU cache using vectorized SIMD instructions, executing much faster and with smaller storage overhead.*

#### Q3: What happens if the internet goes down or the API returns an error?
**Answer:**
> *We implemented fault tolerance using `urllib3.util.Retry` mounted onto a persistent `requests.Session`. If Open-Meteo returns HTTP 429 (rate limited) or transient server errors (500, 502, 503, 504), the client automatically retries up to 3 times with exponential backoff. If the connection times out completely, our custom `WeatherAPIError` is raised, logged with context, and gracefully handled without crashing unhandled.*

#### Q4: What is the difference between Airflow and Dagster?
**Answer:**
> *Apache Airflow is a **task-based** orchestrator (Task A runs, then Task B runs). Dagster is an **asset-based** orchestrator: it models the data itself (the assets) and their dependencies. Dagster tracks which data assets were generated, what metadata they hold (e.g., record counts, latest temperature), and allows direct unit testing of assets in Python without running a distributed cluster.*

#### Q5: How do your SQL queries calculate trends?
**Answer:**
> *In `historical_trends.sql`, we use two key SQL window functions:*
> 1. `AVG(temperature_c) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)` *to compute a 6-hour rolling average that smooths out short-term fluctuations.*
> 2. `LAG(temperature_c, 1) OVER (ORDER BY timestamp)` *to compare the current temperature to the previous hour's reading, categorizing the weather as 'Warming (+)', 'Cooling (-)', or 'Stable (=)'.*

#### Q6: How does the pipeline handle missing values or sensor dropouts?
**Answer:**
> *Open-Meteo occasionally returns `None` for certain hourly slots if a sensor reading is unavailable. In `client.py`, we check if core metrics are present. If a slot is completely empty, it is skipped; if only partial metrics are present (e.g., temperature is recorded but humidity is null), the record is safely ingested with `NULL` for missing columns, and SQL aggregate functions (`AVG`, `MIN`, `MAX`) automatically ignore null values without distorting results.*

#### Q7: What schedule is configured in Dagster?
**Answer:**
> *We configured `hourly_weather_schedule` using the standard cron expression `0 * * * *` set to the `Asia/Kolkata` timezone. This triggers the ingestion job at minute 0 of every hour, keeping the DuckDB dataset continually up to date.*

#### Q8: Where is the data stored? Is it permanent?
**Answer:**
> *The data is permanently stored on disk at `data/weather_data.duckdb`. DuckDB writes WAL (Write-Ahead Logging) and atomic commits to this single file, ensuring ACID compliance and persistence across reboots.*

#### Q9: How did you test the project?
**Answer:**
> *We built an automated test suite with 18 comprehensive tests using `pytest`:*
> - *Unit tests with mocked API responses (simulating timeouts, 500 errors, invalid JSON).*
> - *Live API integration test verifying connectivity to Open-Meteo.*
> - *Pipeline tests verifying table creation, schema types, and deduplication.*
> - *SQL query execution tests verifying window functions and aggregates.*
> - *Dagster tests verifying asset definitions, schedules, and in-process execution.*

#### Q10: How can this pipeline scale in a production cloud environment?
**Answer:**
> *Because our architecture is decoupled, scaling is straightforward:*
> 1. *Change the dlt destination from DuckDB to Snowflake, BigQuery, or Amazon Redshift simply by changing the connection string in configuration.*
> 2. *Deploy Dagster to Kubernetes or Dagster Cloud using Docker containers.*
> 3. *Expand the API client to loop over hundreds of coordinates or cities concurrently.*

---

### 4. Key Metrics for Faridabad, Haryana, India
- **Coordinates**: Latitude `28.4089° N`, Longitude `77.3178° E`
- **Climate Context**: Semi-arid/subtropical with pronounced seasonal variations (monsoon, dry winters, scorching summers).
- **Timezone**: `Asia/Kolkata` (UTC+05:30)
