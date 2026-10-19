# Weather Data Ingestion & Analytics Pipeline

An automated, enterprise-grade data engineering pipeline that fetches real weather data for **Faridabad, Haryana, India**, validates and transforms records, loads them into persistent **DuckDB** storage using **dlt** with duplicate handling, computes **SQL analytics**, and orchestrates workflows with **Dagster**.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Open-Meteo REST API       |
                      | (Faridabad: 28.4089, 77.3178)|
                      +--------------+--------------+
                                     |
                                     |  HTTP GET (with exponential retry)
                                     v
                      +-----------------------------+
                      |  Resilient Ingestion Client |
                      |    (Pydantic Validation)    |
                      +--------------+--------------+
                                     |
                                     |  Structured Records Stream
                                     v
                      +-----------------------------+
                      |  dlt (Data Load Tool)       |
                      |  write_disposition="merge"  |
                      |  PK: (location, timestamp)  |
                      +--------------+--------------+
                                     |
                                     |  Idempotent Upsert
                                     v
                      +-----------------------------+
                      |  DuckDB Columnar Storage    |
                      | (data/weather_data.duckdb)  |
                      +--------------+--------------+
                                     |
                                     |  Analytical SQL Queries
                                     v
                      +-----------------------------+
                      |  SQL Analytics Engine       |
                      | (Trends, Aggregates, Winds) |
                      +-----------------------------+
                                     ^
                                     | Orchestrates
                      +--------------+--------------+
                      |  Dagster Pipeline Engine    |
                      | (Hourly Schedules & Assets) |
                      +-----------------------------+
```

---

## Core Technologies
- **Python 3.11+**: Core development language.
- **Open-Meteo REST API**: Free, open weather API without API key requirement.
- **dlt (data load tool)**: Declarative, schema-evolving data loader with automatic deduplication.
- **DuckDB**: Fast, embedded columnar analytical OLAP database.
- **SQL**: Analytical window functions, rolling averages, and statistical aggregations.
- **Dagster**: Modern data orchestration platform with Software-Defined Assets and hourly schedules.
- **pytest**: Automated unit, integration, and end-to-end test suite.

---

## Project Structure

```
weather-data-ingestion/
├── .env.example                          # Environment variable configuration template
├── .gitignore                            # Excludes venv, databases, temporary files, and secrets
├── README.md                             # Project documentation and Windows guide
├── requirements.txt                      # Production & testing dependencies
├── pytest.ini                            # Test configuration and marker definitions
├── run_pipeline.py                       # Standalone CLI runner for execution and testing
├── data/                                 # DuckDB storage directory (created on first run)
│   └── weather_data.duckdb
├── docs/
│   ├── ARCHITECTURE.md                  # Comprehensive technical architecture & diagrams
│   └── VIVA_PRESENTATION_GUIDE.md        # College presentation & viva voce questions guide
├── sql/
│   ├── recent_readings.sql               # Latest 24 observations
│   ├── temperature_stats.sql             # Daily average, min, max, diurnal ranges
│   ├── humidity_wind_stats.sql           # Humidity conditions & Beaufort wind classifications
│   └── historical_trends.sql             # 6-hour moving averages & warming/cooling trends
├── src/
│   └── weather_pipeline/
│       ├── __init__.py
│       ├── config.py                     # Configuration settings & environment variables
│       ├── models.py                     # Pydantic data models & WMO code mapper
│       ├── client.py                     # Open-Meteo REST API client with retries
│       ├── pipeline.py                   # dlt pipeline with DuckDB merge disposition
│       ├── analytics.py                  # DuckDB SQL analytics runner & table formatter
│       └── dagster_orchestration/
│           ├── __init__.py
│           ├── assets.py                 # Software-defined assets (extract -> load -> analyze)
│           ├── jobs.py                   # Ingestion job definitions
│           ├── schedules.py              # Hourly cron schedule (Asia/Kolkata)
│           └── definitions.py            # Dagster repository definitions
└── tests/
    ├── __init__.py
    ├── conftest.py                       # Test fixtures & mock API payloads
    ├── test_client.py                    # Unit & live API tests
    ├── test_pipeline.py                  # dlt ingestion & deduplication tests
    ├── test_analytics.py                 # DuckDB SQL analytics tests
    └── test_dagster.py                   # Dagster asset & job execution tests
```

---

## Getting Started on Windows

### Prerequisites
- **Python 3.11 or higher** installed. (Check with `python --version` in PowerShell).
- **Git** installed. (Check with `git --version`).

### Step 1: Open PowerShell in Project Folder
Open Windows PowerShell and navigate to the project directory:
```powershell
cd "c:\Users\AYAN MANDAL\OneDrive\文档\PROJECTS\WEATHER-DATA-INGESTION"
```

### Step 2: Create and Activate Virtual Environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```
*(Note: If PowerShell shows a script execution policy restriction, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` and retry activation).*

### Step 3: Install Dependencies
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Configure Environment (Optional)
The project includes pre-configured defaults for Faridabad, Haryana, India. If you wish to customize parameters, copy `.env.example`:
```powershell
Copy-Item .env.example .env
```

---

## Running the Pipeline

### 1. Standalone CLI Runner (`run_pipeline.py`)

Run the complete pipeline (verify connection -> fetch weather -> load into DuckDB -> run all SQL analytics):
```powershell
python run_pipeline.py
```

Other convenient CLI flags:
```powershell
python run_pipeline.py --verify       # Test Open-Meteo connection without writing to DB
python run_pipeline.py --ingest       # Fetch and ingest data into DuckDB only
python run_pipeline.py --analytics    # Execute and print all SQL analytical tables
```

### 2. Launching the Dagster UI (Web Dashboard)

Launch Dagster's web interface to inspect asset lineage, run history, and hourly schedules:
```powershell
dagster dev -f src/weather_pipeline/dagster_orchestration/definitions.py
```
Open your browser and navigate to: **`http://localhost:3000`**

From the UI:
- **Lineage Graph**: Inspect the dependency graph: `fetch_weather_data` -> `load_weather_to_duckdb` -> `weather_analytics_report`.
- **Materialize Assets**: Click **Materialize all** to manually trigger the full pipeline.
- **Schedules**: View and enable the `hourly_weather_schedule` running every hour.
- **Metadata**: Click on `weather_analytics_report` to view latest temperature, humidity, wind speed, and ASCII report tables.

---

## SQL Analytics Details

The SQL queries in the `sql/` directory run directly against the persistent DuckDB storage:

1. **`recent_readings.sql`**: Retrieves the newest 24 observations with temperature (°C and °F), humidity, wind speed, and descriptive condition.
2. **`temperature_stats.sql`**: Groups by observation date to calculate daily averages, minimums, maximums, and diurnal temperature ranges.
3. **`humidity_wind_stats.sql`**: Categorizes humidity into comfort bands (*Dry*, *Comfortable*, *Humid*, *Muggy*) and wind into Beaufort scale categories (*Calm*, *Light Breeze*, *Moderate Breeze*, *Strong Wind*).
4. **`historical_trends.sql`**: Uses window functions `AVG(...) OVER (ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)` and `LAG(...)` to compute 6-hour rolling averages and detect whether weather is warming, cooling, or stable.

---

## Automated Testing

Run the complete automated test suite with pytest:
```powershell
pytest -v
```

The test suite contains **18 automated tests**:
- **API Client Tests**: Mocks for network timeouts, 500 server errors, invalid payloads, missing values, and live API connectivity.
- **Data Validation Tests**: Pydantic checks for realistic temperature and humidity ranges.
- **dlt Pipeline Tests**: DuckDB table creation, schema types, and merge idempotency.
- **SQL Analytics Tests**: Verifies calculation of aggregates, window functions, and ASCII table generation.
- **Dagster Tests**: Structural validation, asset execution with mocked payloads, and in-process job execution.

---

## Git & Version Control

To initialize Git and prepare your repository:
```powershell
git init
git add .
git commit -m "feat: complete weather data ingestion pipeline with dlt, duckdb, sql, and dagster"
```
The `.gitignore` file automatically excludes the `.venv/` virtual environment, DuckDB database files (`*.duckdb`), and credentials.

---

## College Presentation & Viva Preparation
For explanations of the architecture, tech stack trade-offs, and sample viva questions, see:
- [`docs/VIVA_PRESENTATION_GUIDE.md`](docs/VIVA_PRESENTATION_GUIDE.md)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
