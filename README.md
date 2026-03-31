# E-Commerce Data Engineering Project

End-to-end production-grade data engineering pipeline built on the
Brazilian E-Commerce dataset by Olist (~100k orders, 1.5M+ rows).

## Architecture

```
Kaggle (source) → Python ingestion → Snowflake BRONZE
                                   → dbt SILVER (cleaned)
                                   → dbt GOLD (star schema)
                                   → Great Expectations (validation)
                                   → Airflow (orchestration)
                                   → GitHub Actions (CI/CD)
```

## Tech stack

| Tool               | Purpose                                     |
| ------------------ | ------------------------------------------- |
| Snowflake          | Cloud data warehouse                        |
| dbt Core           | SQL transformations, testing, documentation |
| Apache Airflow     | Pipeline orchestration (Docker)             |
| Great Expectations | Data quality validation                     |
| GitHub Actions     | CI/CD — runs dbt test on every push         |
| Python 3.11        | Ingestion scripts and GX validation         |
| pandas             | CSV ingestion                               |
| Git + GitHub       | Version control and collaboration           |

## Project structure

```
ecommerce-de-project/
├── src/ingestion/          # Python ingestion scripts
├── dbt/
│   ├── models/bronze/      # Raw source views (9 models)
│   ├── models/silver/      # Cleaned and typed tables (8 models)
│   ├── models/gold/        # Star schema — fact and dims (5 models)
│   ├── macros/             # Custom dbt macros
│   └── dbt_project.yml     # dbt configuration
├── great_expectations/     # GX validation suite
├── orchestration/
│   ├── dags/               # Airflow DAG definitions
│   └── docker-compose.yml  # Airflow Docker setup
├── .github/workflows/      # GitHub Actions CI
└── requirements.txt        # Pinned dependencies
```

## Data model — medallion architecture

### Bronze layer (9 tables)

Raw data loaded as-is from Kaggle. No transformations. dtype=str
preserves every value exactly as it arrived from the source.

### Silver layer (8 tables)

Cleaned and typed bronze data. Key transformations:

- `try_to_timestamp()` for safe date casting
- `try_to_number()` for safe numeric casting
- `lower(trim())` for text standardisation
- Derived columns: `actual_delivery_days`, `is_late_delivery`

### Gold layer — star schema (5 tables)

Business-ready tables optimised for analysis and ML feature engineering.

```
                 dim_dates
                     |
dim_customers — fact_orders — dim_products
                     |
               dim_sellers
```

| Table         | Description                                                   |
| ------------- | ------------------------------------------------------------- |
| fact_orders   | One row per order — financial, delivery, review metrics       |
| dim_customers | Deduplicated customers with lifetime metrics and segmentation |
| dim_products  | Products with English categories and volume                   |
| dim_sellers   | Sellers with revenue metrics and tier classification          |
| dim_dates     | Date spine 2016–2019 with calendar attributes                 |

## Data quality

- 45 dbt tests across all layers (not_null, unique, relationships, accepted_values)
- 20 Great Expectations validations on gold tables
- GitHub Actions runs all tests automatically on every push to main

## How to run locally

### 1. Clone the repo

```bash
git clone https://github.com/Shiva-Naturi/ecommerce-de-project.git
cd ecommerce-de-project
```

### 2. Set up environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Configure credentials

Create a `.env` file with your Snowflake credentials:

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Create `~/.dbt/profiles.yml` — see `profiles_template.yml` for structure.

### 4. Run the pipeline

```bash
# Ingest raw data
python src/ingestion/ingest_olist.py

# Run dbt models
dbt run --profiles-dir ~/.dbt --project-dir dbt

# Run dbt tests
dbt test --profiles-dir ~/.dbt --project-dir dbt

# Run Great Expectations validation
python great_expectations/gx_validate_gold.py
```

### 5. View dbt documentation

```bash
dbt docs generate --profiles-dir ~/.dbt --project-dir dbt
dbt docs serve --profiles-dir ~/.dbt --project-dir dbt
```

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— 100k orders, 9 relational tables, 2016–2018
