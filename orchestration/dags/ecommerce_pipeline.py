from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='ecommerce_pipeline',
    default_args=default_args,
    description='End-to-end e-commerce data pipeline — ingest, transform, test, validate',
    schedule_interval='0 6 * * *',  # runs daily at 6am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce', 'bronze', 'silver', 'gold', 'dbt', 'snowflake'],
) as dag:

    # ── TASK 1: INGEST RAW DATA ───────────────────────────────────────────────
    ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=lambda: __import__('subprocess').run(
            ['python', '/opt/airflow/src/ingestion/ingest_olist.py'],
            check=True
        ),
        doc_md="""
        ## Ingest Bronze
        Downloads all 9 Olist CSV files from Kaggle and loads them
        into Snowflake BRONZE schema. Overwrites existing data.
        """
    )

    # ── TASK 2: RUN DBT BRONZE MODELS ────────────────────────────────────────
    dbt_bronze = BashOperator(
        task_id='dbt_bronze',
        bash_command="""
            dbt run
              --profiles-dir /opt/airflow/dbt
              --project-dir /opt/airflow/dbt
              --select bronze
        """,
        doc_md="""
        ## dbt Bronze
        Creates views over raw Snowflake BRONZE tables.
        """
    )

    # ── TASK 3: RUN DBT SILVER MODELS ────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
            dbt run
              --profiles-dir /opt/airflow/dbt
              --project-dir /opt/airflow/dbt
              --select silver
        """,
        doc_md="""
        ## dbt Silver
        Cleans and types all bronze tables into silver layer.
        """
    )

    # ── TASK 4: RUN DBT GOLD MODELS ──────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
            dbt run
              --profiles-dir /opt/airflow/dbt
              --project-dir /opt/airflow/dbt
              --select gold
        """,
        doc_md="""
        ## dbt Gold
        Builds star schema — fact_orders and dimension tables.
        """
    )

    # ── TASK 5: RUN DBT TESTS ─────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
            dbt test
              --profiles-dir /opt/airflow/dbt
              --project-dir /opt/airflow/dbt
        """,
        doc_md="""
        ## dbt Test
        Runs all 45 data quality tests across bronze, silver, gold.
        Pipeline fails here if any test fails.
        """
    )

    # ── TASK 6: GREAT EXPECTATIONS VALIDATION ────────────────────────────────
    gx_validate = PythonOperator(
        task_id='gx_validate',
        python_callable=lambda: __import__('subprocess').run(
            ['python', '/opt/airflow/great_expectations/gx_validate_gold.py'],
            check=True
        ),
        doc_md="""
        ## Great Expectations
        Validates gold layer tables against business rules.
        Checks row counts, null rates, value sets, and ranges.
        """
    )

    # ── TASK DEPENDENCIES — defines the order ────────────────────────────────
    ingest_bronze >> dbt_bronze >> dbt_silver >> dbt_gold >> dbt_test >> gx_validate
