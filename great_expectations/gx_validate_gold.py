import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# ─── CONNECT TO SNOWFLAKE ─────────────────────────────────────────────────────
def get_snowflake_df(query):
    engine = create_engine(
        "snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}".format(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="GOLD",
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
    )
    df = pd.read_sql(query, engine)
    df.columns = [col.lower() for col in df.columns]
    engine.dispose()
    return df

# ─── VALIDATE A DATAFRAME ─────────────────────────────────────────────────────
def validate_df(df, suite_name, expectations):
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(name=suite_name)
    da = ds.add_dataframe_asset(name=suite_name)
    batch_def = da.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch_def, suite=suite)
    )
    result = validation_def.run(batch_parameters={"dataframe": df})
    return result

# ─── VALIDATE FACT_ORDERS ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("Validating GOLD.FACT_ORDERS")
print("="*60)

fact_orders_df = get_snowflake_df("SELECT * FROM ECOMMERCE_DB.GOLD.FACT_ORDERS")

result_orders = validate_df(fact_orders_df, "fact_orders_suite", [
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=90000, max_value=110000),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_date"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_status"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="total_order_value"),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="order_status",
        value_set=["delivered", "shipped", "canceled", "unavailable",
                   "invoiced", "processing", "created", "approved"]
    ),
    gx.expectations.ExpectColumnValuesToBeBetween(column="total_order_value", min_value=0),
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="avg_review_score", min_value=1, max_value=5, mostly=0.99
    ),
])

print(f"Status  : {'PASSED' if result_orders.success else 'FAILED'}")
print(f"Results : {result_orders.statistics['successful_expectations']}/{result_orders.statistics['evaluated_expectations']} expectations passed")

# ─── VALIDATE DIM_CUSTOMERS ───────────────────────────────────────────────────
print("\n" + "="*60)
print("Validating GOLD.DIM_CUSTOMERS")
print("="*60)

dim_customers_df = get_snowflake_df("SELECT * FROM ECOMMERCE_DB.GOLD.DIM_CUSTOMERS")

result_customers = validate_df(dim_customers_df, "dim_customers_suite", [
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=90000, max_value=110000),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_state"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_segment"),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="customer_segment",
        value_set=["one_time", "occasional", "loyal"]
    ),
])

print(f"Status  : {'PASSED' if result_customers.success else 'FAILED'}")
print(f"Results : {result_customers.statistics['successful_expectations']}/{result_customers.statistics['evaluated_expectations']} expectations passed")

# ─── VALIDATE DIM_SELLERS ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("Validating GOLD.DIM_SELLERS")
print("="*60)

dim_sellers_df = get_snowflake_df("SELECT * FROM ECOMMERCE_DB.GOLD.DIM_SELLERS")

result_sellers = validate_df(dim_sellers_df, "dim_sellers_suite", [
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=2000, max_value=5000),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_state"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="seller_tier"),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="seller_tier",
        value_set=["platinum", "gold", "silver", "bronze"]
    ),
    gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0),
])

print(f"Status  : {'PASSED' if result_sellers.success else 'FAILED'}")
print(f"Results : {result_sellers.statistics['successful_expectations']}/{result_sellers.statistics['evaluated_expectations']} expectations passed")

# ─── SUMMARY ──────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("GREAT EXPECTATIONS VALIDATION SUMMARY")
print("="*60)
all_passed = result_orders.success and result_customers.success and result_sellers.success
print(f"fact_orders   : {'PASSED' if result_orders.success   else 'FAILED'}")
print(f"dim_customers : {'PASSED' if result_customers.success else 'FAILED'}")
print(f"dim_sellers   : {'PASSED' if result_sellers.success   else 'FAILED'}")
print(f"\nOverall       : {'ALL PASSED' if all_passed else 'FAILURES DETECTED'}")
print("="*60)