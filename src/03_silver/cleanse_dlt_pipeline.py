# ============================================================================
# SILVER LAYER DLT PIPELINE - DATA CLEANSING & QUALITY
# ============================================================================
# Purpose: Cleanse and validate raw bronze data with quality expectations
# Input: workspace.default.bronze_clickstream_events (raw events)
# Output: dev_silver.clean_events (validated, cleansed events)
# ============================================================================

import dlt
from pyspark.sql.functions import col, trim, upper, when, to_timestamp

# ============================================================================
# SILVER TABLE: CLEAN CLICKSTREAM EVENTS
# ============================================================================
# Applies data quality rules and cleansing transformations
# Quality expectations:
#   - event_id must not be null (critical - will drop invalid records)
#   - user_id must not be null (critical)
#   - valid_email format check (warning only)
#   - positive prices (warning only)
# ============================================================================

@dlt.table(
    name="clean_events",
    comment="Cleansed clickstream events with data quality validations",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "product_id,event_type"
    }
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect("valid_email", "email LIKE '%@%'")
@dlt.expect("positive_price", "unit_price >= 0")
def silver_clean_events():
    """
    Read from bronze layer and apply cleansing transformations.
    
    Transformations:
    - Trim whitespace from string fields
    - Standardize event_type to uppercase
    - Parse event_timestamp to proper timestamp format
    - Filter out null critical fields (via expect_or_drop)
    
    Returns:
        Cleansed DataFrame for silver layer
    """
    return (
        dlt.read("workspace.default.bronze_clickstream_events")
        .select(
            col("event_id"),
            col("user_id"),
            trim(col("email")).alias("email"),
            col("session_id"),
            upper(trim(col("event_type"))).alias("event_type"),  # Standardize to uppercase
            col("product_id"),
            col("quantity"),
            col("unit_price"),
            to_timestamp(col("event_timestamp")).alias("event_timestamp"),
            col("page_url"),
            col("user_agent"),
            col("ip_address"),
            col("country"),
            col("city")
        )
    )

# ============================================================================
# SILVER TABLE: EVENT METRICS AGGREGATION
# ============================================================================
# Aggregate metrics per product for downstream analytics
# ============================================================================

@dlt.table(
    name="product_metrics",
    comment="Aggregated product-level metrics from clean events"
)
def silver_product_metrics():
    """
    Aggregate event metrics by product.
    
    Metrics:
    - Total events per product
    - Unique users per product
    - Total quantity sold
    - Average price
    
    Returns:
        Aggregated product metrics
    """
    return (
        dlt.read("clean_events")
        .groupBy("product_id", "event_type")
        .agg(
            {"event_id": "count",
             "user_id": "countDistinct",
             "quantity": "sum",
             "unit_price": "avg"}
        )
        .withColumnRenamed("count(event_id)", "event_count")
        .withColumnRenamed("count(DISTINCT user_id)", "unique_users")
        .withColumnRenamed("sum(quantity)", "total_quantity")
        .withColumnRenamed("avg(unit_price)", "avg_price")
    )
