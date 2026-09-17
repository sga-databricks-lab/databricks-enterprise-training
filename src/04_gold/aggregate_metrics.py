"""
Gold Layer Aggregation Metrics

Creates analytics-ready aggregated tables from silver layer data:
- gold_daily_user_metrics: Daily user activity and revenue metrics
- gold_product_daily_metrics: Daily product performance metrics

Both tables use liquid clustering for optimal query performance.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Read configuration from DLT pipeline or use defaults
try:
    CATALOG = spark.conf.get("catalog", "dev")
    SCHEMA_SILVER = spark.conf.get("schema_silver", "silver")
except:
    CATALOG = "dev"
    SCHEMA_SILVER = "silver"

@dlt.table(
    name="gold_daily_user_metrics",
    comment="Daily aggregated user metrics with revenue and activity analysis",
    table_properties={
        "quality": "gold",
        "layer": "analytics"
    },
    cluster_by=["event_date", "user_id"]
)
@dlt.expect_or_drop("valid_event_date", "event_date IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect("reasonable_revenue", "total_revenue >= 0")
def create_daily_user_metrics():
    """
    Create daily user metrics aggregated from silver layer.
    
    Includes event counts, revenue metrics, product metrics, and conversion rates.
    Liquid clustering optimizes queries filtering by event_date and user_id.
    """
    # Read silver layer data
    silver_df = dlt.read(f"{CATALOG}.{SCHEMA_SILVER}.silver_clickstream")
    
    # Extract event date for daily aggregation
    df_with_date = silver_df.withColumn(
        "event_date", 
        F.to_date(F.col("event_timestamp"))
    )
    
    # Calculate revenue per event
    df_with_revenue = df_with_date.withColumn(
        "revenue",
        (F.col("quantity") * F.col("unit_price")).cast(DecimalType(18, 2))
    )
    
    # Aggregate metrics by date and user
    daily_metrics = df_with_revenue.groupBy("event_date", "user_id").agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        
        # Revenue metrics
        F.sum("revenue").cast(DecimalType(18, 2)).alias("total_revenue"),
        F.avg("revenue").cast(DecimalType(18, 2)).alias("avg_order_value"),
        F.max("revenue").cast(DecimalType(18, 2)).alias("max_order_value"),
        
        # Product metrics
        F.countDistinct("product_id").alias("unique_products_viewed"),
        F.sum("quantity").alias("total_quantity"),
        
        # Session metrics
        F.countDistinct("session_id").alias("session_count"),
        
        # Timestamps
        F.min("event_timestamp").alias("first_event_time"),
        F.max("event_timestamp").alias("last_event_time")
    )
    
    # Calculate conversion rate
    daily_metrics_final = daily_metrics.withColumn(
        "conversion_rate",
        F.when(
            F.col("view_count") > 0,
            (F.col("purchase_count") / F.col("view_count")).cast(DecimalType(5, 4))
        ).otherwise(0)
    )
    
    # Add processing timestamp
    result = daily_metrics_final.withColumn(
        "processed_at",
        F.current_timestamp()
    )
    
    return result


@dlt.table(
    name="gold_product_daily_metrics",
    comment="Daily product performance metrics",
    table_properties={
        "quality": "gold",
        "layer": "analytics"
    },
    cluster_by=["event_date", "product_id"]
)
@dlt.expect_or_drop("valid_product_date", "event_date IS NOT NULL AND product_id IS NOT NULL")
def create_product_daily_metrics():
    """
    Create daily product performance metrics.
    
    Tracks views, purchases, revenue, and conversion funnel metrics.
    Liquid clustering optimizes queries filtering by event_date and product_id.
    """
    # Read silver layer data
    silver_df = dlt.read(f"{CATALOG}.{SCHEMA_SILVER}.silver_clickstream")
    
    # Extract event date and calculate revenue
    df_prep = silver_df.withColumn(
        "event_date", 
        F.to_date(F.col("event_timestamp"))
    ).withColumn(
        "revenue",
        (F.col("quantity") * F.col("unit_price")).cast(DecimalType(18, 2))
    )
    
    # Aggregate metrics by date and product
    product_metrics = df_prep.groupBy("event_date", "product_id").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        
        # Revenue metrics
        F.sum("revenue").cast(DecimalType(18, 2)).alias("total_revenue"),
        F.avg("revenue").cast(DecimalType(18, 2)).alias("avg_revenue_per_event"),
        
        # Quantity metrics
        F.sum("quantity").alias("total_quantity_sold"),
        F.avg("unit_price").cast(DecimalType(18, 2)).alias("avg_unit_price"),
        
        # User engagement
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("unique_sessions")
    )
    
    # Calculate conversion funnel metrics
    result = product_metrics.withColumn(
        "cart_to_purchase_rate",
        F.when(
            F.col("cart_count") > 0,
            (F.col("purchase_count") / F.col("cart_count")).cast(DecimalType(5, 4))
        ).otherwise(0)
    ).withColumn(
        "view_to_purchase_rate",
        F.when(
            F.col("view_count") > 0,
            (F.col("purchase_count") / F.col("view_count")).cast(DecimalType(5, 4))
        ).otherwise(0)
    ).withColumn(
        "processed_at",
        F.current_timestamp()
    )
    
    return result
