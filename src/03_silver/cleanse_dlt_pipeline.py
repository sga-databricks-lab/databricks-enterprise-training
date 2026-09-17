"""
Silver Layer DLT Pipeline

Cleans and enriches bronze clickstream data:
- Streams from bronze layer
- Joins with dimension tables
- Applies data quality checks
- Deduplicates events using watermark
- Outputs to silver layer with liquid clustering
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Read configuration from DLT pipeline or use defaults
try:
    CATALOG = spark.conf.get("catalog", "dev")
    SCHEMA_BRONZE = spark.conf.get("schema_bronze", "bronze")
except:
    CATALOG = "dev"
    SCHEMA_BRONZE = "bronze"

@dlt.table(
    name="silver_clickstream",
    comment="Cleaned silver layer with enriched clickstream events",
    table_properties={
        "quality": "silver"
    },
    cluster_by=["user_id", "event_timestamp"]
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_unit_price", "unit_price > 0")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_timestamp", "event_timestamp IS NOT NULL")
def create_silver_cleaned():
    """
    Create cleaned silver layer with streaming data quality and deduplication.
    
    Quality checks drop invalid records (negative prices, missing IDs, etc.).
    Deduplication uses watermark to handle late-arriving events efficiently.
    """
    # Read bronze stream
    bronze_df = dlt.read_stream(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_clickstream_events")
    
    # Read dimension table
    dim_users_df = dlt.read(f"{CATALOG}.{SCHEMA_BRONZE}.dim_users")
    
    # Enrich with user dimension (stream-static join)
    df = bronze_df.join(dim_users_df, "user_id", "left")
    
    # Cast columns to proper types for quality expectations
    df = df.withColumn("quantity", F.col("quantity").cast("int")) \
           .withColumn("unit_price", F.col("unit_price").cast("double")) \
           .withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))
    
    # Apply watermark for late data (5 minute grace period)
    df_with_watermark = df.withWatermark("event_timestamp", "5 minutes")
    
    # Deduplicate events within watermark window (keeps first occurrence)
    df_deduped = df_with_watermark.dropDuplicatesWithinWatermark(
        ["event_id"]
    )
    
    # Add processing timestamp for lineage tracking
    df_final = df_deduped.withColumn(
        "processing_timestamp",
        F.current_timestamp()
    )
    
    return df_final