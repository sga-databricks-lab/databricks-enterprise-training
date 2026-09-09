import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

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
    Create cleaned silver layer with DLT streaming:
    - Stream from bronze using DLT streaming
    - Join with dim_users
    - Data quality checks via expectations (drop invalid records)
    - Deduplication with streaming window
    - Derived processing timestamp
    """
    
    # Step 1: Read bronze stream
    bronze_df = dlt.read_stream("bronze_clickstream_events")
    
    # Step 2: Read dim_users as static dimension table
    dim_users_df = dlt.read("dim_users")
    
    # Step 3: Join bronze with dim_users (stream-static join)
    df = bronze_df.join(dim_users_df, "user_id", "left")
    
    # Step 4: Cast columns to proper types for quality checks
    # (Expectations will filter these automatically)
    df = df.withColumn("quantity", F.col("quantity").cast("int")) \
           .withColumn("unit_price", F.col("unit_price").cast("double"))
    
    # Step 5: STREAMING DEDUPLICATION with Watermark
    # Apply watermark for handling late data (5 minutes)
    df_with_watermark = df.withWatermark("event_timestamp", "5 minutes")
    
    # Deduplication: Keep latest record per event_id within watermark window
    # dropDuplicatesWithinWatermark is optimized for streaming and uses watermark
    df_deduped = df_with_watermark.dropDuplicatesWithinWatermark(
        ["event_id"], 
        orderBy=F.col("_ingested_at").desc()
    )
    
    # Step 6: Add processing timestamp
    df_final = df_deduped.withColumn(
        "processing_timestamp",
        F.current_timestamp()
    )
    
    return df_final