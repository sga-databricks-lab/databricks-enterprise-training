# one time load data 
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time
from datetime import datetime

# 1. Create the volume if it doesn't exist, then define landing path
spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.default.raw_data
""")

LANDING_PATH = "/Volumes/workspace/default/raw_data/clickstream_landing"
dbutils.fs.mkdirs(LANDING_PATH)

def generate_user_metadata(num_users=1000):
    """Generates user dimension table with raw PII for masking labs."""
    return (
        spark.range(1, num_users + 1)
        .withColumn("user_id", F.concat(F.lit("USR_"), F.col("id")))
        .withColumn("full_name", F.concat(F.lit("User_"), F.col("id")))
        .withColumn("email", F.concat(F.lit("user_"), F.col("id"), F.lit("@company.com")))
        .withColumn("credit_card_num", F.concat(F.lit("4532-"), (F.rand() * 8999 + 1000).cast("int"), F.lit("-"), (F.rand() * 8999 + 1000).cast("int"), F.lit("-"), (F.rand() * 8999 + 1000).cast("int")))
        .withColumn("ip_address", F.concat((F.rand() * 200 + 10).cast("int"), F.lit("."), (F.rand() * 250).cast("int"), F.lit(".1.100")))
        .drop("id")
    )

def generate_streaming_batch(batch_id, records_per_batch=2000, inject_bad_records=True):
    """Generates continuous raw JSON clickstream events simulating real-time file arrivals."""
    df = (
        spark.range(0, records_per_batch)
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("user_id", F.concat(F.lit("USR_"), (F.rand() * 1000 + 1).cast("int")))
        .withColumn("session_id", F.expr("uuid()"))
        .withColumn("product_id", F.concat(F.lit("PROD_"), (F.rand() * 50 + 1).cast("int")))
        .withColumn("event_type", F.element_at(F.array(F.lit("view"), F.lit("cart"), F.lit("purchase")), (F.rand() * 3 + 1).cast("int")))
        .withColumn("quantity", (F.rand() * 5 + 1).cast("int"))
        .withColumn("unit_price", F.round(F.rand() * 100 + 5, 2))
        .withColumn("event_timestamp", F.current_timestamp())
    )
    
    # Inject dirty data to test expectations
    if inject_bad_records and batch_id % 2 == 0:
        current_time = datetime.now()
        dirty_df = spark.createDataFrame([
            (9999, "", "USR_999", "sess_bad", "PROD_1", "purchase", -5, 10.0, current_time),
            (9998, "evt_corrupt", "USR_998", "sess_bad", "PROD_2", "view", 0, -50.0, current_time)
        ], df.schema)
        df = df.union(dirty_df)

    output_file = f"{LANDING_PATH}/batch_{batch_id}_{int(time.time())}.json"
    df.coalesce(1).write.mode("overwrite").json(output_file)
    print(f"Generated raw streaming file: {output_file}")

# Execute Initial Setup
user_dim = generate_user_metadata(1000)
user_dim.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_users")

# Simulate initial 5 micro-batches for Auto Loader landing
for b in range(1, 6):
    generate_streaming_batch(batch_id=b)