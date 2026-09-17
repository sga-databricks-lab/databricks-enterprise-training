#incremental data generation
from pyspark.sql import functions as F
from datetime import datetime
import time

# Enable Delta Change Data Feed (CDF)
spark.sql("ALTER TABLE workspace.default.dim_users SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

LANDING_PATH = "/Volumes/workspace/default/raw_data/clickstream_landing"

def generate_single_batch(records_per_batch=2000, inject_bad_records=True):
    timestamp_id = int(time.time())
    
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
    
    if inject_bad_records:
        current_time = datetime.now()
        dirty_df = spark.createDataFrame([
            (9999, "", "USR_999", "sess_bad", "PROD_1", "purchase", -5, 10.0, current_time)
        ], df.schema)
        df = df.union(dirty_df)

    output_file = f"{LANDING_PATH}/batch_{timestamp_id}.json"
    df.coalesce(1).write.mode("overwrite").json(output_file)

# Run to push a new batch to landing volume
generate_single_batch()