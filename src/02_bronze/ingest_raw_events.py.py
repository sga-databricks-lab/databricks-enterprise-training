# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Auto Loader Pipeline Script.
# MAGIC
# MAGIC Run this cell to ingest all generated JSON batches incrementally into workspace.default.bronze_clickstream_events:

# COMMAND ----------

# DBTITLE 1,Cell 2
# File: src/bronze/ingest_raw_events.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

class BronzeClickstreamIngestion:
    def __init__(self, catalog: str = "${var.catalog}", schema: str = "${var.schema_bronze}", volume: str = "raw_data"):
        self.spark = SparkSession.builder.getOrCreate()
        
        # Exact Storage Endpoints aligned with Data Generator
        self.landing_path = f"/Volumes/{catalog}/{schema}/{volume}/clickstream_landing/"
        self.bronze_table = f"{catalog}.{schema}.bronze_clickstream_events"
        self.checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/clickstream_events/"
        self.schema_path = f"/Volumes/{catalog}/{schema}/{volume}/_schemas/clickstream_events/"

    def run_pipeline(self):
        print(f"Starting Auto Loader Stream from: {self.landing_path}")

        # 1. Read Stream via Auto Loader with Rescued Data Tracking
        df_raw = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", self.schema_path)
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(self.landing_path)
        )

        # 2. Bronze Transformations (Metadata & Lineage)
        df_bronze = (
            df_raw
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )

        # 3. Write Incremental Stream to Delta Table
        query = (
            df_bronze.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .table(self.bronze_table)
        )
        
        query.awaitTermination()
        print(f"Ingestion finished successfully into table: {self.bronze_table}")

if __name__ == "__main__":
    pipeline = BronzeClickstreamIngestion()
    pipeline.run_pipeline()

# COMMAND ----------

