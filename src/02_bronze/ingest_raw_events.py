"""
Bronze Auto Loader Pipeline Script
==================================

This script implements the Bronze layer of a Medallion Architecture pipeline.
It uses Databricks Auto Loader to incrementally ingest raw clickstream JSON
events from a Unity Catalog volume and writes them into a Delta table with
basic metadata columns for lineage tracking.

Bronze Layer Responsibilities:
  - Ingest raw data as-is from the landing zone (no business transformations).
  - Preserve the original file source and ingestion timestamp for traceability.
  - Handle schema evolution automatically as new columns appear in source files.
"""

# SparkSession is the unified entry point for all Spark functionality (SQL, Streaming, etc.).
from pyspark.sql import SparkSession
# 'col' lets us reference column names (including nested struct fields like _metadata.file_path).
# 'current_timestamp' generates the current system timestamp for ingestion-time tracking.
from pyspark.sql.functions import col, current_timestamp


class BronzeClickstreamIngestion:
    """
    Encapsulates the entire Bronze-layer ingestion flow for clickstream events.

    Configurable defaults align with the Data Generator that drops JSON files
    into the landing volume.  Override the constructor arguments if your
    catalog/schema/volume names differ.
    """

    def __init__(self, catalog: str = "workspace", schema: str = "default", volume: str = "raw_data"):
        """
        Initialise the pipeline with storage endpoints.

        Parameters
        ----------
        catalog : str
            Unity Catalog catalog name (default: "workspace").
        schema  : str
            Unity Catalog schema (database) name (default: "default").
        volume : str
            Unity Catalog volume that holds the raw landing files (default: "raw_data").
        """
        # Get or create the active SparkSession.
        self.spark = SparkSession.builder.getOrCreate()

        # --- Storage Endpoints (all aligned with the Data Generator) ---
        # Landing zone: raw JSON files are dropped here by the data generator.
        self.landing_path = f"/Volumes/{catalog}/{schema}/{volume}/clickstream_landing/"
        # Target Delta table (fully-qualified for Unity Catalog).
        self.bronze_table = f"{catalog}.{schema}.bronze_clickstream_events"
        # Checkpoint location: Auto Loader stores progress here so it only processes
        # NEW files on each run (ensures exactly-once processing).
        self.checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/clickstream_events/"
        # Schema location: Auto Loader infers and stores the evolving schema here.
        # New columns detected in incoming files are automatically added.
        self.schema_path = f"/Volumes/{catalog}/{schema}/{volume}/_schemas/clickstream_events/"

    def run_pipeline(self):
        """
        Execute the full Bronze ingestion pipeline:
          1. Read raw files incrementally via Auto Loader.
          2. Attach lineage metadata (ingestion time + source file path).
          3. Stream-append the enriched rows into the Bronze Delta table.
        """
        print(f"Starting Auto Loader Stream from: {self.landing_path}")

        # ──────────────────────────────────────────────────────────────
        # STEP 1 — Read raw files incrementally via Auto Loader
        # ──────────────────────────────────────────────────────────────
        # Auto Loader (cloudFiles format) efficiently discovers and processes
        # new files as they arrive, without rescanning the entire directory.
        df_raw = (
            self.spark.readStream
            .format("cloudFiles")                          # Enable Auto Loader
            .option("cloudFiles.format", "json")           # Source files are JSON
            .option("cloudFiles.schemaLocation", self.schema_path)       # Where inferred schema is stored/evolved
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")     # Unrecognised fields land here instead of being dropped
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # Automatically add new columns found in future files
            .load(self.landing_path)                        # Read from the landing directory
        )

        # ──────────────────────────────────────────────────────────────
        # STEP 2 — Bronze Transformations (Metadata & Lineage)
        # ──────────────────────────────────────────────────────────────
        # In the Bronze layer we keep raw data unchanged but enrich it with
        # provenance columns so we can always trace a row back to its source.
        df_bronze = (
            df_raw
            # Record the exact time this row was ingested into the Bronze table.
            .withColumn("_ingested_at", current_timestamp())
            # Capture the source file path from Auto Loader's built-in _metadata struct.
            # _metadata.file_path tells us which file this row originated from.
            .withColumn("_source_file", col("_metadata.file_path"))
        )

        # ──────────────────────────────────────────────────────────────
        # STEP 3 — Write Incremental Stream to Delta Table
        # ──────────────────────────────────────────────────────────────
        # The stream writes new rows into the Bronze Delta table in append mode.
        # 'availableNow=True' processes all available files once, then stops
        # — ideal for batch-style triggered runs (e.g., from a Databricks Job).
        query = (
            df_bronze.writeStream
            .format("delta")                              # Write to a Delta Lake table
            .outputMode("append")                         # Only append new rows (no updates/deletes)
            .option("checkpointLocation", self.checkpoint_path)  # Track progress for exactly-once semantics
            .option("mergeSchema", "true")                  # Allow new columns to be merged into the table schema
            .trigger(availableNow=True)                    # Process everything available now, then terminate
            .table(self.bronze_table)                      # Target table name in Unity Catalog
        )

        # Block until the stream finishes processing all available files.
        query.awaitTermination()
        print(f"Ingestion finished successfully into table: {self.bronze_table}")


# ──────────────────────────────────────────────────────────────
# Entry point — only runs when the script is executed directly
# (not when imported as a module in another script/notebook).
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pipeline = BronzeClickstreamIngestion()
    pipeline.run_pipeline()