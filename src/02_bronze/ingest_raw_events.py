"""
Bronze Layer Ingestion Pipeline

Ingests raw clickstream events from landing zone to bronze Delta table using Auto Loader.
Supports schema evolution, rescued data tracking, and incremental processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Read parameters from widgets or bundle configuration
dbutils.widgets.text("catalog", "dev", "Catalog name (dev or prod)")
dbutils.widgets.text("schema_bronze", "bronze", "Bronze schema name")

class BronzeClickstreamIngestion:
    """Ingest raw clickstream JSON files into bronze Delta table using Auto Loader."""
    
    def __init__(self, catalog: str = None, schema: str = None, volume: str = "raw_data"):
        """Initialize ingestion pipeline with catalog, schema, and volume configuration."""
        # Use widget values if parameters not provided
        if catalog is None:
            catalog = dbutils.widgets.get("catalog")
        if schema is None:
            schema = dbutils.widgets.get("schema_bronze")
        self.spark = SparkSession.builder.getOrCreate()
        
        # Define storage paths
        self.landing_path = f"/Volumes/{catalog}/{schema}/{volume}/clickstream_landing/"
        self.bronze_table = f"{catalog}.{schema}.bronze_clickstream_events"
        self.checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/clickstream_events/"
        self.schema_path = f"/Volumes/{catalog}/{schema}/{volume}/_schemas/clickstream_events/"

    def _ensure_table_exists(self):
        """Create the bronze Delta table with log retention properties if it doesn't exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.bronze_table}
            USING DELTA
            TBLPROPERTIES (
                'delta.logRetentionDuration' = 'interval 60 days',
                'delta.deletedFileRetentionDuration' = 'interval 14 days'
            )
        """)
        print(f"Table ensured with retention properties: {self.bronze_table}")

    def run_pipeline(self):
        """Run Auto Loader streaming pipeline to ingest JSON files into Delta table."""
        self._ensure_table_exists()
        print(f"Starting Auto Loader Stream from: {self.landing_path}")

        # Read stream using Auto Loader with schema inference and rescued data
        df_raw = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", self.schema_path)
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(self.landing_path)
        )

        # Add ingestion metadata for lineage tracking
        df_bronze = (
            df_raw
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )

        # Write stream to Delta table with trigger availableNow for batch processing
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