"""
Query Execution Benchmarking Tool

Benchmarks query performance by measuring execution time, bytes scanned, file pruning,
and memory/disk spills. Uses noop sink to avoid write overhead during testing.
"""

# Read parameters from widgets or bundle configuration
dbutils.widgets.text("catalog", "dev", "Catalog name (dev or prod)")
dbutils.widgets.text("schema_bronze", "bronze", "Bronze schema name")
dbutils.widgets.text("schema_silver", "silver", "Silver schema name")
dbutils.widgets.text("schema_gold", "gold", "Gold schema name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")

import time
import re
from pyspark.sql import SparkSession


class QueryExecutionBenchmarker:
    """Benchmark query execution and extract performance metrics from explain plans."""
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _sanitize_sql(self, raw_sql: str) -> str:
        """Sanitize SQL by removing non-ASCII characters and trailing semicolons."""
        # Replace non-ASCII characters with spaces
        clean = re.sub(r'[^\x00-\x7F]+', ' ', raw_sql)
        # Normalize whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean.endswith(';'):
            clean = clean[:-1]
        return clean

    def run_benchmark(self, query_sql: str, test_name: str = "Gold Layer Query Execution"):
        """Execute query and collect performance metrics."""
        clean_query = self._sanitize_sql(query_sql)
        start_time = time.time()
        
        # Execute query using noop sink to avoid write overhead
        df = self.spark.sql(clean_query)
        df.write.format("noop").mode("overwrite").save()
        
        total_time = round(time.time() - start_time, 2)
        
        # Extract metrics from physical execution plan
        explain_plan = self.spark.sql(f"EXPLAIN {clean_query}").collect()[0][0]
        
        metrics = self._parse_metrics(explain_plan)
        metrics["test_name"] = test_name
        metrics["execution_time_sec"] = total_time
        
        self._print_report(metrics)
        return metrics

    def _parse_metrics(self, plan_text: str) -> dict:
        """Extract performance metrics from query execution plan."""
        bytes_read = re.search(r"size of files read:\s*([\d\.]+\s*[KMGT]?B)", plan_text, re.IGNORECASE)
        files_read = re.search(r"number of files read:\s*(\d+)", plan_text, re.IGNORECASE)
        files_pruned = re.search(r"files pruned:\s*(\d+)", plan_text, re.IGNORECASE)
        mem_spill = re.search(r"memory bytes spilled:\s*([\d\.]+\s*[KMGT]?B)", plan_text, re.IGNORECASE)
        disk_spill = re.search(r"disk bytes spilled:\s*([\d\.]+\s*[KMGT]?B)", plan_text, re.IGNORECASE)

        return {
            "bytes_scanned": bytes_read.group(1) if bytes_read else "0 B",
            "files_read": int(files_read.group(1)) if files_read else 0,
            "files_pruned": int(files_pruned.group(1)) if files_pruned else 0,
            "memory_spilled": mem_spill.group(1) if mem_spill else "0 B",
            "disk_spilled": disk_spill.group(1) if disk_spill else "0 B"
        }

    def _print_report(self, m: dict):
        """Print formatted benchmark report."""
        total_files = m["files_read"] + m["files_pruned"]
        prune_pct = round((m["files_pruned"] / total_files * 100), 2) if total_files > 0 else 0.0
        
        print("=" * 60)
        print(f" BENCHMARK REPORT: {m['test_name']}")
        print("=" * 60)
        print(f" Execution Time      : {m['execution_time_sec']} sec")
        print(f" Bytes Scanned       : {m['bytes_scanned']}")
        print(f" Files Read / Total  : {m['files_read']} / {total_files} ({prune_pct}% Pruned)")
        print(f" Memory Spill        : {m['memory_spilled']}")
        print(f" Disk Spill          : {m['disk_spilled']}")
        print("=" * 60 + "\n")

# Initialize benchmarker
benchmarker = QueryExecutionBenchmarker(spark)

# Test 1: Liquid Clustering - verify file pruning on clustered columns (product_id, event_type)
liquid_clustering_query = f"""
SELECT 
    product_id,
    event_type,
    COUNT(DISTINCT session_id) as unique_sessions,
    SUM(CAST(quantity AS INT)) as total_qty,
    ROUND(AVG(CAST(unit_price AS DOUBLE)), 2) as avg_unit_price
FROM {CATALOG}.{SCHEMA_BRONZE}.bronze_clickstream_events
WHERE product_id IN ('PROD_10', 'PROD_25', 'PROD_50')
  AND event_type = 'purchase'
GROUP BY product_id, event_type
"""

benchmarker.run_benchmark(
    query_sql=liquid_clustering_query, 
    test_name="Gold Aggregation - Liquid Clustering Active"
)

# Test 2: Disaster Recovery - validate cross-table integrity and region parity
dr_failover_query = f"""
SELECT 
    u.user_id,
    u.email,
    COUNT(e.event_id) as total_user_events,
    MAX(e.event_timestamp) as latest_event_timestamp,
    ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN CAST(e.quantity AS INT) * CAST(e.unit_price AS DOUBLE) ELSE 0 END), 2) as total_spend
FROM {CATALOG}.{SCHEMA_BRONZE}.bronze_clickstream_events e
JOIN {CATALOG}.{SCHEMA_BRONZE}.dim_users u
  ON e.user_id = u.user_id
GROUP BY u.user_id, u.email
"""

benchmarker.run_benchmark(
    query_sql=dr_failover_query, 
    test_name="Disaster Recovery - Secondary Region Parity Check"
)



