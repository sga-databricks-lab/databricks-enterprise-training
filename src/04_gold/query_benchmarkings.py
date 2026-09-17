# Execution Benchmarker
import time
import re
from pyspark.sql import SparkSession

class QueryExecutionBenchmarker:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _sanitize_sql(self, raw_sql: str) -> str:
        """Strips all hidden Unicode, non-ASCII characters, and trailing semicolons."""
        # Replace non-ASCII characters with standard spaces
        clean = re.sub(r'[^\x00-\x7F]+', ' ', raw_sql)
        # Collapse multiple spaces and newlines into single spaces
        clean = re.sub(r'\s+', ' ', clean).strip()
        # Remove trailing semicolon if present
        if clean.endswith(';'):
            clean = clean[:-1]
        return clean

    def run_benchmark(self, query_sql: str, test_name: str = "Gold Layer Query Execution"):
        clean_query = self._sanitize_sql(query_sql)
        start_time = time.time()
        
        # Execute query against noop sink
        df = self.spark.sql(clean_query)
        df.write.format("noop").mode("overwrite").save()
        
        total_time = round(time.time() - start_time, 2)
        
        # Extract physical execution plan
        explain_plan = self.spark.sql(f"EXPLAIN {clean_query}").collect()[0][0]
        
        metrics = self._parse_metrics(explain_plan)
        metrics["test_name"] = test_name
        metrics["execution_time_sec"] = total_time
        
        self._print_report(metrics)
        return metrics

    def _parse_metrics(self, plan_text: str) -> dict:
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

# Instantiate the benchmarker
benchmarker = QueryExecutionBenchmarker(spark)


# 1. Liquid Clustering Pruning Test
# Targets specific clustered columns (product_id & event_type) to verify file pruning performance
liquid_clustering_query = """
SELECT 
    product_id,
    event_type,
    COUNT(DISTINCT session_id) as unique_sessions,
    SUM(CAST(quantity AS INT)) as total_qty,
    ROUND(AVG(CAST(unit_price AS DOUBLE)), 2) as avg_unit_price
FROM workspace.default.bronze_clickstream_events
WHERE product_id IN ('PROD_10', 'PROD_25', 'PROD_50')
  AND event_type = 'purchase'
GROUP BY product_id, event_type
"""

benchmarker.run_benchmark(
    query_sql=liquid_clustering_query, 
    test_name="Gold Aggregation - Liquid Clustering Active"
)

# 2. Disaster Recovery Validation
# Tests cross-table integrity with dim_users, event counts, and total spend for region parity
dr_failover_query = """
SELECT 
    u.user_id,
    u.email,
    COUNT(e.event_id) as total_user_events,
    MAX(e.event_timestamp) as latest_event_timestamp,
    ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN CAST(e.quantity AS INT) * CAST(e.unit_price AS DOUBLE) ELSE 0 END), 2) as total_spend
FROM workspace.default.bronze_clickstream_events e
JOIN workspace.default.dim_users u
  ON e.user_id = u.user_id
GROUP BY u.user_id, u.email
"""

benchmarker.run_benchmark(
    query_sql=dr_failover_query, 
    test_name="Disaster Recovery - Secondary Region Parity Check"
)


# Benchmark Analysis: Understanding the Two Tests
# Test 1: Liquid Clustering Pruning Test
# Purpose: Validates the effectiveness of Databricks Liquid Clustering for query performance optimization.

# What it does:

# Queries the bronze_clickstream_events table with filters on clustered columns (product_id and event_type)
# Specifically targets 3 products (PROD_10, PROD_25, PROD_50) and one event type (purchase)
# Performs aggregations: counts unique sessions, sums quantities, and averages prices
# Key Metrics to Watch:

# Files Pruned %: Should be HIGH (60%+) because Liquid Clustering organizes data by product_id and event_type, allowing Spark to skip irrelevant files
# Bytes Scanned: Should be LOW relative to total table size
# Execution Time: Should be FAST due to efficient file pruning
# Success Indicator: High file pruning percentage means Liquid Clustering is working effectively!

# Test 2: Disaster Recovery Validation
# Purpose: Simulates a DR (Disaster Recovery) scenario to verify cross-table data integrity and completeness.

# What it does:

# Performs a JOIN between bronze_clickstream_events and dim_users tables
# Aggregates per-user metrics: total events, latest event timestamp, and total spend
# Tests that data relationships are intact across tables (e.g., after a regional failover)
# Key Metrics to Watch:

# Files Pruned %: Typically LOWER than Test 1 because it's a full table scan with JOIN
# Bytes Scanned: HIGHER than Test 1 (scanning both tables)
# Execution Time: SLOWER due to JOIN and no targeted filtering on clustered columns
# Success Indicator: Query completes successfully with accurate aggregations, proving data integrity across tables.

# Liquid Clustering may not be enabled or optimized on the table
# Consider running OPTIMIZE <table> WHERE <condition> with CLUSTER BY
# If Test 2 is significantly slower than expected:

# Check if dim_users table has appropriate partitioning or indexing
# Verify broadcast join hints if dim_users is small
# Ensure network connectivity between regions for DR scenarios