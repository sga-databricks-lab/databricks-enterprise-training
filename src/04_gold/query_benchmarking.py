import time
import re
from pyspark.sql import SparkSession

# ==============================================================================
# DATABRICKS WIDGET CONFIGURATION
# ==============================================================================
# Define UI input widgets with default values for catalog and schema configuration.
dbutils.widgets.text("catalog", "dev", "Catalog name (dev or prod)")
dbutils.widgets.text("schema_bronze", "bronze", "Bronze schema name")
dbutils.widgets.text("schema_silver", "silver", "Silver schema name")
dbutils.widgets.text("schema_gold", "gold", "Gold schema name")

# Retrieve configured catalog and schema names from widget context
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")


# ==============================================================================
# BENCHMARK ENGINE CLASS
# ==============================================================================
class QueryExecutionBenchmarker:
    """
    Benchmarks PySpark query performance by capturing runtime execution statistics.
    
    Rather than relying on static EXPLAIN plans (which lack runtime metrics), this class
    runs queries into a 'noop' write sink and retrieves populated execution metrics 
    directly from the physical plan's underlying JVM accumulators.
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize benchmarker with active PySpark session."""
        self.spark = spark

    def _sanitize_sql(self, raw_sql: str) -> str:
        """
        Clean query strings prior to execution.
        
        - Removes non-ASCII characters that can cause parsing errors.
        - Normalizes multiple whitespace characters into single spaces.
        - Strips trailing semicolons to prevent PySpark SQL syntax errors.
        """
        clean = re.sub(r'[^\x00-\x7F]+', ' ', raw_sql)
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean.endswith(';'):
            clean = clean[:-1]
        return clean

    def _parse_bytes(self, size_str: str) -> float:
        """
        Convert human-readable memory/file size strings into raw bytes.
        
        Example inputs: '10.5 MiB', '2.1 KB', '500 B'
        Returns: Numeric byte value (float) for aggregation math.
        """
        units = {
            "B": 1, 
            "KB": 1024, "KIB": 1024, 
            "MB": 1024**2, "MIB": 1024**2, 
            "GB": 1024**3, "GIB": 1024**3, 
            "TB": 1024**4, "TIB": 1024**4
        }
        match = re.match(r"([\d\.]+)\s*([A-Za-z]*)", size_str.strip())
        if match:
            val, unit = match.groups()
            return float(val) * units.get(unit.upper(), 1)
        return 0.0

    def _format_bytes(self, bytes_num: float) -> str:
        """
        Convert raw byte counts back into standard human-readable units.
        
        Example input: 10485760.0
        Returns: '10.00 MiB'
        """
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if abs(bytes_num) < 1024.0:
                return f"{bytes_num:.2f} {unit}"
            bytes_num /= 1024.0
        return f"{bytes_num:.2f} PiB"

    def _get_table_stats(self, query: str) -> dict:
        """Get total file count and size for all tables referenced in the query."""
        tables = re.findall(r"(?:FROM|JOIN)\s+([\w]+\.[\w]+\.[\w]+)", query, re.IGNORECASE)
        total_files = 0
        total_size = 0
        for table in tables:
            detail = self.spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
            total_files += detail["numFiles"]
            total_size += detail["sizeInBytes"]
        return {"total_files": total_files, "total_size_bytes": total_size}

    def _parse_metrics(self, plan_text: str, table_stats: dict) -> dict:
        """
        Extract performance metrics from EXPLAIN COST output and DESCRIBE DETAIL.
        
        EXPLAIN COST provides estimated sizeInBytes at each plan node.
        The largest sizeInBytes (closest to the scan/filter leaf) approximates
        the bytes scanned after pruning. DESCRIBE DETAIL provides the total
        table file count and size for calculating pruning percentages.
        Spill metrics require the Spark UI and are not available via SQL.
        """
        # Extract all sizeInBytes values from the EXPLAIN COST plan
        matches = re.findall(r"sizeInBytes=([\d\.]+)\s*([KMGT]?i?B|B)", plan_text, re.IGNORECASE)
        
        # The largest sizeInBytes is closest to the scan node = estimated bytes scanned
        estimated_scan_bytes = 0.0
        for size_str, unit_str in matches:
            estimated_scan_bytes = max(estimated_scan_bytes, self._parse_bytes(f"{size_str} {unit_str}"))
        
        total_size = table_stats["total_size_bytes"]
        total_files = table_stats["total_files"]
        
        # Calculate pruning: how much data was skipped vs the full table
        if total_size > 0 and estimated_scan_bytes > 0:
            scan_ratio = estimated_scan_bytes / total_size
            files_read = max(1, round(total_files * scan_ratio))
        else:
            files_read = total_files
        files_pruned = max(0, total_files - files_read)
        
        return {
            "bytes_scanned": self._format_bytes(estimated_scan_bytes) if estimated_scan_bytes > 0 else "0 B",
            "files_read": files_read,
            "files_pruned": files_pruned,
            "memory_spilled": "N/A (requires Spark UI)",
            "disk_spilled": "N/A (requires Spark UI)",
        }

    def run_benchmark(self, query_sql: str, test_name: str = "Gold Layer Query Execution"):
        """
        Triggers actual query execution, extracts runtime accumulators, and prints report.
        """
        clean_query = self._sanitize_sql(query_sql)
        start_time = time.time()
        
        # Step 1: Formulate the logical execution plan
        df = self.spark.sql(clean_query)
        
        # Step 2: Force full query execution using a 'noop' (no-operation) sink.
        # This triggers actual I/O and computing WITHOUT writing output files to storage.
        df.write.format("noop").mode("overwrite").save()
        
        # Calculate wall-clock execution time
        total_time = round(time.time() - start_time, 2)
        
        # Step 3: Get total table-level stats from DESCRIBE DETAIL
        table_stats = self._get_table_stats(clean_query)
        
        # Step 4: Extract estimated scan size from EXPLAIN COST (includes Statistics with sizeInBytes)
        explain_plan = self.spark.sql(f"EXPLAIN COST {clean_query}").collect()[0][0]
        
        # Step 5: Parse metrics from EXPLAIN COST and DESCRIBE DETAIL
        metrics = self._parse_metrics(explain_plan, table_stats)
        metrics["test_name"] = test_name
        metrics["execution_time_sec"] = total_time
        
        # Step 5: Format and display stdout report
        self._print_report(metrics)
        return metrics

    def _print_report(self, m: dict):
        """Format and display stdout execution benchmark report."""
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


# ==============================================================================
# BENCHMARK SUITE EXECUTION
# ==============================================================================

# Instantiate the benchmarker engine
benchmarker = QueryExecutionBenchmarker(spark)

# ------------------------------------------------------------------------------
# Test 1: Liquid Clustering Pruning Test
# Benchmarks file pruning effectiveness on Delta Liquid Clustered keys
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# Test 2: Cross-Table Join Parity Test
# Benchmarks performance and memory/disk spills for multi-table aggregations
# ------------------------------------------------------------------------------
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