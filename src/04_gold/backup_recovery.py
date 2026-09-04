"""
Backup & Recovery for Silver and Gold tables using DEEP CLONE.

Usage:
    pipeline = BackupPipeline()
    pipeline.create()           # Initial deep clone of all tables
    pipeline.sync()             # Incremental sync (scheduled job)
    pipeline.verify()           # Check row counts and schema match
    pipeline.list_backups()     # Show backup status
    pipeline.restore_all()      # Disaster recovery from backup

    # Delta Time Travel:
    pipeline.history("silver_clickstream")           # Show version history
    # pipeline.rollback_to_version("silver_clickstream", 0)
    # pipeline.rollback_to_timestamp("silver_clickstream", "2025-01-01")

    # Streaming (continuous, append-only -- captures new inserts, not updates/deletes):
    pipeline.start_streaming()
    pipeline.stop_streaming()
"""

from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Tables to back up, grouped by layer
TABLES: Dict[str, List[str]] = {
    "silver": ["silver_clickstream"],
    "gold": ["gold_daily_user_metrics", "gold_product_daily_metrics"],
}

BACKUP_SCHEMA = "workspace.backup"
CHECKPOINT_BASE = "/Volumes/workspace/default/raw_data/_checkpoints/streaming_backup"


class BackupPipeline:
    """DEEP CLONE backup and recovery for Silver and Gold tables."""

    def __init__(self, tables: Optional[Dict[str, List[str]]] = None):
        self.spark = SparkSession.builder.getOrCreate()
        self.tables = tables or TABLES
        self._streams: Dict[str, object] = {}  # table_name -> StreamingQuery

    # ── helpers ──────────────────────────────────────────────────────

    def _src(self, table: str) -> str:
        return f"workspace.default.{table}"

    def _bak(self, table: str) -> str:
        return f"{BACKUP_SCHEMA}.{table}"

    def _exists(self, table: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE TABLE {table}")
            return True
        except Exception:
            return False

    def _count(self, table: str) -> int:
        try:
            return self.spark.table(table).count()
        except Exception:
            return -1

    def _columns(self, table: str) -> set:
        """Return column names via SQL to avoid per-iteration Analyze RPCs."""
        rows = self.spark.sql(f"DESCRIBE TABLE {table}").collect()
        return {r[0] for r in rows if r[0] and not r[0].startswith("#")}

    def _all_tables(self):
        for layer, tables in self.tables.items():
            for table in tables:
                yield table, layer

    # ── 1. Create initial DEEP CLONE backup ──────────────────────────

    def create(self) -> None:
        """Initial DEEP CLONE of all tables. Skips tables that already have a backup."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BACKUP_SCHEMA}")

        for table, layer in self._all_tables():
            src, bak = self._src(table), self._bak(table)
            if not self._exists(src):
                print(f"  SKIP  {table} — source does not exist")
            elif self._exists(bak):
                print(f"  SKIP  {table} — backup already exists (use sync())")
            else:
                self.spark.sql(f"CREATE TABLE {bak} DEEP CLONE {src}")
                print(f"  OK    {table} — cloned ({self._count(bak)} rows)")

    # ── 2. Incremental sync ───────────────────────────────────────────

    def sync(self) -> None:
        """Incremental DEEP CLONE sync — only copies new/changed Delta files."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BACKUP_SCHEMA}")

        for table, layer in self._all_tables():
            src, bak = self._src(table), self._bak(table)
            if not self._exists(src):
                print(f"  SKIP  {table} — source does not exist")
                continue
            self.spark.sql(f"CREATE OR REPLACE TABLE {bak} DEEP CLONE {src}")
            print(f"  OK    {table} — synced (source={self._count(src)}, backup={self._count(bak)})")

    # ── 3. Restore from backup ────────────────────────────────────────

    def restore(self, table: str) -> None:
        """Restore a single table from its backup clone."""
        src, bak = self._src(table), self._bak(table)
        if not self._exists(bak):
            print(f"  FAIL  {table} — no backup exists")
            return
        self.spark.sql(f"CREATE OR REPLACE TABLE {src} DEEP CLONE {bak}")
        print(f"  OK    {table} — restored ({self._count(src)} rows)")

    def restore_all(self) -> None:
        """Restore all tables that have backups."""
        for table, layer in self._all_tables():
            if self._exists(self._bak(table)):
                self.restore(table)
            else:
                print(f"  SKIP  {table} — no backup exists")

    # ── 4. Verify backup integrity ─────────────────────────────────────

    def verify(self) -> None:
        """Compare row counts and schema between source and backup."""
        for table, layer in self._all_tables():
            src, bak = self._src(table), self._bak(table)
            if not self._exists(src) or not self._exists(bak):
                print(f"  SKIP  {table} — source or backup missing")
                continue

            src_rows, bak_rows = self._count(src), self._count(bak)
            src_cols = self._columns(src)
            bak_cols = self._columns(bak)

            if src_rows == bak_rows and src_cols == bak_cols:
                print(f"  OK    {table} — rows match ({src_rows}), schema match")
            elif src_rows == bak_rows:
                print(f"  FAIL  {table} — rows match but schema differs")
            elif src_cols == bak_cols:
                print(f"  FAIL  {table} — schema match but rows differ (src={src_rows}, bak={bak_rows})")
            else:
                print(f"  FAIL  {table} — both rows and schema differ")

    # ── 5. List backups ───────────────────────────────────────────────

    def list_backups(self) -> None:
        """Show all backup tables with row counts and last sync time."""
        for table, layer in self._all_tables():
            bak = self._bak(table)
            if not self._exists(bak):
                print(f"  [--] {table} [{layer}] — no backup")
                continue
            rows = self._count(bak)
            try:
                ts = self.spark.sql(f"DESCRIBE HISTORY {bak}").select(F.col("timestamp").cast("string")).limit(1).collect()[0][0]
            except Exception:
                ts = "unknown"
            print(f"  [OK] {table} [{layer}] — rows={rows}, last_sync={ts}")

    # -- 6. Delta Time Travel ------------------------------------------

    def history(self, table: str) -> None:
        """Show Delta version history (version, timestamp, operation) for a table.

        Accepts a short name (e.g. 'silver_clickstream') or a full path.
        """
        full = table if "." in table else self._src(table)
        print(f"\n  --- {table} ---")
        self.spark.sql(f"DESCRIBE HISTORY {full}").select(
            "version", "timestamp", "operation"
        ).show(truncate=False)

    def rollback_to_version(self, table: str, version: int) -> str:
        """Generate a RESTORE TABLE SQL statement to roll back to a specific Delta version.

        Returns the SQL string for manual execution. Does NOT auto-run it,
        since RESTORE TABLE is a destructive operation that reverts data.
        """
        full = table if "." in table else self._src(table)
        sql = f"RESTORE TABLE {full} TO VERSION AS OF {version}"
        print(f"  REVIEW {table} -- run this SQL to roll back to version {version}:")
        print(f"    {sql}")
        return sql

    def rollback_to_timestamp(self, table: str, timestamp: str) -> str:
        """Generate a RESTORE TABLE SQL statement to roll back to a timestamp.

        Accepts ISO 8601 strings like '2025-01-01' or '2025-01-01T12:00:00Z'.
        Returns the SQL string for manual execution. Does NOT auto-run it.
        """
        full = table if "." in table else self._src(table)
        sql = f"RESTORE TABLE {full} TO TIMESTAMP AS OF '{timestamp}'"
        print(f"  REVIEW {table} -- run this SQL to roll back to {timestamp}:")
        print(f"    {sql}")
        return sql

    # -- 7. Streaming incremental backup --------------------------------─

    def start_streaming(self) -> None:
        """
        Start continuous streaming backups. Creates a DEEP CLONE baseline if needed,
        then streams only new rows (append-only — does not capture updates/deletes).
        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BACKUP_SCHEMA}")

        for table, layer in self._all_tables():
            src, bak = self._src(table), self._bak(table)
            ckpt = f"{CHECKPOINT_BASE}/{table}"

            if not self._exists(src):
                print(f"  SKIP  {table} — source does not exist")
                continue
            if table in self._streams:
                print(f"  SKIP  {table} — stream already active")
                continue

            # Create baseline if backup doesn't exist yet
            if not self._exists(bak):
                self.spark.sql(f"CREATE TABLE {bak} DEEP CLONE {src}")
                print(f"  Baseline clone created for {table} ({self._count(bak)} rows)")

            # Stream only new rows (startingVersion=latest skips the baseline data)
            query = (
                self.spark.readStream
                .option("startingVersion", "latest")
                .table(src)
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", ckpt)
                .queryName(f"backup_stream_{table}")
                .toTable(bak)
            )
            self._streams[table] = query
            print(f"  OK    {table} — streaming started (ckpt: {ckpt})")

    def stop_streaming(self) -> None:
        """Stop all active streaming backup queries."""
        for table, query in list(self._streams.items()):
            query.stop()
            del self._streams[table]
            print(f"  OK    {table} — streaming stopped")
        if not self._streams:
            print("  No active streams.")

    def list_streams(self) -> None:
        """Show status of active streaming backup queries."""
        if not self._streams:
            print("  No active streams.")
            return
        for table, query in self._streams.items():
            status = query.status
            print(f"  [ACTIVE] {table} — {status.get('message', 'N/A')}")


if __name__ == "__main__":
    pipeline = BackupPipeline()

    # 1. Create initial backups
    pipeline.create()

    # 2. Verify integrity
    pipeline.verify()

    # 3. List all backups
    pipeline.list_backups()

    # 4. Incremental sync (schedule this as a recurring job)
    pipeline.sync()

    # 5. Disaster recovery
    # pipeline.restore_all()

    # 6. Delta Time Travel
    pipeline.history("silver_clickstream")
    pipeline.rollback_to_version("silver_clickstream", 0)
    # pipeline.rollback_to_timestamp("silver_clickstream", "2025-01-01")

    # 7. Streaming incremental (continuous, append-only)
    # pipeline.start_streaming()
    # pipeline.list_streams()
    # pipeline.stop_streaming()
