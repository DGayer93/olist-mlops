import time
from datetime import datetime, timezone
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, TimestampType, DoubleType)

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("layer", StringType()),
    StructField("table_name", StringType()), StructField("source_file", StringType()),
    StructField("status", StringType()), StructField("row_count", LongType()),
    StructField("started_at", TimestampType()), StructField("finished_at", TimestampType()),
    StructField("duration_s", DoubleType()), StructField("error", StringType()),
])


class AuditLogger:
    """Executa passos de carga, mede e registra o resultado em <catalog>.ops.ingestion_log."""

    def __init__(self, spark, catalog: str, layer: str, run_id: str):
        self.spark, self.catalog, self.layer, self.run_id = spark, catalog, layer, run_id
        self.records = []
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.ops.ingestion_log (
                run_id STRING, layer STRING, table_name STRING, source_file STRING,
                status STRING, row_count BIGINT, started_at TIMESTAMP,
                finished_at TIMESTAMP, duration_s DOUBLE, error STRING)""")

    def run(self, table_name: str, source: str, fn):
        """fn() deve gravar a tabela <catalog>.<layer>.<table_name>."""
        started, t0 = datetime.now(timezone.utc), time.time()
        try:
            fn()
            rows = self.spark.table(f"{self.catalog}.{self.layer}.{table_name}").count()
            status, error = "SUCCESS", None
        except Exception as e:
            rows, status, error = None, "FAILED", str(e)[:2000]
        self.records.append((self.run_id, self.layer, table_name, source, status, rows,
                             started, datetime.now(timezone.utc),
                             round(time.time() - t0, 2), error))
        print(f"[{status}] {self.layer}.{table_name}: {rows} linhas")

    def flush(self):
        """Grava o log e falha se algum passo falhou."""
        if self.records:
            (self.spark.createDataFrame(self.records, LOG_SCHEMA)
                 .write.mode("append")
                 .saveAsTable(f"{self.catalog}.ops.ingestion_log"))
        failed = [r[2] for r in self.records if r[4] == "FAILED"]
        if failed:
            raise Exception(f"Falha em {self.layer}: {failed}")