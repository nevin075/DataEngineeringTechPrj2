"""
spark_utils.py -- shared helpers for the PySpark jobs (spark_cleanse.py, spark_analytics.py).

    * load_config / get_spark      SparkSession built from config/spark_config.json
    * get_logger                   timestamped logging to console (-> Airflow task log) and a log file
    * RunPaths                     one place that knows where every file of a run lives
    * CleansingTracker             counts records in/out and records dropped per reason, keeps rejects
    * normalize_string / parse_*   reusable column expressions for cleansing
    * write_summary_report         builds data/reports/<run_id>/cleansing_summary.csv
"""
from __future__ import annotations

import csv
import glob
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_CONFIG_PATH = os.getenv("PIPELINE_CONFIG_PATH", "/opt/airflow/config/spark_config.json")
DEFAULT_DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")

# Date / timestamp formats found in the raw data (see generate_data.py)
DATE_FORMATS = ["yyyy-MM-dd", "MM/dd/yyyy", "MMM d, yyyy"]
TIMESTAMP_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "MM/dd/yyyy HH:mm",
                     "MM/dd/yyyy", "MMM d, yyyy", "yyyy-MM-dd"]


# --------------------------------------------------------------------------- config / session
def load_config(path: str | None = None) -> dict:
    path = path or DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def get_spark(app_name: str, config: dict) -> SparkSession:
    """Create (or reuse) a SparkSession. The master URL comes from spark-submit --master;
    when the script is run with plain `python`, PySpark falls back to local[*]."""
    builder = SparkSession.builder.appName(f"{config.get('app_name_prefix', 'pipeline')}_{app_name}")
    for key, value in config.get("spark_conf", {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- logging
def get_logger(name: str, log_dir: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"), encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    logger.propagate = False
    return logger


# --------------------------------------------------------------------------- paths
@dataclass(frozen=True)
class RunPaths:
    data_dir: str
    run_id: str

    @property
    def raw_dir(self) -> str:
        return os.path.join(self.data_dir, "raw", self.run_id)

    def raw(self, filename: str) -> str:
        return os.path.join(self.raw_dir, filename)

    def cleaned(self, entity: str) -> str:
        return os.path.join(self.data_dir, "cleaned", self.run_id, entity)

    def rejected(self, entity: str) -> str:
        return os.path.join(self.data_dir, "rejected", self.run_id, entity)

    def analytics(self, metric: str) -> str:
        return os.path.join(self.data_dir, "analytics", self.run_id, metric)

    def analytics_csv(self, metric: str) -> str:
        return os.path.join(self.data_dir, "analytics", self.run_id, "csv", metric)

    @property
    def reports_dir(self) -> str:
        return os.path.join(self.data_dir, "reports", self.run_id)

    @property
    def logs_dir(self) -> str:
        return os.path.join(self.data_dir, "logs", self.run_id)


RAW_FILES = {
    "customers": "customers.csv",
    "products": "products.json",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
}


# --------------------------------------------------------------------------- column helpers
def normalize_string(col: str | Column) -> Column:
    """Trim all whitespace (spaces AND tabs -- F.trim only removes spaces), collapse inner
    runs of whitespace to one space, and turn empty strings into NULL."""
    c = F.col(col) if isinstance(col, str) else col
    cleaned = F.regexp_replace(F.regexp_replace(c, r"\s+", " "), r"^ | $", "")
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def parse_date(col: str | Column) -> Column:
    """Try every known date format; first one that parses wins (NULL if none do).
    Requires spark.sql.legacy.timeParserPolicy=CORRECTED (set in spark_config.json)."""
    c = F.col(col) if isinstance(col, str) else col
    return F.coalesce(*[F.to_date(c, fmt) for fmt in DATE_FORMATS])


def parse_timestamp(col: str | Column) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.coalesce(*[F.to_timestamp(c, fmt) for fmt in TIMESTAMP_FORMATS])


def parse_amount(col: str | Column, scale: str = "decimal(12,2)") -> Column:
    """'$1,234.50 ' -> 1234.50 ; garbage -> NULL."""
    c = F.col(col) if isinstance(col, str) else col
    return F.regexp_replace(c, r"[\s$,]", "").cast(scale)


FLAG_PREFIX = "_flag__"


def add_flag(df: DataFrame, condition: Column, flag: str) -> DataFrame:
    """Record that a value was *fixed* (record kept but changed) -- as opposed to a reject
    (record dropped). Each flag is its own helper column holding the flag name or NULL;
    collect_flags() combines them once at the end.

    Why not append to one array column every time? Because each append would reference the
    previous array twice, and Spark's optimizer inlines projections -> the expression tree
    doubles with every flag (2^20 nodes after 20 flags = driver OOM)."""
    col_name = FLAG_PREFIX + flag
    new = F.when(F.coalesce(condition, F.lit(False)), F.lit(flag))
    if col_name in df.columns:          # same flag raised by another column -> OR them
        new = F.coalesce(F.col(col_name), new)
    return df.withColumn(col_name, new)


def collect_flags(df: DataFrame) -> DataFrame:
    """Combine all _flag__* columns into one array column `_dq_flags` and drop the helpers."""
    flag_cols = [c for c in df.columns if c.startswith(FLAG_PREFIX)]
    if not flag_cols:
        return df.withColumn("_dq_flags", F.array().cast("array<string>"))
    return df.withColumn("_dq_flags", F.array_compact(F.array(*flag_cols))).drop(*flag_cols)


def completeness_score(cols: list[str]) -> Column:
    """Number of non-null columns -- used to keep the 'most complete' near-duplicate."""
    return sum((F.col(c).isNotNull().cast("int") for c in cols), F.lit(0))


# --------------------------------------------------------------------------- statistics
class CleansingTracker:
    """Tracks records in / out / dropped-per-reason for one entity and collects the rejected
    rows (as JSON) so they can be written to data/rejected/<run_id>/<entity>/ for auditing."""

    def __init__(self, entity: str, logger: logging.Logger):
        self.entity = entity
        self.logger = logger
        self.records_in = 0
        self.records_out = 0
        self.dropped: dict[str, int] = {}
        self.flags: dict[str, int] = {}
        self._rejects: list[DataFrame] = []
        self.started = datetime.now(timezone.utc)

    def reject(self, df: DataFrame, condition: Column, reason: str) -> DataFrame:
        """Split df into (kept, rejected) on condition. NULL conditions count as 'keep'."""
        cond = F.coalesce(condition, F.lit(False))
        # localCheckpoint() materializes the step and truncates the lineage. With plain
        # .cache() the query plan keeps every previous step and grows with each reject,
        # until building the plan text alone exhausts the driver heap.
        marked = df.withColumn("_reject", cond).localCheckpoint(eager=True)
        bad = marked.filter(F.col("_reject")).drop("_reject")
        n_bad = bad.count()
        self.dropped[reason] = self.dropped.get(reason, 0) + n_bad
        if n_bad:
            # keep real data columns (+ the raw text of malformed lines), skip helper columns
            data_cols = [c for c in bad.columns if not c.startswith("_") or c == "_corrupt_record"]
            self._rejects.append(bad.select(
                F.lit(self.entity).alias("entity"),
                F.lit(reason).alias("reject_reason"),
                F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in data_cols])).alias("record"),
            ))
        self.logger.info("  [%s] %-28s dropped %8s", self.entity, reason, f"{n_bad:,}")
        return marked.filter(~F.col("_reject")).drop("_reject")

    def count_flags(self, df: DataFrame):
        rows = (df.select(F.explode("_dq_flags").alias("flag")).groupBy("flag").count().collect())
        self.flags = {r["flag"]: int(r["count"]) for r in sorted(rows, key=lambda r: r["flag"])}

    def write_rejects(self, path: str):
        if not self._rejects:
            return
        out = self._rejects[0]
        for df in self._rejects[1:]:
            out = out.unionByName(df)
        out.coalesce(1).write.mode("overwrite").parquet(path)

    @property
    def records_dropped(self) -> int:
        return sum(self.dropped.values())

    def to_dict(self) -> dict:
        return {
            "entity": self.entity,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "records_dropped": self.records_dropped,
            "dropped_by_reason": self.dropped,
            "records_flagged_by_fix": self.flags,
            "started_at": self.started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    def log_summary(self):
        self.logger.info("=" * 64)
        self.logger.info("CLEANSING SUMMARY: %s", self.entity.upper())
        self.logger.info("  records in      : %10s", f"{self.records_in:,}")
        self.logger.info("  records out     : %10s", f"{self.records_out:,}")
        self.logger.info("  records dropped : %10s", f"{self.records_dropped:,}")
        for reason, n in self.dropped.items():
            self.logger.info("      - %-26s %8s", reason, f"{n:,}")
        if self.flags:
            self.logger.info("  values fixed (record kept):")
            for flag, n in self.flags.items():
                self.logger.info("      - %-26s %8s", flag, f"{n:,}")
        self.logger.info("=" * 64)

    def write_stats(self, reports_dir: str):
        os.makedirs(reports_dir, exist_ok=True)
        with open(os.path.join(reports_dir, f"cleanse_{self.entity}.json"), "w", encoding="utf-8") as fh:
            json.dump(self.to_dict(), fh, indent=2)


ENTITY_ORDER = ["customers", "products", "orders", "order_items"]


def write_summary_report(reports_dir: str) -> str:
    """Combine every cleanse_<entity>.json of the run into cleansing_summary.csv."""
    stats = {}
    for path in glob.glob(os.path.join(reports_dir, "cleanse_*.json")):
        with open(path, encoding="utf-8") as fh:
            s = json.load(fh)
        stats[s["entity"]] = s
    out = os.path.join(reports_dir, "cleansing_summary.csv")
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["entity", "records_in", "records_out", "records_dropped", "pct_dropped",
                    "dropped_by_reason", "values_fixed"])
        for entity in sorted(stats, key=lambda e: ENTITY_ORDER.index(e) if e in ENTITY_ORDER else 99):
            s = stats[entity]
            pct = 100.0 * s["records_dropped"] / s["records_in"] if s["records_in"] else 0.0
            w.writerow([entity, s["records_in"], s["records_out"], s["records_dropped"], f"{pct:.2f}",
                        "; ".join(f"{k}={v}" for k, v in s["dropped_by_reason"].items() if v),
                        "; ".join(f"{k}={v}" for k, v in s["records_flagged_by_fix"].items())])
    return out
