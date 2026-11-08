"""
load_postgres.py -- load Spark's Parquet output into PostgreSQL (called by the Airflow
`load_to_postgres` task; can also be run by hand).

    python load_postgres.py --run-id 20261107T060000

Design
  * One transaction for the whole load: TRUNCATE + COPY every table, then COMMIT.
    Any error (connection drop, constraint violation, bad file) -> ROLLBACK, so the warehouse
    always holds the last *complete* good run, never half of one.
  * COPY ... FROM STDIN (PostgreSQL's bulk loader) -- ~10-50x faster than INSERTs.
  * Load order respects foreign keys: customers, products -> orders -> order_items.
  * Connection errors are retried with exponential backoff.
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time

import psycopg2
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg2 import errors, sql

log = logging.getLogger("load_postgres")

DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")
SCHEMA_PATH = os.getenv("PIPELINE_SCHEMA_PATH", "/opt/airflow/sql/schema.sql")

CLEAN_TABLES = ["customers", "products", "orders", "order_items"]          # FK-safe load order
ANALYTICS_TABLES = [
    "customer_metrics", "customer_type_distribution", "top_customers",
    "product_metrics", "category_performance", "inventory_status",
    "sales_monthly", "sales_daily", "order_status_breakdown", "payment_method_distribution",
    "discount_summary",
    "revenue_by_customer_type", "top_categories_by_customer_type", "city_revenue",
    "brand_performance", "sales_by_weekday", "repeat_customers",
]


# --------------------------------------------------------------------------- connections
def conn_params(dbname: str | None = None) -> dict:
    return {
        "host": os.getenv("WAREHOUSE_HOST", "localhost"),
        "port": int(os.getenv("WAREHOUSE_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "dbname": dbname or os.getenv("WAREHOUSE_DB", "retail_dw"),
        "connect_timeout": 10,
        "application_name": "data430_prj2_loader",
    }


def connect(dbname: str | None = None, retries: int = 5, base_delay: float = 2.0):
    """Connect with exponential backoff -- handles the DB still starting or a network blip."""
    params = conn_params(dbname)
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**params)
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                log.error("Could not connect to %s:%s/%s after %d attempts",
                          params["host"], params["port"], params["dbname"], retries)
                raise
            wait = base_delay * 2 ** (attempt - 1)
            log.warning("Connection attempt %d/%d failed (%s) -- retrying in %.0fs",
                        attempt, retries, str(exc).strip(), wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def ensure_database():
    """Create the warehouse database if it does not exist (e.g. WAREHOUSE_DB was renamed)."""
    target = conn_params()["dbname"]
    conn = connect("postgres")
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target,))
            if cur.fetchone() is None:
                log.info("Creating database %s", target)
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target)))
    finally:
        conn.close()


def apply_schema(cur, path: str = SCHEMA_PATH):
    with open(path, encoding="utf-8") as fh:
        cur.execute(fh.read())


# --------------------------------------------------------------------------- copy
def table_columns(cur, qualified: str) -> list[str]:
    schema, table = qualified.split(".") if "." in qualified else ("public", qualified)
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""", (schema, table))
    return [r[0] for r in cur.fetchall()]


def copy_parquet(cur, parquet_dir: str, qualified_table: str) -> int:
    """COPY a Spark Parquet folder into a table. Only columns that exist in both are loaded
    (so DB-side defaults such as loaded_at still apply)."""
    if not os.path.isdir(parquet_dir):
        raise FileNotFoundError(f"Parquet output not found: {parquet_dir}")
    data = pq.read_table(parquet_dir)                 # pyarrow skips _SUCCESS / .crc files
    db_cols = set(table_columns(cur, qualified_table))
    if not db_cols:
        raise RuntimeError(f"table {qualified_table} does not exist -- check sql/schema.sql")
    cols = [c for c in data.column_names if c in db_cols]
    missing = [c for c in data.column_names if c not in db_cols]
    if missing:
        log.warning("  %s: ignoring columns not in the table: %s", qualified_table, missing)
    data = data.select(cols)

    buf = io.BytesIO()
    pacsv.write_csv(data, buf, pacsv.WriteOptions(include_header=False))
    buf.seek(0)
    schema, table = qualified_table.split(".") if "." in qualified_table else ("public", qualified_table)
    stmt = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(
        sql.Identifier(schema), sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, cols)))
    cur.copy_expert(stmt.as_string(cur), buf)
    return data.num_rows


# --------------------------------------------------------------------------- main load
def load_run(run_id: str, data_dir: str = DATA_DIR, schema_path: str = SCHEMA_PATH) -> dict:
    """Load one pipeline run. Returns {table: rows_loaded}. Raises on any failure (after rollback)."""
    cleaned = os.path.join(data_dir, "cleaned", run_id)
    analytics = os.path.join(data_dir, "analytics", run_id)
    ensure_database()
    conn = connect()
    loaded: dict[str, int] = {}
    t0 = time.time()
    try:
        with conn:                                    # commit on success, rollback on exception
            with conn.cursor() as cur:
                apply_schema(cur, schema_path)
                cur.execute("TRUNCATE order_items, orders, products, customers")
                cur.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(
                    sql.SQL("analytics.") + sql.Identifier(t) for t in ANALYTICS_TABLES)))
                for table in CLEAN_TABLES:
                    loaded[table] = copy_parquet(cur, os.path.join(cleaned, table), table)
                    log.info("  loaded %-34s %9s rows", table, f"{loaded[table]:,}")
                for table in ANALYTICS_TABLES:
                    qualified = f"analytics.{table}"
                    loaded[qualified] = copy_parquet(cur, os.path.join(analytics, table), qualified)
                    log.info("  loaded %-34s %9s rows", qualified, f"{loaded[qualified]:,}")
        log.info("Transaction committed: %s rows in %d tables (%.1fs)",
                 f"{sum(loaded.values()):,}", len(loaded), time.time() - t0)
    except errors.IntegrityError as exc:
        diag = exc.diag
        log.error("Constraint violation -> rolled back. constraint=%s table=%s detail=%s",
                  diag.constraint_name, diag.table_name, diag.message_detail)
        raise
    except psycopg2.Error as exc:
        log.error("Database error -> rolled back: %s", str(exc).strip())
        raise
    except Exception:
        log.exception("Load failed -> rolled back")
        raise
    finally:
        conn.close()

    # Refresh planner statistics (outside the transaction; failure here is not fatal)
    try:
        conn = connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in CLEAN_TABLES:
                cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table)))
        conn.close()
    except psycopg2.Error as exc:
        log.warning("ANALYZE skipped: %s", exc)
    return loaded


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Load a run's Parquet output into PostgreSQL")
    p.add_argument("--run-id", required=True)
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--schema", default=SCHEMA_PATH)
    args = p.parse_args(argv)
    try:
        load_run(args.run_id, args.data_dir, args.schema)
        return 0
    except Exception:  # noqa: BLE001
        return 1


if __name__ == "__main__":
    sys.exit(main())
