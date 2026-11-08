"""
pipeline_checks.py -- Python callables used by the Airflow DAG (PythonOperator tasks and
callbacks). Kept free of Airflow imports so every function can be tested on its own.

    validate_raw_files()   task file_validation   files exist, readable, non-empty, right header
    validate_warehouse()   task data_validation   row counts, referential integrity, business rules,
                                                  revenue reconciliation between tables
    record_audit()         task completion + DAG failure callback -> etl_audit table
    send_alert()           task failure callback -> log + optional Slack webhook
"""
from __future__ import annotations

import csv
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

from load_postgres import ANALYTICS_TABLES, CLEAN_TABLES, connect

log = logging.getLogger("pipeline_checks")

EXPECTED_RAW = {
    "customers.csv": ["customer_id", "first_name", "last_name", "email", "phone", "city",
                      "registration_date", "customer_type"],
    "products.json": ["product_id", "product_name", "category", "brand", "unit_price", "stock_quantity",
                      "is_active", "created_date"],
    "orders.csv": ["order_id", "customer_id", "order_date", "order_status", "payment_method",
                   "total_amount", "discount_amount", "shipping_cost"],
    "order_items.csv": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
}


# --------------------------------------------------------------------------- file validation
def validate_raw_files(raw_dir: str, min_rows: int = 100) -> dict:
    """Fail fast before any Spark job starts if an input file is missing or unusable."""
    if not os.path.isdir(raw_dir):
        raise FileNotFoundError(f"raw data folder does not exist: {raw_dir}")
    report, problems = {}, []
    for name, expected_cols in EXPECTED_RAW.items():
        path = os.path.join(raw_dir, name)
        if not os.path.isfile(path):
            problems.append(f"{name}: missing")
            continue
        if not os.access(path, os.R_OK):
            problems.append(f"{name}: not readable")
            continue
        size = os.path.getsize(path)
        if size == 0:
            problems.append(f"{name}: empty file")
            continue
        with open(path, encoding="utf-8") as fh:
            if name.endswith(".csv"):
                header = next(csv.reader(fh))
                if header != expected_cols:
                    problems.append(f"{name}: header {header} != expected {expected_cols}")
                rows = sum(1 for _ in fh)
            else:
                first = json.loads(fh.readline())
                missing = set(expected_cols) - set(first)
                if missing:
                    problems.append(f"{name}: first record lacks keys {sorted(missing)}")
                rows = 1 + sum(1 for _ in fh)
        if rows < min_rows:
            problems.append(f"{name}: only {rows} rows (expected >= {min_rows})")
        report[name] = {"bytes": size, "rows": rows}
        log.info("  %-16s %10s rows  %8.1f MB", name, f"{rows:,}", size / 1e6)
    if problems:
        raise ValueError("raw file validation failed: " + "; ".join(problems))
    return report


# --------------------------------------------------------------------------- warehouse validation
def _read_json(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _scalar(cur, query: str):
    cur.execute(query)
    return cur.fetchone()[0]


def validate_warehouse(run_id: str, data_dir: str) -> list[dict]:
    """Compare PostgreSQL against Spark's own statistics and check integrity rules.
    Raises ValueError listing every failed check; returns the check results otherwise."""
    reports = os.path.join(data_dir, "reports", run_id)
    spark_counts = {e: _read_json(os.path.join(reports, f"cleanse_{e}.json"))["records_out"]
                    for e in CLEAN_TABLES}
    analytics_counts = _read_json(os.path.join(reports, "analytics_summary.json"))["metrics"]
    checks: list[dict] = []

    def check(name, actual, expected, tolerance=0.0):
        a = float(actual or 0)
        e = float(expected or 0)
        ok = abs(a - e) <= tolerance
        checks.append({"check": name, "actual": a, "expected": e, "passed": ok})
        log.info("  [%s] %-52s actual=%-14s expected=%s", "PASS" if ok else "FAIL", name, f"{a:,.2f}", f"{e:,.2f}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            # 1. row counts: database == what Spark wrote
            for table in CLEAN_TABLES:
                check(f"row count {table}", _scalar(cur, f"SELECT COUNT(*) FROM {table}"), spark_counts[table])
            for table in ANALYTICS_TABLES:
                check(f"row count analytics.{table}",
                      _scalar(cur, f"SELECT COUNT(*) FROM analytics.{table}"), analytics_counts.get(table))
            # 2. referential integrity (also enforced by FKs -- checked explicitly for the report)
            check("orphan orders (no customer)", _scalar(cur, """
                SELECT COUNT(*) FROM orders o LEFT JOIN customers c USING (customer_id)
                WHERE c.customer_id IS NULL"""), 0)
            check("orphan order_items (no order)", _scalar(cur, """
                SELECT COUNT(*) FROM order_items i LEFT JOIN orders o USING (order_id)
                WHERE o.order_id IS NULL"""), 0)
            check("orphan order_items (no product)", _scalar(cur, """
                SELECT COUNT(*) FROM order_items i LEFT JOIN products p USING (product_id)
                WHERE p.product_id IS NULL"""), 0)
            # 3. business rules
            check("duplicate customer emails", _scalar(cur, """
                SELECT COUNT(*) FROM (SELECT email FROM customers WHERE email IS NOT NULL
                                      GROUP BY email HAVING COUNT(*) > 1) d"""), 0)
            check("orders with negative amounts", _scalar(cur, """
                SELECT COUNT(*) FROM orders
                WHERE total_amount < 0 OR discount_amount < 0 OR shipping_cost < 0"""), 0)
            check("orders with discount > total",
                  _scalar(cur, "SELECT COUNT(*) FROM orders WHERE discount_amount > total_amount"), 0)
            check("orders dated in the future",
                  _scalar(cur, "SELECT COUNT(*) FROM orders WHERE order_date > now()"), 0)
            check("strings with leading/trailing spaces", _scalar(cur, r"""
                SELECT COUNT(*) FROM customers
                WHERE first_name ~ '^\s|\s$' OR last_name ~ '^\s|\s$' OR city ~ '^\s|\s$'"""), 0)
            # 4. analytics reconcile with the base tables
            revenue = _scalar(cur, "SELECT SUM(net_amount) FROM orders")
            check("CLV total == order revenue",
                  _scalar(cur, "SELECT SUM(lifetime_value) FROM analytics.customer_metrics"), revenue, 0.01)
            check("monthly sales total == order revenue",
                  _scalar(cur, "SELECT SUM(revenue) FROM analytics.sales_monthly"), revenue, 0.01)
            check("monthly order count == orders",
                  _scalar(cur, "SELECT SUM(order_count) FROM analytics.sales_monthly"),
                  _scalar(cur, "SELECT COUNT(*) FROM orders"))
            check("revenue by customer type == order revenue",
                  _scalar(cur, "SELECT SUM(revenue) FROM analytics.revenue_by_customer_type"), revenue, 0.01)
            # product revenue covers orders that still have at least one line item
            revenue_with_items = _scalar(cur, """
                SELECT SUM(net_amount) FROM orders o
                WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.order_id)""")
            check("category revenue == revenue of orders with items",
                  _scalar(cur, "SELECT SUM(revenue) FROM analytics.category_performance"),
                  revenue_with_items, 1.00)
    finally:
        conn.close()

    failed = [c for c in checks if not c["passed"]]
    log.info("Data validation: %d/%d checks passed", len(checks) - len(failed), len(checks))
    if failed:
        raise ValueError("data validation failed: " + "; ".join(
            f"{c['check']} (actual {c['actual']}, expected {c['expected']})" for c in failed))
    return checks


# --------------------------------------------------------------------------- audit
def collect_run_stats(run_id: str, data_dir: str) -> dict:
    """Read the statistics files Spark wrote for this run (missing files are skipped)."""
    reports = os.path.join(data_dir, "reports", run_id)
    out = {"cleansing": {}, "analytics": None}
    for entity in CLEAN_TABLES:
        path = os.path.join(reports, f"cleanse_{entity}.json")
        if os.path.exists(path):
            out["cleansing"][entity] = _read_json(path)
    path = os.path.join(reports, "analytics_summary.json")
    if os.path.exists(path):
        out["analytics"] = _read_json(path)
    return out


def record_audit(*, dag_id: str, run_id: str, data_run_id: str, status: str,
                 start_time: datetime | None, rows_loaded: int | None = None,
                 failed_task: str | None = None, error_message: str | None = None,
                 extra: dict | None = None, data_dir: str | None = None) -> dict:
    """Insert or update this run's row in etl_audit (one row per dag_id + run_id)."""
    end_time = datetime.now(timezone.utc)
    stats = collect_run_stats(data_run_id, data_dir) if data_dir else {"cleansing": {}, "analytics": None}
    cl = stats["cleansing"].values()
    records_raw = sum(s["records_in"] for s in cl) if cl else None
    records_clean = sum(s["records_out"] for s in cl) if cl else None
    records_rejected = sum(s["records_dropped"] for s in cl) if cl else None
    analytics_tables = len(stats["analytics"]["metrics"]) if stats["analytics"] else None
    details = {"cleansing": {e: {k: s[k] for k in ("records_in", "records_out", "records_dropped",
                                                    "dropped_by_reason")}
                             for e, s in stats["cleansing"].items()},
               "kpis": stats["analytics"]["kpis"] if stats["analytics"] else None,
               **(extra or {})}
    duration = (end_time - start_time).total_seconds() if start_time else None
    row = dict(dag_id=dag_id, run_id=run_id, data_run_id=data_run_id, status=status, start_time=start_time,
               end_time=end_time, duration_seconds=duration, records_raw=records_raw,
               records_clean=records_clean, records_rejected=records_rejected, rows_loaded=rows_loaded,
               analytics_tables=analytics_tables, failed_task=failed_task,
               error_message=(error_message or None) and error_message[:4000],
               details=json.dumps(details, default=str))
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            # make sure the table exists even if the run failed before load_to_postgres
            cur.execute("""CREATE TABLE IF NOT EXISTS etl_audit (audit_id SERIAL PRIMARY KEY,
                dag_id VARCHAR(250) NOT NULL, run_id VARCHAR(250) NOT NULL, data_run_id VARCHAR(50),
                status VARCHAR(20) NOT NULL, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
                duration_seconds NUMERIC(10,1), records_raw BIGINT, records_clean BIGINT,
                records_rejected BIGINT, rows_loaded BIGINT, analytics_tables INTEGER,
                failed_task VARCHAR(250), error_message TEXT, details JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT uq_etl_audit_run UNIQUE (dag_id, run_id))""")
            cols = list(row)
            cur.execute(f"""
                INSERT INTO etl_audit ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})
                ON CONFLICT (dag_id, run_id) DO UPDATE SET
                    {", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("dag_id", "run_id"))},
                    updated_at = now()
                RETURNING audit_id""", [row[c] for c in cols])
            row["audit_id"] = cur.fetchone()[0]
    finally:
        conn.close()
    log.info("etl_audit #%s: %s run=%s raw=%s clean=%s rejected=%s loaded=%s duration=%.0fs",
             row["audit_id"], status, run_id, records_raw, records_clean, records_rejected, rows_loaded,
             duration or 0)
    return {k: v for k, v in row.items() if k != "details"}


# --------------------------------------------------------------------------- alerting
def send_alert(message: str) -> None:
    """Always logs; also posts to Slack when SLACK_WEBHOOK_URL is set. Never raises."""
    log.error("ALERT: %s", message)
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        return
    try:
        body = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        log.info("Slack alert sent")
    except Exception as exc:  # noqa: BLE001 -- alerting must never break the pipeline
        log.warning("Slack alert failed: %s", exc)
