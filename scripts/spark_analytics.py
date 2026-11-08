#!/usr/bin/env python3
"""
spark_analytics.py -- business analytics on the cleansed data (DataFrame API + Spark SQL).

    spark-submit --master spark://spark-master:7077 --py-files spark_utils.py \
        spark_analytics.py --run-id 20261107T060000

Reads   data/cleaned/<run_id>/{customers,products,orders,order_items}   (Parquet)
Writes  data/analytics/<run_id>/<metric>/        Parquet (loaded into PostgreSQL)
        data/analytics/<run_id>/csv/<metric>.csv one CSV per metric (easy to open in Excel)
        data/reports/<run_id>/analytics_summary.json, analytics_join_plan.txt

Revenue definition: net_amount = total_amount - discount_amount (shipping excluded).
Product/category revenue: each order's net_amount is split over its line items in proportion
to line_total, so product revenue adds up to order revenue (for orders that have items).

Optimizations (bonus): cached base tables, explicit broadcast joins for the small dimension
tables, 16 shuffle partitions instead of the default 200, adaptive query execution (config).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from spark_utils import DEFAULT_DATA_DIR, RunPaths, get_logger, get_spark, load_config


def money(c) -> F.Column:
    return F.round(c, 2).cast("decimal(14,2)")


def pct(c) -> F.Column:
    return F.round(c, 2).cast("decimal(7,2)")


# --------------------------------------------------------------------------- load + prepare
def load_tables(spark: SparkSession, paths: RunPaths) -> dict[str, DataFrame]:
    tables = {}
    for name in ["customers", "products", "orders", "order_items"]:
        try:
            tables[name] = spark.read.parquet(paths.cleaned(name)).drop("dq_flags").cache()
        except AnalysisException as exc:
            raise FileNotFoundError(f"cleaned {name} not found at {paths.cleaned(name)}") from exc
        tables[name].createOrReplaceTempView(name)
    return tables


def build_order_lines(t: dict[str, DataFrame]) -> DataFrame:
    """One row per order line, enriched with order, customer and product attributes, and the
    line's share of the order's net revenue."""
    items, orders = t["order_items"], t["orders"]
    w = Window.partitionBy("order_id")
    lines = (items
             .join(orders.select("order_id", "customer_id", "order_date", "net_amount"), "order_id")
             .withColumn("_order_subtotal", F.sum("line_total").over(w))
             .withColumn("line_net_revenue",
                         F.when(F.col("_order_subtotal") > 0,
                                F.col("line_total") / F.col("_order_subtotal") * F.col("net_amount"))
                          .otherwise(F.lit(0)).cast("decimal(14,4)"))
             .drop("_order_subtotal")
             # broadcast joins: products (~100K rows, few columns) and customers fit in memory
             .join(F.broadcast(t["products"].select("product_id", "category", "brand")), "product_id")
             .join(F.broadcast(t["customers"].select("customer_id", "customer_type")), "customer_id"))
    lines = lines.cache()
    lines.createOrReplaceTempView("order_lines")
    return lines


# --------------------------------------------------------------------------- customer analytics
def customer_metrics(spark, t) -> DataFrame:
    """Customer lifetime value, order frequency, average order value (DataFrame API)."""
    per_customer = (t["orders"].groupBy("customer_id")
                    .agg(F.count("order_id").alias("order_count"),
                         F.sum("net_amount").alias("lifetime_value"),
                         F.avg("net_amount").alias("avg_order_value"),
                         F.min(F.to_date("order_date")).alias("first_order_date"),
                         F.max(F.to_date("order_date")).alias("last_order_date")))
    return (t["customers"].select("customer_id", "first_name", "last_name", "customer_type", "city")
            .join(per_customer, "customer_id", "left")
            .select("customer_id", "first_name", "last_name", "customer_type", "city",
                    F.coalesce("order_count", F.lit(0)).cast("int").alias("order_count"),
                    money(F.coalesce("lifetime_value", F.lit(0))).alias("lifetime_value"),
                    money("avg_order_value").alias("avg_order_value"),
                    "first_order_date", "last_order_date"))


def customer_type_distribution(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT customer_type,
               COUNT(*)                                                   AS customer_count,
               CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS DECIMAL(7,2)) AS pct_of_customers
        FROM customers
        GROUP BY customer_type
        ORDER BY customer_count DESC
    """)


def top_customers(cm: DataFrame, n: int) -> DataFrame:
    w = Window.orderBy(F.col("lifetime_value").desc(), F.col("customer_id"))
    return (cm.orderBy(F.col("lifetime_value").desc(), F.col("customer_id")).limit(n)
              .withColumn("revenue_rank", F.row_number().over(w).cast("int"))
              .select("revenue_rank", "customer_id", "first_name", "last_name", "customer_type", "city",
                      "order_count", "lifetime_value", "avg_order_value"))


# --------------------------------------------------------------------------- product analytics
def product_metrics(spark, t, lines) -> DataFrame:
    """Revenue per product, popularity (# orders), units sold, average selling price."""
    agg = (lines.groupBy("product_id")
           .agg(F.countDistinct("order_id").alias("order_count"),
                F.sum("quantity").alias("units_sold"),
                F.sum("line_net_revenue").alias("revenue"),
                F.avg("unit_price").alias("avg_selling_price")))
    return (t["products"].select("product_id", "product_name", "category", "brand", "is_active")
            .join(agg, "product_id", "left")
            .select("product_id", "product_name", "category", "brand", "is_active",
                    F.coalesce("order_count", F.lit(0)).cast("int").alias("order_count"),
                    F.coalesce("units_sold", F.lit(0)).cast("bigint").alias("units_sold"),
                    money(F.coalesce("revenue", F.lit(0))).alias("revenue"),
                    F.round("avg_selling_price", 2).cast("decimal(10,2)").alias("avg_selling_price")))


def category_performance(spark, t) -> DataFrame:
    return spark.sql("""
        WITH cat AS (
            SELECT category,
                   COUNT(DISTINCT order_id)  AS order_count,
                   SUM(quantity)             AS units_sold,
                   SUM(line_net_revenue)     AS revenue,
                   AVG(unit_price)           AS avg_selling_price
            FROM order_lines
            GROUP BY category
        ), prods AS (
            SELECT category, COUNT(*) AS product_count FROM products GROUP BY category
        )
        SELECT p.category,
               p.product_count,
               COALESCE(c.order_count, 0)                                   AS order_count,
               COALESCE(c.units_sold, 0)                                    AS units_sold,
               CAST(ROUND(COALESCE(c.revenue, 0), 2) AS DECIMAL(14,2))      AS revenue,
               CAST(ROUND(c.avg_selling_price, 2) AS DECIMAL(10,2))         AS avg_selling_price,
               CAST(ROUND(COALESCE(c.revenue, 0) * 100.0 / SUM(c.revenue) OVER (), 2) AS DECIMAL(7,2))
                                                                            AS pct_of_revenue
        FROM prods p LEFT JOIN cat c ON p.category = c.category
        ORDER BY revenue DESC
    """)


def inventory_status(spark, t) -> DataFrame:
    """Active vs inactive products (filter + count, as in the spec)."""
    p = t["products"]
    total = p.count()
    rows = []
    for label, cond in [("Active", F.col("is_active")), ("Inactive", ~F.col("is_active"))]:
        subset = p.filter(cond)
        n = subset.count()
        stock = subset.agg(F.sum("stock_quantity")).first()[0] or 0
        rows.append((label, n, int(stock), round(100.0 * n / total, 2) if total else 0.0))
    return (spark.createDataFrame(rows, "status string, product_count long, total_stock_units long, pct double")
            .withColumn("pct_of_products", pct(F.col("pct"))).drop("pct"))


# --------------------------------------------------------------------------- order analytics
def sales_monthly(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT CAST(date_trunc('month', order_date) AS DATE)          AS month,
               COUNT(*)                                               AS order_count,
               CAST(ROUND(SUM(net_amount), 2) AS DECIMAL(14,2))       AS revenue,
               CAST(ROUND(AVG(net_amount), 2) AS DECIMAL(14,2))       AS avg_order_value,
               CAST(ROUND(SUM(discount_amount), 2) AS DECIMAL(14,2))  AS total_discount
        FROM orders
        GROUP BY 1
        ORDER BY 1
    """)


def sales_daily(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT to_date(order_date)                              AS order_day,
               COUNT(*)                                         AS order_count,
               CAST(ROUND(SUM(net_amount), 2) AS DECIMAL(14,2)) AS revenue
        FROM orders
        GROUP BY 1
        ORDER BY 1
    """)


def order_status_breakdown(spark, t) -> DataFrame:
    o = t["orders"]
    total = o.count()
    return (o.groupBy("order_status")
             .agg(F.count("*").alias("order_count"), money(F.sum("net_amount")).alias("revenue"))
             .withColumn("pct_of_orders", pct(F.col("order_count") * 100.0 / F.lit(total)))
             .orderBy(F.col("order_count").desc()))


def payment_method_distribution(spark, t) -> DataFrame:
    o = t["orders"]
    total_rev = o.agg(F.sum("net_amount")).first()[0]
    return (o.groupBy("payment_method")
             .agg(F.count("*").alias("order_count"), F.sum("net_amount").alias("_rev"))
             .select("payment_method", "order_count", money("_rev").alias("revenue"),
                     pct(F.col("_rev") * 100.0 / F.lit(total_rev)).alias("pct_of_revenue"))
             .orderBy(F.col("revenue").desc()))


def discount_summary(spark, t) -> DataFrame:
    """Average discount rate = mean(discount_amount / total_amount) over orders (withColumn + avg)."""
    o = (t["orders"].filter(F.col("total_amount") > 0)
         .withColumn("discount_rate", F.col("discount_amount") / F.col("total_amount") * 100))
    return o.agg(
        F.count("*").cast("long").alias("order_count"),
        F.sum(F.when(F.col("discount_amount") > 0, 1).otherwise(0)).cast("long").alias("orders_with_discount"),
        pct(F.avg(F.when(F.col("discount_amount") > 0, 100.0).otherwise(0.0))).alias("pct_orders_discounted"),
        pct(F.avg("discount_rate")).alias("avg_discount_rate_pct"),
        pct(F.avg(F.when(F.col("discount_amount") > 0, F.col("discount_rate")))).alias("avg_discount_rate_when_discounted_pct"),
        money(F.sum("discount_amount")).alias("total_discount"),
    )


# --------------------------------------------------------------------------- cross-entity analytics
def revenue_by_customer_type(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT /*+ BROADCAST(c) */
               c.customer_type,
               COUNT(DISTINCT o.customer_id)                                 AS customer_count,
               COUNT(*)                                                      AS order_count,
               CAST(ROUND(SUM(o.net_amount), 2) AS DECIMAL(14,2))            AS revenue,
               CAST(ROUND(AVG(o.net_amount), 2) AS DECIMAL(14,2))            AS avg_order_value,
               CAST(ROUND(SUM(o.net_amount) * 100.0 / SUM(SUM(o.net_amount)) OVER (), 2) AS DECIMAL(7,2))
                                                                             AS pct_of_revenue
        FROM orders o JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_type
        ORDER BY revenue DESC
    """)


def top_categories_by_customer_type(spark, t, n: int) -> DataFrame:
    return spark.sql(f"""
        WITH ranked AS (
            SELECT customer_type, category,
                   COUNT(*)              AS order_lines,
                   SUM(quantity)         AS units_sold,
                   SUM(line_net_revenue) AS revenue,
                   ROW_NUMBER() OVER (PARTITION BY customer_type
                                      ORDER BY COUNT(*) DESC, SUM(line_net_revenue) DESC) AS category_rank
            FROM order_lines
            GROUP BY customer_type, category
        )
        SELECT customer_type, CAST(category_rank AS INT) AS category_rank, category, order_lines, units_sold,
               CAST(ROUND(revenue, 2) AS DECIMAL(14,2)) AS revenue
        FROM ranked
        WHERE category_rank <= {int(n)}
        ORDER BY customer_type, category_rank
    """)


def city_revenue(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT /*+ BROADCAST(c) */
               c.city,
               COUNT(DISTINCT o.customer_id)                       AS customer_count,
               COUNT(*)                                            AS order_count,
               CAST(ROUND(SUM(o.net_amount), 2) AS DECIMAL(14,2))  AS revenue,
               CAST(ROUND(AVG(o.net_amount), 2) AS DECIMAL(14,2))  AS avg_order_value
        FROM orders o JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.city
        ORDER BY revenue DESC
    """)


# --------------------------------------------------------------------------- extra analytics (bonus)
def brand_performance(spark, t, lines) -> DataFrame:
    agg = (lines.groupBy("brand")
           .agg(F.countDistinct("product_id").alias("products_sold"),
                F.countDistinct("order_id").alias("order_count"),
                F.sum("quantity").alias("units_sold"),
                money(F.sum("line_net_revenue")).alias("revenue")))
    w = Window.orderBy(F.col("revenue").desc())
    return (agg.withColumn("revenue_rank", F.row_number().over(w).cast("int"))
               .select("revenue_rank", "brand", F.col("products_sold").cast("int"),
                       F.col("order_count").cast("int"), F.col("units_sold").cast("bigint"), "revenue"))


def sales_by_weekday(spark, t) -> DataFrame:
    return spark.sql("""
        SELECT CAST(dayofweek(order_date) AS INT)                     AS weekday_num,
               date_format(order_date, 'EEEE')                        AS weekday,
               COUNT(*)                                               AS order_count,
               CAST(ROUND(SUM(net_amount), 2) AS DECIMAL(14,2))       AS revenue,
               CAST(ROUND(AVG(net_amount), 2) AS DECIMAL(14,2))       AS avg_order_value
        FROM orders
        GROUP BY 1, 2
        ORDER BY 1
    """)


def repeat_customers(spark, cm: DataFrame) -> DataFrame:
    buyers = cm.filter(F.col("order_count") > 0)
    return buyers.agg(
        F.count("*").cast("long").alias("customers_with_orders"),
        F.sum(F.when(F.col("order_count") >= 2, 1).otherwise(0)).cast("long").alias("repeat_customers"),
        F.sum(F.when(F.col("order_count") == 1, 1).otherwise(0)).cast("long").alias("one_time_customers"),
        pct(F.avg(F.when(F.col("order_count") >= 2, 100.0).otherwise(0.0))).alias("repeat_rate_pct"),
        F.round(F.avg("order_count"), 2).cast("decimal(7,2)").alias("avg_orders_per_customer"),
    )


# --------------------------------------------------------------------------- output
def write_metric(df: DataFrame, name: str, paths: RunPaths, log, show: bool = False) -> int:
    df = df.cache()
    rows = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(paths.analytics(name))
    # single, nicely named CSV next to the Parquet output
    tmp = paths.analytics_csv(name) + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    part = glob.glob(os.path.join(tmp, "part-*.csv"))
    if part:
        shutil.move(part[0], paths.analytics_csv(name) + ".csv")
    shutil.rmtree(tmp, ignore_errors=True)
    log.info("  %-34s %8s rows", name, f"{rows:,}")
    if show:
        df.show(10, truncate=False)
    df.unpersist()
    return rows


def run_analytics(spark: SparkSession, paths: RunPaths, cfg: dict, log) -> dict:
    a = cfg.get("analytics", {})
    t = load_tables(spark, paths)
    lines = build_order_lines(t)
    # Save the physical plan of the main join so the broadcast joins can be shown in the report
    os.makedirs(paths.reports_dir, exist_ok=True)
    with open(os.path.join(paths.reports_dir, "analytics_join_plan.txt"), "w", encoding="utf-8") as fh:
        fh.write(lines._jdf.queryExecution().executedPlan().toString())

    cm = customer_metrics(spark, t).cache()
    metrics = {
        # customer
        "customer_metrics": (cm, False),
        "customer_type_distribution": (customer_type_distribution(spark, t), True),
        "top_customers": (top_customers(cm, a.get("top_n_customers", 10)), True),
        # product
        "product_metrics": (product_metrics(spark, t, lines), False),
        "category_performance": (category_performance(spark, t), True),
        "inventory_status": (inventory_status(spark, t), True),
        # order
        "sales_monthly": (sales_monthly(spark, t), True),
        "sales_daily": (sales_daily(spark, t), False),
        "order_status_breakdown": (order_status_breakdown(spark, t), True),
        "payment_method_distribution": (payment_method_distribution(spark, t), True),
        "discount_summary": (discount_summary(spark, t), True),
        # cross-entity
        "revenue_by_customer_type": (revenue_by_customer_type(spark, t), True),
        "top_categories_by_customer_type": (top_categories_by_customer_type(
            spark, t, a.get("top_categories_per_customer_type", 3)), True),
        "city_revenue": (city_revenue(spark, t), True),
        # extra (bonus)
        "brand_performance": (brand_performance(spark, t, lines), False),
        "sales_by_weekday": (sales_by_weekday(spark, t), True),
        "repeat_customers": (repeat_customers(spark, cm), True),
    }
    log.info("Writing %d analytics outputs -> %s", len(metrics), os.path.dirname(paths.analytics("x")))
    row_counts = {name: write_metric(df, name, paths, log, show) for name, (df, show) in metrics.items()}

    kpis = t["orders"].agg(F.count("*").alias("orders"), F.sum("net_amount").alias("revenue"),
                           F.avg("net_amount").alias("aov"), F.countDistinct("customer_id").alias("buyers")).first()
    summary = {
        "run_id": paths.run_id,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "metrics": row_counts,
        "kpis": {"orders": int(kpis["orders"]), "net_revenue": float(kpis["revenue"]),
                 "avg_order_value": round(float(kpis["aov"]), 2), "customers_with_orders": int(kpis["buyers"])},
    }
    with open(os.path.join(paths.reports_dir, "analytics_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    log.info("KPIs: %s", summary["kpis"])
    return summary


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Compute business analytics with Spark.")
    p.add_argument("--run-id", required=True)
    p.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    p.add_argument("--config", default=None)
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    paths = RunPaths(args.data_dir, args.run_id)
    log = get_logger("spark_analytics", paths.logs_dir)
    spark = None
    try:
        cfg = load_config(args.config)
        spark = get_spark("analytics", cfg)
        log.info("Spark %s | master=%s | app=%s | run_id=%s", spark.version, spark.sparkContext.master,
                 spark.sparkContext.applicationId, args.run_id)
        run_analytics(spark, paths, cfg, log)
        return 0
    except FileNotFoundError as exc:
        log.error("Input missing: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        log.exception("Analytics job failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
