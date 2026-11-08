#!/usr/bin/env python3
"""
spark_cleanse.py -- PySpark cleansing + deduplication for customers, products and orders.

Submitted by Airflow with spark-submit, one entity per call:

    spark-submit --master spark://spark-master:7077 --py-files spark_utils.py \
        spark_cleanse.py --entity customers --run-id 20261107T060000

`--entity orders` also cleanses order_items, and needs the cleaned customers and products of
the same run (foreign-key checks), so Airflow runs it after those two jobs.

Order of operations for every entity
    1. read raw file as all-string columns, PERMISSIVE mode -> malformed lines land in _corrupt_record
    2. drop malformed lines                         (reject: malformed_record)
    3. drop exact duplicates                        (reject: exact_duplicate)
    4. normalize strings: trim / collapse whitespace, '' -> NULL
    5. drop NULL / non-numeric primary keys          (reject: null_primary_key, invalid_primary_key)
    6. near-duplicates: same id -> keep the most complete (then most recent) record
    7. standardize formats (case, dates -> ISO, phone, booleans, money)
    8. validate values: reject or fix+flag       (negative amounts, impossible dates, ...)
    9. referential integrity (orders/order_items)  (reject: orphan_customer, orphan_product, orphan_order)
   10. write Parquet + rejected records + statistics JSON + cleansing_summary.csv

Records that are kept but had a value repaired carry a `dq_flags` column, e.g.
"missing_email;city_case_standardized".
"""
from __future__ import annotations

import argparse
import os
import sys
from itertools import chain

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from spark_utils import (DEFAULT_DATA_DIR, RAW_FILES, CleansingTracker, RunPaths, add_flag,
                         collect_flags, completeness_score, get_logger, get_spark, load_config,
                         normalize_string, parse_amount, parse_date, parse_timestamp,
                         write_summary_report)

CUSTOMER_COLS = ["customer_id", "first_name", "last_name", "email", "phone", "city",
                 "registration_date", "customer_type"]
PRODUCT_COLS = ["product_id", "product_name", "category", "brand", "unit_price", "stock_quantity",
                "is_active", "created_date"]
ORDER_COLS = ["order_id", "customer_id", "order_date", "order_status", "payment_method",
              "total_amount", "discount_amount", "shipping_cost"]
ORDER_ITEM_COLS = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
ISO_DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
ISO_TS_RE = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$"
PAYMENT_MAP = {"creditcard": "Credit Card", "debitcard": "Debit Card", "paypal": "PayPal",
               "applepay": "Apple Pay", "giftcard": "Gift Card", "banktransfer": "Bank Transfer"}
STATUS_RANK = {"Pending": 1, "Shipped": 2, "Delivered": 3}


# --------------------------------------------------------------------------- readers
def string_schema(cols: list[str]) -> StructType:
    """Read every column as string; casting happens after cleaning so bad values never crash a job."""
    return StructType([StructField(c, StringType(), True) for c in cols] +
                      [StructField("_corrupt_record", StringType(), True)])


def read_raw(spark: SparkSession, path: str, cols: list[str]) -> DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"raw input not found: {path}")
    reader = (spark.read.schema(string_schema(cols))
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record"))
    if path.endswith(".json"):
        df = reader.json(path)
    else:
        df = reader.option("header", True).option("multiLine", False).csv(path)
    # Spark only lets you query _corrupt_record after the scan is materialized -> cache
    return df.cache()


# --------------------------------------------------------------------------- shared steps
def common_front_steps(df: DataFrame, cols: list[str], pk: str, t: CleansingTracker) -> DataFrame:
    """Steps 2-5: malformed lines, exact duplicates, string normalization, primary key checks."""
    t.records_in = df.count()
    df = t.reject(df, F.col("_corrupt_record").isNotNull(), "malformed_record").drop("_corrupt_record")

    w = Window.partitionBy(*cols).orderBy(F.lit(1))
    df = df.withColumn("_rn", F.row_number().over(w))
    df = t.reject(df, F.col("_rn") > 1, "exact_duplicate").drop("_rn")

    for c in cols:
        df = df.withColumn(c, normalize_string(c))

    df = t.reject(df, F.col(pk).isNull(), "null_primary_key")
    df = df.withColumn("_pk", F.col(pk).cast("int"))
    df = t.reject(df, F.col("_pk").isNull(), "invalid_primary_key")
    return df.withColumn(pk, F.col("_pk")).drop("_pk")


def keep_best_near_duplicate(df: DataFrame, pk: str, cols: list[str], t: CleansingTracker,
                             extra_order: list | None = None) -> DataFrame:
    """Same id, different data -> keep the most complete record, then extra_order (e.g. most
    recent), then a stable hash so the choice is deterministic between runs."""
    order = [F.col("_completeness").desc()] + (extra_order or []) + [F.col("_hash")]
    w = Window.partitionBy(pk).orderBy(*order)
    df = (df.withColumn("_completeness", completeness_score([c for c in cols if c != pk]))
            .withColumn("_hash", F.xxhash64(*cols))
            .withColumn("_rn", F.row_number().over(w)))
    return t.reject(df, F.col("_rn") > 1, "near_duplicate").drop("_rn", "_completeness", "_hash")


def fix_case(c: str):
    """Title-case values that are ALL lower or ALL UPPER; leave mixed case (e.g. 'McDonald') alone."""
    col = F.col(c)
    return F.when((col == F.lower(col)) | (col == F.upper(col)), F.initcap(col)).otherwise(col)


def flag_date_format(df: DataFrame, raw_col: str, parsed, iso_re: str, flag: str) -> DataFrame:
    return add_flag(df, F.col(raw_col).isNotNull() & parsed.isNotNull() & ~F.col(raw_col).rlike(iso_re), flag)


def finalize(df: DataFrame, out_cols: list[str], t: CleansingTracker, out_path: str,
             partitions: int) -> DataFrame:
    df = collect_flags(df)
    df = df.withColumn("dq_flags", F.when(F.size("_dq_flags") > 0, F.concat_ws(";", "_dq_flags")))
    df = df.cache()
    t.count_flags(df)
    final = df.select(*out_cols, "dq_flags").repartition(partitions)
    final.write.mode("overwrite").parquet(out_path)
    t.records_out = df.count()
    t.logger.info("  [%s] wrote %s clean records -> %s", t.entity, f"{t.records_out:,}", out_path)
    return final


def presence(df: DataFrame, key: str, marker: str) -> DataFrame:
    """Distinct keys with a marker column -- used for anti-join style orphan checks that still
    let us keep the orphans as rejects."""
    return df.select(key).distinct().withColumn(marker, F.lit(True))


# --------------------------------------------------------------------------- customers
def cleanse_customers(spark: SparkSession, paths: RunPaths, cfg: dict, log) -> list[CleansingTracker]:
    rules = cfg["cleanse"]
    t = CleansingTracker("customers", log)
    raw = read_raw(spark, paths.raw(RAW_FILES["customers"]), CUSTOMER_COLS)
    df = common_front_steps(raw, CUSTOMER_COLS, "customer_id", t)
    df = keep_best_near_duplicate(df, "customer_id", CUSTOMER_COLS, t)

    # Names: fix ALL-CAPS / all-lower, fill missing with 'Unknown'
    for c in ["first_name", "last_name"]:
        df = add_flag(df, F.col(c).isNull(), f"missing_{c}")
        df = add_flag(df, F.col(c).isNotNull() & (fix_case(c) != F.col(c)), f"{c}_case_standardized")
        df = df.withColumn(c, F.coalesce(fix_case(c), F.lit("Unknown")))

    # Email: lower-case, validate format; invalid -> NULL (we can't invent an address)
    df = df.withColumn("email", F.lower("email"))
    invalid_email = F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE)
    df = add_flag(df, F.col("email").isNull(), "missing_email")
    df = add_flag(df, invalid_email, "invalid_email")
    df = df.withColumn("email", F.when(invalid_email, F.lit(None)).otherwise(F.col("email")))

    # Phone: digits only -> XXX-XXX-XXXX
    digits = F.regexp_replace("phone", r"\D", "")
    digits = F.when((F.length(digits) == 11) & digits.startswith("1"), digits.substr(2, 10)).otherwise(digits)
    std_phone = F.when(F.length(digits) == 10,
                       F.concat_ws("-", digits.substr(1, 3), digits.substr(4, 3), digits.substr(7, 4)))
    df = add_flag(df, F.col("phone").isNull(), "missing_phone")
    df = add_flag(df, F.col("phone").isNotNull() & std_phone.isNull(), "invalid_phone")
    df = add_flag(df, F.col("phone").isNotNull() & std_phone.isNotNull() & (std_phone != F.col("phone")),
                  "phone_format_standardized")
    df = df.withColumn("phone", std_phone)

    # City: Title Case ("new york" / "NEW YORK" -> "New York")
    df = add_flag(df, F.col("city").isNull(), "missing_city")
    df = add_flag(df, F.col("city").isNotNull() & (F.initcap("city") != F.col("city")), "city_case_standardized")
    df = df.withColumn("city", F.coalesce(F.initcap("city"), F.lit("Unknown")))

    # Registration date: any of the 3 formats -> DATE; future / pre-2000 -> NULL + flag
    parsed = parse_date("registration_date")
    df = flag_date_format(df, "registration_date", parsed, ISO_DATE_RE, "date_format_standardized")
    df = add_flag(df, F.col("registration_date").isNull(), "missing_registration_date")
    df = add_flag(df, F.col("registration_date").isNotNull() & parsed.isNull(), "unparseable_registration_date")
    df = df.withColumn("registration_date", parsed)
    impossible = (F.col("registration_date") > F.current_date()) | \
                 (F.col("registration_date") < F.lit(rules["min_valid_date"]).cast("date"))
    df = add_flag(df, impossible, "impossible_registration_date")
    df = df.withColumn("registration_date", F.when(impossible, F.lit(None)).otherwise(F.col("registration_date")))

    # Customer type: map any casing to Regular / Premium / VIP, default Regular
    up = F.upper("customer_type")
    mapped = None
    for v in rules["valid_customer_types"]:
        mapped = (F.when(up == v.upper(), F.lit(v)) if mapped is None else mapped.when(up == v.upper(), F.lit(v)))
    df = add_flag(df, F.col("customer_type").isNull(), "missing_customer_type")
    df = add_flag(df, F.col("customer_type").isNotNull() & mapped.isNull(), "invalid_customer_type")
    df = add_flag(df, mapped.isNotNull() & (mapped != F.col("customer_type")), "customer_type_case_standardized")
    df = df.withColumn("customer_type", F.coalesce(mapped, F.lit(rules["default_customer_type"])))

    # Email must be unique: the same address on two customer ids -> keep it on the lowest id
    w = Window.partitionBy("email").orderBy("customer_id")
    df = df.withColumn("_dup_email", F.col("email").isNotNull() & (F.row_number().over(w) > 1))
    df = add_flag(df, F.col("_dup_email"), "duplicate_email_removed")
    df = df.withColumn("email", F.when(F.col("_dup_email"), F.lit(None)).otherwise(F.col("email"))).drop("_dup_email")

    finalize(df, CUSTOMER_COLS, t, paths.cleaned("customers"), rules["output_partitions"])
    t.write_rejects(paths.rejected("customers"))
    return [t]


# --------------------------------------------------------------------------- products
def cleanse_products(spark: SparkSession, paths: RunPaths, cfg: dict, log) -> list[CleansingTracker]:
    rules = cfg["cleanse"]
    t = CleansingTracker("products", log)
    raw = read_raw(spark, paths.raw(RAW_FILES["products"]), PRODUCT_COLS)
    df = common_front_steps(raw, PRODUCT_COLS, "product_id", t)
    df = keep_best_near_duplicate(df, "product_id", PRODUCT_COLS, t)

    df = add_flag(df, F.col("product_name").isNull(), "missing_product_name")
    df = add_flag(df, F.col("product_name").isNotNull() & (fix_case("product_name") != F.col("product_name")),
                  "product_name_case_standardized")
    df = df.withColumn("product_name", F.coalesce(fix_case("product_name"), F.lit("Unnamed Product")))

    df = add_flag(df, F.col("category").isNull(), "missing_category")
    df = add_flag(df, F.col("category").isNotNull() & (F.initcap("category") != F.col("category")),
                  "category_case_standardized")
    df = df.withColumn("category", F.coalesce(F.initcap("category"), F.lit("Uncategorized")))

    df = add_flag(df, F.col("brand").isNull(), "missing_brand")
    df = df.withColumn("brand", F.coalesce(F.col("brand"), F.lit("Unknown Brand")))

    # Price: "$12.99" -> 12.99 ; negative -> NULL + flag (product kept; orders carry their own price)
    price = parse_amount("unit_price", "decimal(10,2)")
    df = add_flag(df, F.col("unit_price").contains("$"), "price_format_standardized")
    df = add_flag(df, F.col("unit_price").isNull(), "missing_unit_price")
    df = add_flag(df, F.col("unit_price").isNotNull() & price.isNull(), "unparseable_unit_price")
    df = df.withColumn("unit_price", price)
    df = add_flag(df, F.col("unit_price") < 0, "negative_unit_price")
    df = df.withColumn("unit_price", F.when(F.col("unit_price") < 0, F.lit(None)).otherwise(F.col("unit_price")))

    # Stock: NULL or negative -> 0
    stock = F.col("stock_quantity").cast("int")
    df = add_flag(df, stock.isNull(), "missing_stock_quantity")
    df = add_flag(df, stock < 0, "negative_stock_quantity")
    df = df.withColumn("stock_quantity", F.when(stock.isNull() | (stock < 0), F.lit(0)).otherwise(stock))

    # is_active: true/yes/Y/1/... -> boolean, unknown -> False
    v = F.lower("is_active")
    as_bool = (F.when(v.isin("true", "t", "yes", "y", "1"), F.lit(True))
                .when(v.isin("false", "f", "no", "n", "0"), F.lit(False)))
    df = add_flag(df, F.col("is_active").isNotNull() & ~v.isin("true", "false") & as_bool.isNotNull(),
                  "boolean_format_standardized")
    df = add_flag(df, as_bool.isNull(), "missing_or_invalid_is_active")
    df = df.withColumn("is_active", F.coalesce(as_bool, F.lit(False)))

    parsed = parse_date("created_date")
    df = flag_date_format(df, "created_date", parsed, ISO_DATE_RE, "date_format_standardized")
    df = add_flag(df, F.col("created_date").isNull() | parsed.isNull(), "missing_created_date")
    df = df.withColumn("created_date", F.when(parsed <= F.current_date(), parsed))

    finalize(df, PRODUCT_COLS, t, paths.cleaned("products"), rules["output_partitions"])
    t.write_rejects(paths.rejected("products"))
    return [t]


# --------------------------------------------------------------------------- orders + order_items
def _clean_int(df: DataFrame, col: str, t: CleansingTracker, reason: str) -> DataFrame:
    df = df.withColumn(f"_{col}", F.col(col).cast("int"))
    df = t.reject(df, F.col(f"_{col}").isNull(), reason)
    return df.withColumn(col, F.col(f"_{col}")).drop(f"_{col}")


def cleanse_orders(spark: SparkSession, paths: RunPaths, cfg: dict, log) -> list[CleansingTracker]:
    rules = cfg["cleanse"]
    try:
        customers = spark.read.parquet(paths.cleaned("customers"))
        products = spark.read.parquet(paths.cleaned("products"))
    except AnalysisException as exc:
        raise RuntimeError("cleaned customers/products not found -- run those cleanse jobs first") from exc

    # ---------------- order_items
    ti = CleansingTracker("order_items", log)
    raw_items = read_raw(spark, paths.raw(RAW_FILES["order_items"]), ORDER_ITEM_COLS)
    items = common_front_steps(raw_items, ORDER_ITEM_COLS, "order_item_id", ti)
    items = _clean_int(items, "order_id", ti, "null_or_invalid_order_id")
    items = _clean_int(items, "product_id", ti, "null_or_invalid_product_id")
    items = items.withColumn("quantity", F.col("quantity").cast("int"))
    items = ti.reject(items, F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
    items = items.withColumn("unit_price", parse_amount("unit_price", "decimal(10,2)"))
    items = ti.reject(items, F.col("unit_price") < 0, "negative_unit_price")
    items = keep_best_near_duplicate(items, "order_item_id", ORDER_ITEM_COLS, ti)

    # Referential integrity: product must exist (broadcast: products table is small)
    catalog = products.select("product_id", F.col("unit_price").alias("_catalog_price"),
                              F.lit(True).alias("_product_exists"))
    items = items.join(F.broadcast(catalog), "product_id", "left")
    items = ti.reject(items, F.col("_product_exists").isNull(), "orphan_product")
    # Missing sale price -> use the catalog price (business rule), else drop
    items = add_flag(items, F.col("unit_price").isNull() & F.col("_catalog_price").isNotNull(),
                     "unit_price_from_catalog")
    items = items.withColumn("unit_price", F.coalesce("unit_price", "_catalog_price"))
    items = ti.reject(items, F.col("unit_price").isNull(), "missing_unit_price")
    items = (items.drop("_catalog_price", "_product_exists")
                  .withColumn("line_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
                  .cache())

    # ---------------- orders
    t = CleansingTracker("orders", log)
    raw = read_raw(spark, paths.raw(RAW_FILES["orders"]), ORDER_COLS)
    df = common_front_steps(raw, ORDER_COLS, "order_id", t)
    df = _clean_int(df, "customer_id", t, "null_or_invalid_customer_id")

    # Dates: parse 3 formats -> TIMESTAMP; reject missing / unparseable / impossible
    parsed = parse_timestamp("order_date")
    df = t.reject(df, F.col("order_date").isNull(), "missing_order_date")
    df = t.reject(df, parsed.isNull(), "unparseable_order_date")
    df = flag_date_format(df, "order_date", parsed, ISO_TS_RE, "date_format_standardized")
    df = df.withColumn("order_date", parsed)
    df = t.reject(df, (F.col("order_date") > F.current_timestamp()) |
                      (F.col("order_date") < F.lit(rules["min_valid_date"]).cast("timestamp")),
                  "impossible_order_date")

    # Status: any casing -> Pending/Shipped/Delivered ; missing -> Pending
    status = F.initcap("order_status")
    valid = status.isin(*rules["valid_order_statuses"])
    df = add_flag(df, F.col("order_status").isNull() | ~valid, "missing_or_invalid_status")
    df = add_flag(df, valid & (status != F.col("order_status")), "status_case_standardized")
    df = df.withColumn("order_status", F.when(valid, status).otherwise(F.lit(rules["default_order_status"])))

    # Payment method: 'credit_card' / 'CREDIT CARD' / 'CreditCard' -> 'Credit Card'
    key = F.lower(F.regexp_replace("payment_method", r"[^A-Za-z]", ""))
    pm_map = F.create_map(*[F.lit(x) for x in chain(*PAYMENT_MAP.items())])
    pm = pm_map[key]
    df = add_flag(df, F.col("payment_method").isNull(), "missing_payment_method")
    df = add_flag(df, F.col("payment_method").isNotNull() & pm.isNull(), "unknown_payment_method")
    df = add_flag(df, pm.isNotNull() & (pm != F.col("payment_method")), "payment_method_standardized")
    df = df.withColumn("payment_method",
                       F.when(F.col("payment_method").isNull(), F.lit("Unknown")).otherwise(F.coalesce(pm, F.lit("Other"))))

    # Amounts: strip '$' / spaces -> decimal
    for c in ["total_amount", "discount_amount", "shipping_cost"]:
        df = add_flag(df, F.col(c).contains("$"), "amount_format_standardized")
        df = df.withColumn(c, parse_amount(c))

    # Near-duplicates: most complete, then most advanced status (= most recent snapshot)
    rank = F.create_map(*[F.lit(x) for x in chain(*STATUS_RANK.items())])
    df = keep_best_near_duplicate(df, "order_id", ORDER_COLS, t, [rank[F.col("order_status")].desc()])

    # Referential integrity: customer must exist in the cleaned customers
    df = df.join(F.broadcast(presence(customers, "customer_id", "_customer_exists")), "customer_id", "left")
    df = t.reject(df, F.col("_customer_exists").isNull(), "orphan_customer").drop("_customer_exists")

    # NULL total -> recompute from the order's line items (business rule), else drop
    subtotal = items.groupBy("order_id").agg(F.sum("line_total").cast("decimal(12,2)").alias("_items_total"))
    df = df.join(subtotal, "order_id", "left")
    df = add_flag(df, F.col("total_amount").isNull() & F.col("_items_total").isNotNull(), "total_recomputed_from_items")
    df = df.withColumn("total_amount", F.coalesce("total_amount", "_items_total")).drop("_items_total")
    df = t.reject(df, F.col("total_amount").isNull(), "missing_total_amount")
    df = t.reject(df, F.col("total_amount") < 0, "negative_total_amount")

    # Discount / shipping: NULL or negative -> 0 ; discount larger than the order -> invalid
    for c in ["discount_amount", "shipping_cost"]:
        df = add_flag(df, F.col(c).isNull(), f"missing_{c}")
        df = add_flag(df, F.col(c) < 0, f"negative_{c}")
        df = df.withColumn(c, F.when(F.col(c).isNull() | (F.col(c) < 0), F.lit(0).cast("decimal(12,2)"))
                              .otherwise(F.col(c)))
    df = t.reject(df, F.col("discount_amount") > F.col("total_amount"), "discount_exceeds_total")
    df = df.withColumn("net_amount", (F.col("total_amount") - F.col("discount_amount")).cast("decimal(12,2)"))

    orders_out = finalize(df, ORDER_COLS + ["net_amount"], t, paths.cleaned("orders"), rules["output_partitions"])
    t.write_rejects(paths.rejected("orders"))

    # Items whose order was rejected are now orphans
    items = items.join(presence(orders_out, "order_id", "_order_exists"), "order_id", "left")
    items = ti.reject(items, F.col("_order_exists").isNull(), "orphan_order").drop("_order_exists")
    finalize(items, ORDER_ITEM_COLS + ["line_total"], ti, paths.cleaned("order_items"), rules["output_partitions"])
    ti.write_rejects(paths.rejected("order_items"))
    return [t, ti]


JOBS = {"customers": cleanse_customers, "products": cleanse_products, "orders": cleanse_orders}


# --------------------------------------------------------------------------- main
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Cleanse one entity with PySpark.")
    p.add_argument("--entity", required=True, choices=sorted(JOBS))
    p.add_argument("--run-id", required=True)
    p.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    p.add_argument("--config", default=None, help="path to spark_config.json")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    paths = RunPaths(args.data_dir, args.run_id)
    log = get_logger(f"spark_cleanse_{args.entity}", paths.logs_dir)
    spark = None
    try:
        cfg = load_config(args.config)
        spark = get_spark(f"cleanse_{args.entity}", cfg)
        log.info("Spark %s | master=%s | app=%s | run_id=%s", spark.version, spark.sparkContext.master,
                 spark.sparkContext.applicationId, args.run_id)
        for tracker in JOBS[args.entity](spark, paths, cfg, log):
            tracker.log_summary()
            tracker.write_stats(paths.reports_dir)
        log.info("Summary report: %s", write_summary_report(paths.reports_dir))
        return 0
    except FileNotFoundError as exc:
        log.error("Input missing: %s", exc)
        return 1
    except AnalysisException as exc:
        log.exception("Spark analysis error (bad path, column or schema): %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 -- any failure must give a non-zero exit code for Airflow
        log.exception("Cleansing job for %s failed: %s", args.entity, exc)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
