#!/usr/bin/env python3
"""
benchmark_pandas_vs_spark.py -- bonus: compare the Project 1 style (pandas, single process)
with Spark on the *same* cleansing + aggregation workload at several data sizes.

Run inside the stack (uses the same Python/Spark as the pipeline):

    docker compose exec airflow-scheduler python /opt/airflow/scripts/benchmark_pandas_vs_spark.py
    docker compose exec airflow-scheduler python /opt/airflow/scripts/benchmark_pandas_vs_spark.py \
        --sizes 1000 10000 100000 500000 --master spark://spark-master:7077

Workload (identical logic in both engines, customers + orders):
    read CSV -> trim/normalize strings -> drop exact duplicates -> drop null/invalid keys
    -> keep most complete near-duplicate -> parse 3 date formats -> standardize city/email
    -> drop invalid amounts + orphan orders -> customer lifetime value + monthly revenue

Output: data/reports/benchmark/benchmark_results.csv, benchmark_results.md, benchmark_chart.png
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generate_data  # noqa: E402

DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")
CUST_COLS = ["customer_id", "first_name", "last_name", "email", "phone", "city", "registration_date",
             "customer_type"]
ORDER_COLS = ["order_id", "customer_id", "order_date", "order_status", "payment_method", "total_amount",
              "discount_amount", "shipping_cost"]


# --------------------------------------------------------------------------- pandas (Project 1 style)
def pandas_pipeline(raw_dir: str) -> dict:
    t = {}
    t0 = time.perf_counter()
    cust = pd.read_csv(os.path.join(raw_dir, "customers.csv"), dtype=str, on_bad_lines="skip",
                       keep_default_na=False, engine="python")
    orders = pd.read_csv(os.path.join(raw_dir, "orders.csv"), dtype=str, on_bad_lines="skip",
                         keep_default_na=False, engine="python")
    t["read"] = time.perf_counter() - t0

    t1 = time.perf_counter()

    def normalize(df):
        for c in df.columns:
            df[c] = df[c].str.replace(r"\s+", " ", regex=True).str.strip().replace("", None)
        return df

    def dedupe(df, pk):
        df = df.drop_duplicates()
        df = df[df[pk].notna()].copy()
        df[pk] = pd.to_numeric(df[pk], errors="coerce")
        df = df[df[pk].notna()]
        df["_complete"] = df.notna().sum(axis=1)
        return df.sort_values([pk, "_complete"], ascending=[True, False]).drop_duplicates(pk).drop(columns="_complete")

    def parse_dates(s, fmts):
        out = pd.Series(pd.NaT, index=s.index)
        for f in fmts:
            out = out.fillna(pd.to_datetime(s, format=f, errors="coerce"))
        return out

    cust = dedupe(normalize(cust[CUST_COLS].copy()), "customer_id")
    cust["city"] = cust["city"].str.title().fillna("Unknown")
    cust["email"] = cust["email"].str.lower()
    cust["registration_date"] = parse_dates(cust["registration_date"], ["%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y"])

    orders = dedupe(normalize(orders[ORDER_COLS].copy()), "order_id")
    orders["customer_id"] = pd.to_numeric(orders["customer_id"], errors="coerce")
    orders["order_date"] = parse_dates(orders["order_date"], ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%b %d, %Y"])
    for c in ["total_amount", "discount_amount", "shipping_cost"]:
        orders[c] = pd.to_numeric(orders[c].str.replace("$", "", regex=False), errors="coerce")
    orders[["discount_amount", "shipping_cost"]] = orders[["discount_amount", "shipping_cost"]].fillna(0)
    orders = orders[orders["order_date"].notna() & orders["total_amount"].notna() & (orders["total_amount"] >= 0)
                    & (orders["discount_amount"] <= orders["total_amount"])]
    orders = orders[orders["customer_id"].isin(cust["customer_id"])]
    orders["net_amount"] = orders["total_amount"] - orders["discount_amount"]
    t["cleanse"] = time.perf_counter() - t1

    t2 = time.perf_counter()
    clv = orders.groupby("customer_id")["net_amount"].agg(["sum", "count", "mean"])
    monthly = orders.groupby(orders["order_date"].dt.to_period("M"))["net_amount"].sum()
    t["aggregate"] = time.perf_counter() - t2
    t["total"] = time.perf_counter() - t0
    t["rows_out"] = len(orders)
    t["_check"] = (len(clv), len(monthly))
    return t


# --------------------------------------------------------------------------- Spark
def spark_pipeline(spark, raw_dir: str) -> dict:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from spark_utils import normalize_string, parse_amount, parse_date, parse_timestamp

    t = {}
    t0 = time.perf_counter()
    cust = spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(
        ", ".join(f"{c} string" for c in CUST_COLS)).csv(os.path.join(raw_dir, "customers.csv"))
    orders = spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(
        ", ".join(f"{c} string" for c in ORDER_COLS)).csv(os.path.join(raw_dir, "orders.csv"))
    cust, orders = cust.cache(), orders.cache()
    cust.count(), orders.count()                       # force the read so it is timed separately
    t["read"] = time.perf_counter() - t0

    t1 = time.perf_counter()

    def dedupe(df, pk, cols):
        df = df.dropDuplicates()
        for c in cols:
            df = df.withColumn(c, normalize_string(c))
        df = df.withColumn(pk, F.col(pk).cast("int")).filter(F.col(pk).isNotNull())
        complete = sum((F.col(c).isNotNull().cast("int") for c in cols), F.lit(0))
        w = Window.partitionBy(pk).orderBy(complete.desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    cust = (dedupe(cust, "customer_id", CUST_COLS)
            .withColumn("city", F.coalesce(F.initcap("city"), F.lit("Unknown")))
            .withColumn("email", F.lower("email"))
            .withColumn("registration_date", parse_date("registration_date")))
    orders = (dedupe(orders, "order_id", ORDER_COLS)
              .withColumn("customer_id", F.col("customer_id").cast("int"))
              .withColumn("order_date", parse_timestamp("order_date"))
              .withColumn("total_amount", parse_amount("total_amount"))
              .withColumn("discount_amount", F.coalesce(parse_amount("discount_amount"), F.lit(0)))
              .withColumn("shipping_cost", F.coalesce(parse_amount("shipping_cost"), F.lit(0)))
              .filter(F.col("order_date").isNotNull() & (F.col("total_amount") >= 0)
                      & (F.col("discount_amount") <= F.col("total_amount")))
              .join(F.broadcast(cust.select("customer_id")), "customer_id", "left_semi")
              .withColumn("net_amount", F.col("total_amount") - F.col("discount_amount"))
              .cache())
    rows_out = orders.count()
    t["cleanse"] = time.perf_counter() - t1

    t2 = time.perf_counter()
    clv = orders.groupBy("customer_id").agg(F.sum("net_amount"), F.count("*"), F.avg("net_amount")).count()
    monthly = orders.groupBy(F.date_trunc("month", "order_date")).agg(F.sum("net_amount")).count()
    t["aggregate"] = time.perf_counter() - t2
    t["total"] = time.perf_counter() - t0
    t["rows_out"] = rows_out
    t["_check"] = (clv, monthly)
    for df in (cust, orders):
        df.unpersist()
    return t


# --------------------------------------------------------------------------- reporting
def write_chart(rows: list[dict], path: str):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = {"pandas": "#2a78d6", "spark": "#eb6834"}          # categorical slots 1 and 2
    ink, muted, surface = "#0b0b0b", "#52514e", "#fcfcfb"
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
    fig.patch.set_facecolor(surface)
    ax.set_facecolor(surface)
    ends = {}
    for engine in ["pandas", "spark"]:
        pts = sorted((r["records"], r["total_s"]) for r in rows if r["engine"] == engine)
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=colors[engine], linewidth=2, marker="o", markersize=8,
                markeredgecolor=surface, markeredgewidth=2, label=engine)
        ends[engine] = (xs[-1], ys[-1])
    # direct labels at the line ends, nudged apart so they never overlap
    higher = max(ends, key=lambda e: ends[e][1])
    for engine, (x, y) in ends.items():
        ax.annotate(f"{engine}  {y:.1f}s", (x, y), xytext=(10, 8 if engine == higher else -8),
                    textcoords="offset points", va="center", color=ink, fontsize=9)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Base records per entity (log scale)", color=muted)
    ax.set_ylabel("Wall-clock seconds (log scale)", color=muted)
    ax.set_title("Cleanse + aggregate time: pandas vs Spark", color=ink, loc="left", fontsize=12)
    ax.grid(True, which="major", color="#e4e3df", linewidth=0.8)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color("#c9c8c2")
    ax.tick_params(colors=muted)
    ax.legend(frameon=False, loc="upper left")
    ax.margins(x=0.15)
    fig.tight_layout()
    fig.savefig(path, facecolor=surface)
    plt.close(fig)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Benchmark pandas vs Spark on the cleansing workload")
    p.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 100000])
    p.add_argument("--master", default="local[*]", help="Spark master (local[*] or spark://spark-master:7077)")
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--repeats", type=int, default=1, help="runs per size; the fastest is kept")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession
    from spark_utils import load_config

    bench_dir = os.path.join(args.data_dir, "benchmark")
    out_dir = os.path.join(args.data_dir, "reports", "benchmark")
    os.makedirs(out_dir, exist_ok=True)

    t0 = time.perf_counter()
    builder = SparkSession.builder.appName("data430_prj2_benchmark").master(args.master)
    for k, v in load_config().get("spark_conf", {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.perf_counter() - t0
    print(f"Spark session startup: {startup:.1f}s (not included below) | master={spark.sparkContext.master}")

    # Warm-up (not timed): the first Spark query pays JVM class-loading / JIT costs
    warm = os.path.join(bench_dir, f"raw_{min(args.sizes)}")
    if not os.path.exists(os.path.join(warm, "orders.csv")):
        generate_data.main(["--records", str(min(args.sizes)), "--output-dir", warm])
    spark_pipeline(spark, warm)

    rows = []
    for n in args.sizes:
        raw = os.path.join(bench_dir, f"raw_{n}")
        if not os.path.exists(os.path.join(raw, "orders.csv")):
            generate_data.main(["--records", str(n), "--output-dir", raw])
        for engine, fn in [("pandas", lambda: pandas_pipeline(raw)), ("spark", lambda: spark_pipeline(spark, raw))]:
            best = min((fn() for _ in range(args.repeats)), key=lambda r: r["total"])
            rows.append({"engine": engine, "records": n, "read_s": round(best["read"], 3),
                         "cleanse_s": round(best["cleanse"], 3), "aggregate_s": round(best["aggregate"], 3),
                         "total_s": round(best["total"], 3), "orders_out": best["rows_out"]})
            print(f"{engine:7s} {n:>9,} records  total {best['total']:7.2f}s  "
                  f"(read {best['read']:.2f} / cleanse {best['cleanse']:.2f} / agg {best['aggregate']:.2f})"
                  f"  orders out {best['rows_out']:,}")
    spark.stop()

    with open(os.path.join(out_dir, "benchmark_results.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    with open(os.path.join(out_dir, "benchmark_results.md"), "w") as fh:
        fh.write(f"Spark master: `{args.master}` - session startup {startup:.1f}s (excluded)\n\n")
        fh.write("| Records | pandas total (s) | Spark total (s) | Spark speed-up (pandas ÷ Spark) | pandas orders out | Spark orders out |\n")
        fh.write("|---:|---:|---:|---:|---:|---:|\n")
        for n in args.sizes:
            pr = next(r for r in rows if r["engine"] == "pandas" and r["records"] == n)
            sr = next(r for r in rows if r["engine"] == "spark" and r["records"] == n)
            fh.write(f"| {n:,} | {pr['total_s']:.2f} | {sr['total_s']:.2f} | "
                     f"{pr['total_s'] / sr['total_s']:.2f}x | {pr['orders_out']:,} | {sr['orders_out']:,} |\n")
    write_chart(rows, os.path.join(out_dir, "benchmark_chart.png"))
    print(f"Results -> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
