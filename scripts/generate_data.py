#!/usr/bin/env python3
"""
generate_data.py -- Faker-based generator for DATA 430 Project 2.

Produces N clean "base" records per entity (default 100,000) and then deliberately
injects the data-quality problems that the Spark cleansing jobs must fix:

    1. Duplicate records      exact copies + near-duplicates (same id, slightly different data)
    2. Null values            emails, names, product names, order amounts, keys, ...
    3. Inconsistent formats   city / category / status / payment casing, phone formats, "$" prices
    4. Invalid values         negative totals, discount > total, negative stock, future dates
    5. Orphan records         orders -> missing customers, order_items -> missing products
    6. Whitespace issues      leading/trailing/double spaces in string fields
    7. Date inconsistencies   "2024-01-15", "01/15/2024", "Jan 15, 2024" mixed in every date field
    (+) Malformed lines       a few corrupt CSV / JSON lines for Spark's PERMISSIVE mode

Output files (one folder per pipeline run):
    customers.csv, products.json (JSON Lines), orders.csv, order_items.csv,
    generation_manifest.json  <- ground truth of what was injected (useful for the report)

Usage:
    python generate_data.py --records 100000 --output-dir /opt/airflow/data/raw/<run_id>
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
from faker import Faker

log = logging.getLogger("generate_data")

MONTH_ABBR = np.array(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Columbus", "Fort Worth",
    "Charlotte", "Indianapolis", "Seattle", "Denver", "Washington", "Boston", "Nashville",
    "Detroit", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore", "Milwaukee",
    "Albuquerque", "Tucson", "Sacramento", "Kansas City", "Atlanta", "Miami", "Raleigh",
    "Omaha", "Minneapolis", "Cleveland", "Tampa", "Pittsburgh", "Cincinnati", "St. Louis",
    "Orlando", "Salt Lake City", "New Orleans",
]
CUSTOMER_TYPES = (["Regular", "Premium", "VIP"], [0.70, 0.22, 0.08])

CATEGORIES = {
    # category: (nouns, (min_price, max_price))
    "Electronics": (["Headphones", "Speaker", "Monitor", "Keyboard", "Mouse", "Charger", "Webcam",
                     "Tablet", "Smartwatch", "Router"], (15, 900)),
    "Home & Kitchen": (["Blender", "Toaster", "Cookware Set", "Knife Set", "Coffee Maker", "Lamp",
                        "Vacuum", "Air Fryer", "Kettle", "Cutting Board"], (10, 400)),
    "Sports & Outdoors": (["Yoga Mat", "Dumbbells", "Tent", "Backpack", "Water Bottle", "Bike Helmet",
                           "Jump Rope", "Sleeping Bag", "Hiking Poles", "Cooler"], (8, 350)),
    "Clothing": (["T-Shirt", "Hoodie", "Jeans", "Jacket", "Sneakers", "Socks", "Cap", "Scarf",
                  "Dress", "Shorts"], (6, 220)),
    "Beauty & Personal Care": (["Shampoo", "Face Cream", "Perfume", "Hair Dryer", "Razor", "Lotion",
                                "Lip Balm", "Sunscreen", "Toothbrush", "Nail Kit"], (4, 150)),
    "Books": (["Novel", "Cookbook", "Biography", "Textbook", "Comic", "Journal", "Atlas",
               "Poetry Collection", "Guidebook", "Workbook"], (5, 120)),
    "Toys & Games": (["Puzzle", "Board Game", "Action Figure", "Doll", "Building Blocks", "RC Car",
                      "Plush Toy", "Card Game", "Kite", "Train Set"], (5, 180)),
    "Grocery": (["Coffee Beans", "Green Tea", "Olive Oil", "Granola", "Pasta", "Honey", "Hot Sauce",
                 "Trail Mix", "Protein Bar", "Spice Set"], (2, 60)),
    "Office Supplies": (["Notebook", "Pen Set", "Desk Organizer", "Stapler", "Planner", "Whiteboard",
                         "Paper Shredder", "Desk Lamp", "File Cabinet", "Label Maker"], (3, 250)),
    "Pet Supplies": (["Dog Bed", "Cat Tree", "Pet Bowl", "Leash", "Chew Toy", "Litter Box",
                      "Aquarium", "Bird Feeder", "Pet Shampoo", "Scratching Post"], (4, 200)),
}
ADJECTIVES = ["Premium", "Classic", "Ultra", "Compact", "Deluxe", "Eco", "Smart", "Pro",
              "Portable", "Essential", "Everyday", "Signature"]

ORDER_STATUSES = ["Pending", "Shipped", "Delivered"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Gift Card", "Bank Transfer"]
# Messy spellings of the same payment methods (inconsistent formats)
PAYMENT_VARIANTS = {
    "Credit Card": ["credit card", "CREDIT CARD", "credit_card", "CreditCard"],
    "Debit Card": ["debit card", "DEBIT CARD", "debit_card"],
    "PayPal": ["paypal", "PAYPAL", "Paypal"],
    "Apple Pay": ["apple pay", "APPLE PAY", "applepay"],
    "Gift Card": ["gift card", "GIFT CARD", "giftcard"],
    "Bank Transfer": ["bank transfer", "BANK TRANSFER", "bank_transfer"],
}

ORDER_START = pd.Timestamp("2024-01-01")
ORDER_END = pd.Timestamp("2026-06-30 23:59:59")


# --------------------------------------------------------------------------- helpers
def mask(rng: np.random.Generator, n: int, rate: float) -> np.ndarray:
    """Boolean mask selecting roughly `rate` of n rows."""
    return rng.random(n) < rate


def random_dates(rng, n, start: pd.Timestamp, end: pd.Timestamp, with_time=False) -> pd.Series:
    span = int((end - start).total_seconds())
    secs = rng.integers(0, span, size=n)
    out = start + pd.to_timedelta(secs, unit="s")
    if not with_time:
        out = out.normalize()
    return pd.Series(out)


def mixed_date_strings(rng, dates: pd.Series, with_time=False) -> np.ndarray:
    """Format dates in three inconsistent styles: ISO (70%), US (15%), 'Mon d, yyyy' (15%)."""
    dates = pd.Series(pd.to_datetime(dates)).reset_index(drop=True)
    style = rng.choice(3, size=len(dates), p=[0.70, 0.15, 0.15])
    iso = dates.dt.strftime("%Y-%m-%d %H:%M:%S" if with_time else "%Y-%m-%d").to_numpy()
    us = dates.dt.strftime("%m/%d/%Y %H:%M" if with_time else "%m/%d/%Y").to_numpy()
    text = (MONTH_ABBR[dates.dt.month.to_numpy() - 1].astype(object) + " "
            + dates.dt.day.astype(str).to_numpy(dtype=object)
            + ", " + dates.dt.year.astype(str).to_numpy(dtype=object))
    return np.where(style == 0, iso, np.where(style == 1, us, text)).astype(object)


def case_variants(rng, values: pd.Series, rate: float) -> tuple[pd.Series, int]:
    """Randomly lower-case or upper-case a fraction of string values."""
    values = values.copy()
    m = mask(rng, len(values), rate) & values.notna().to_numpy()
    lower = m & (rng.random(len(values)) < 0.5)
    upper = m & ~lower
    values[lower] = values[lower].str.lower()
    values[upper] = values[upper].str.upper()
    return values, int(m.sum())


def inject_nulls(rng, df: pd.DataFrame, rates: dict, manifest: dict, prefix: str):
    for col, rate in rates.items():
        m = mask(rng, len(df), rate)
        df.loc[m, col] = None
        manifest[f"{prefix}.null_{col}"] = int(m.sum())


def pad_whitespace(rng, df: pd.DataFrame, cols: list, rate: float, manifest: dict, prefix: str):
    total = 0
    pads = np.array(["  {}", "{}  ", " {} ", "\t{}", "{} "])
    for col in cols:
        m = mask(rng, len(df), rate) & df[col].notna().to_numpy()
        choice = pads[rng.integers(0, len(pads), size=int(m.sum()))]
        df.loc[m, col] = [p.format(v) for p, v in zip(choice, df.loc[m, col])]
        # also squeeze a double space into some multi-word values
        m2 = mask(rng, len(df), rate / 3) & df[col].notna().to_numpy()
        df.loc[m2, col] = df.loc[m2, col].astype(str).str.replace(" ", "  ", n=1, regex=False)
        total += int(m.sum()) + int(m2.sum())
    manifest[f"{prefix}.whitespace_cells"] = total


def add_exact_duplicates(rng, df: pd.DataFrame, rate: float, manifest: dict, prefix: str) -> pd.DataFrame:
    idx = rng.choice(len(df), size=int(len(df) * rate), replace=False)
    manifest[f"{prefix}.exact_duplicates"] = len(idx)
    return pd.concat([df, df.iloc[idx]], ignore_index=True)


def shuffle(rng, df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[rng.permutation(len(df))].reset_index(drop=True)


def money(values: np.ndarray) -> np.ndarray:
    return np.array([f"{v:.2f}" for v in values], dtype=object)


def insert_corrupt_lines(rng, lines: list[str], corrupt: list[str]) -> list[str]:
    """Insert malformed lines at random positions (never before the header)."""
    for bad in corrupt:
        pos = int(rng.integers(1, len(lines)))
        lines.insert(pos, bad)
    return lines


def write_csv_with_corruption(rng, df: pd.DataFrame, path: str, corrupt: list[str]):
    text = df.to_csv(index=False, lineterminator="\n")
    lines = insert_corrupt_lines(rng, text.rstrip("\n").split("\n"), corrupt)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")


# --------------------------------------------------------------------------- customers
def generate_customers(n: int, rng, fake: Faker, manifest: dict) -> pd.DataFrame:
    first_pool = np.array(sorted({fake.first_name() for _ in range(4000)}))
    last_pool = np.array(sorted({fake.last_name() for _ in range(4000)}))
    domains = np.array(sorted({fake.free_email_domain() for _ in range(200)} |
                              {fake.domain_name() for _ in range(40)}))

    ids = np.arange(1, n + 1)
    first = rng.choice(first_pool, n)
    last = rng.choice(last_pool, n)
    clean = np.vectorize(lambda s: re.sub(r"[^A-Za-z]", "", s).lower(), otypes=[object])
    email = (clean(first) + "." + clean(last) + ids.astype(str).astype(object) + "@"
             + rng.choice(domains, n).astype(object))

    area, exch, line = rng.integers(201, 990, n), rng.integers(200, 999, n), rng.integers(0, 10000, n)
    phone_style = rng.choice(4, size=n, p=[0.75, 0.10, 0.10, 0.05])
    phones = np.empty(n, dtype=object)
    for i in range(n):
        a, e, l = area[i], exch[i], line[i]
        phones[i] = (f"{a}-{e}-{l:04d}", f"({a}) {e}-{l:04d}",
                     f"{a}.{e}.{l:04d}", f"+1 {a} {e} {l:04d}")[phone_style[i]]
    manifest["customers.phone_nonstandard_format"] = int((phone_style > 0).sum())

    reg = random_dates(rng, n, pd.Timestamp("2018-01-01"), pd.Timestamp("2026-06-30"))
    base_types = rng.choice(CUSTOMER_TYPES[0], n, p=CUSTOMER_TYPES[1])
    df = pd.DataFrame({
        "customer_id": ids.astype(str).astype(object),
        "first_name": first.astype(object),
        "last_name": last.astype(object),
        "email": email.astype(object),
        "phone": phones,
        "city": rng.choice(CITIES, n).astype(object),
        "registration_date": mixed_date_strings(rng, reg),
        "customer_type": base_types.astype(object),
    })
    manifest["customers.base_records"] = n

    # Inconsistent formats: city and customer_type casing
    df["city"], k = case_variants(rng, df["city"], 0.30)
    manifest["customers.city_case_variants"] = k
    df["customer_type"], k = case_variants(rng, df["customer_type"], 0.10)
    manifest["customers.customer_type_case_variants"] = k

    # Invalid values: duplicated emails across different customers, malformed emails, future dates
    m = mask(rng, n, 0.002)
    df.loc[m, "email"] = df["email"].iloc[rng.integers(0, n, int(m.sum()))].to_numpy()
    manifest["customers.email_collisions"] = int(m.sum())
    m = mask(rng, n, 0.005)
    df.loc[m, "email"] = df.loc[m, "email"].str.replace("@", "_at_", regex=False)
    manifest["customers.invalid_email"] = int(m.sum())
    m = mask(rng, n, 0.003)
    df.loc[m, "registration_date"] = mixed_date_strings(
        rng, random_dates(rng, int(m.sum()), pd.Timestamp("2027-01-01"), pd.Timestamp("2035-12-31")))
    manifest["customers.future_registration_date"] = int(m.sum())

    # Near-duplicates: same customer_id, slightly different data
    idx = rng.choice(n, size=int(n * 0.03), replace=False)
    near = df.iloc[idx].copy().reset_index(drop=True)
    kind = rng.integers(0, 4, len(near))
    near.loc[kind == 0, "phone"] = None                                  # less complete copy
    near.loc[kind == 1, "email"] = None                                  # less complete copy
    near.loc[kind == 2, "first_name"] = near.loc[kind == 2, "first_name"].str.upper()
    near.loc[kind == 2, "city"] = near.loc[kind == 2, "city"].str.lower()
    near.loc[kind == 3, "phone"] = near.loc[kind == 3, "phone"].str.replace(r"\D", "", regex=True)
    df = pd.concat([df, near], ignore_index=True)
    manifest["customers.near_duplicates"] = len(near)

    # Nulls (including a few null / invalid primary keys)
    inject_nulls(rng, df, {"email": 0.03, "phone": 0.02, "city": 0.02, "first_name": 0.01,
                           "last_name": 0.01, "registration_date": 0.01, "customer_type": 0.01,
                           "customer_id": 0.003}, manifest, "customers")
    m = mask(rng, len(df), 0.0005)
    df.loc[m, "customer_id"] = rng.choice(["N/A", "unknown", "#REF!"], int(m.sum()))
    manifest["customers.invalid_customer_id"] = int(m.sum())

    pad_whitespace(rng, df, ["first_name", "last_name", "email", "city", "customer_type"], 0.05,
                   manifest, "customers")
    df = add_exact_duplicates(rng, df, 0.02, manifest, "customers")
    return shuffle(rng, df), base_types


# --------------------------------------------------------------------------- products
def generate_products(n: int, rng, fake: Faker, manifest: dict) -> tuple[list[str], np.ndarray]:
    brands = sorted({re.sub(r"[^A-Za-z]", "", re.split(r"[\s,-]", fake.company())[0]) for _ in range(1500)} - {""})
    brands = np.array(brands[:300])
    cats = np.array(list(CATEGORIES))
    cat = rng.choice(cats, n)
    noun = np.array([CATEGORIES[c][0][i] for c, i in zip(cat, rng.integers(0, 10, n))])
    lo = np.array([CATEGORIES[c][1][0] for c in cat], dtype=float)
    hi = np.array([CATEGORIES[c][1][1] for c in cat], dtype=float)
    # log-uniform price inside the category range -> many cheap items, few expensive ones
    price = np.round(np.exp(rng.uniform(np.log(lo), np.log(hi))), 2)
    brand = rng.choice(brands, n)
    model = np.char.add(rng.choice(list("ABCDEFGHJKLMNPRSTVXZ"), n),
                        rng.integers(100, 999, n).astype(str)).astype(object)
    name = (brand.astype(object) + " " + rng.choice(ADJECTIVES, n).astype(object) + " "
            + noun.astype(object) + " " + model)
    created = random_dates(rng, n, pd.Timestamp("2015-01-01"), pd.Timestamp("2025-12-31"))

    df = pd.DataFrame({
        "product_id": np.arange(1, n + 1).astype(object),
        "product_name": name.astype(object),
        "category": cat.astype(object),
        "brand": brand.astype(object),
        "unit_price": price.astype(object),
        "stock_quantity": rng.integers(0, 500, n).astype(object),
        "is_active": (rng.random(n) < 0.90).astype(object),
        "created_date": mixed_date_strings(rng, created),
    })
    manifest["products.base_records"] = n
    clean_prices = price.copy()

    df["category"], k = case_variants(rng, df["category"], 0.15)
    manifest["products.category_case_variants"] = k
    # Inconsistent boolean / price representations
    m = mask(rng, n, 0.10)
    df.loc[m, "is_active"] = [rng.choice(["yes", "Y", "1", "TRUE"]) if v else rng.choice(["no", "N", "0", "FALSE"])
                              for v in df.loc[m, "is_active"]]
    manifest["products.is_active_text_variants"] = int(m.sum())
    m = mask(rng, n, 0.03)
    df.loc[m, "unit_price"] = ["$" + f"{v:.2f}" for v in df.loc[m, "unit_price"]]
    manifest["products.price_as_text"] = int(m.sum())
    # Invalid values
    m = mask(rng, n, 0.005)
    df.loc[m, "unit_price"] = [-abs(float(str(v).strip("$"))) for v in df.loc[m, "unit_price"]]
    manifest["products.negative_price"] = int(m.sum())
    m = mask(rng, n, 0.005)
    df.loc[m, "stock_quantity"] = -rng.integers(1, 50, int(m.sum()))
    manifest["products.negative_stock"] = int(m.sum())

    # Near-duplicates
    idx = rng.choice(n, size=int(n * 0.03), replace=False)
    near = df.iloc[idx].copy().reset_index(drop=True)
    kind = rng.integers(0, 3, len(near))
    near.loc[kind == 0, "brand"] = None
    near.loc[kind == 1, "product_name"] = near.loc[kind == 1, "product_name"].str.lower()
    near.loc[kind == 2, "stock_quantity"] = None
    df = pd.concat([df, near], ignore_index=True)
    manifest["products.near_duplicates"] = len(near)

    inject_nulls(rng, df, {"product_name": 0.02, "brand": 0.02, "category": 0.015, "unit_price": 0.015,
                           "stock_quantity": 0.01, "is_active": 0.01, "created_date": 0.01,
                           "product_id": 0.003}, manifest, "products")
    pad_whitespace(rng, df, ["product_name", "category", "brand"], 0.05, manifest, "products")
    df = add_exact_duplicates(rng, df, 0.02, manifest, "products")
    df = shuffle(rng, df)

    def clean_value(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v

    lines = [json.dumps({k: clean_value(v) for k, v in row.items()})
             for row in df.to_dict(orient="records")]
    corrupt = ['{"product_id": 999999999, "product_name": "Broken', '{not valid json at all}',
               '{"product_id": "123", "unit_price": 12.5,,}', "garbage line ###"]
    lines = insert_corrupt_lines(rng, lines, corrupt)
    manifest["products.malformed_lines"] = len(corrupt)
    return lines, clean_prices


# --------------------------------------------------------------------------- orders + items
def generate_orders(n: int, customer_types: np.ndarray, product_prices: np.ndarray, rng, manifest: dict):
    n_customers, n_products = len(customer_types), len(product_prices)
    # Heavy-tailed customer activity (a few customers place many orders); Premium and VIP
    # customers order more often, so the analytics have something to find.
    type_factor = np.select([customer_types == "VIP", customer_types == "Premium"], [3.0, 1.7], 1.0)
    weights = rng.gamma(0.6, 1.0, n_customers) * type_factor
    cust = rng.choice(np.arange(1, n_customers + 1), n, p=weights / weights.sum())
    odate = random_dates(rng, n, ORDER_START, ORDER_END, with_time=True)
    # Holiday season: move ~15% of orders into 15 Nov - 31 Dec of 2024 or 2025
    m = mask(rng, n, 0.15)
    year = rng.choice([2024, 2025], int(m.sum()))
    holiday = (pd.to_datetime([f"{y}-11-15" for y in year])
               + pd.to_timedelta(rng.integers(0, 47 * 86400, int(m.sum())), unit="s"))
    odate = odate.copy()
    odate[m] = holiday
    age_days = (ORDER_END - odate).dt.days.to_numpy()
    status = np.where(age_days < 7, rng.choice(ORDER_STATUSES, n, p=[0.7, 0.25, 0.05]),
             np.where(age_days < 21, rng.choice(ORDER_STATUSES, n, p=[0.15, 0.45, 0.40]),
                      rng.choice(ORDER_STATUSES, n, p=[0.03, 0.07, 0.90])))
    payment = rng.choice(PAYMENT_METHODS, n, p=[0.38, 0.22, 0.18, 0.12, 0.05, 0.05])

    # ---- order items (1-5 lines per order)
    n_lines = rng.choice([1, 2, 3, 4, 5], n, p=[0.35, 0.28, 0.18, 0.12, 0.07])
    # VIP baskets are a bit bigger
    vip = customer_types[cust - 1] == "VIP"
    n_lines[vip] = np.minimum(n_lines[vip] + rng.integers(0, 2, int(vip.sum())), 5)
    item_order = np.repeat(np.arange(1, n + 1), n_lines)
    m_items = len(item_order)
    pweights = rng.gamma(0.5, 1.0, n_products)
    item_product = rng.choice(np.arange(1, n_products + 1), m_items, p=pweights / pweights.sum())
    qty = rng.choice([1, 2, 3, 4], m_items, p=[0.62, 0.23, 0.10, 0.05])
    promo = rng.choice([0, 0.05, 0.10, 0.15, 0.20], m_items, p=[0.60, 0.15, 0.12, 0.08, 0.05])
    item_price = np.round(product_prices[item_product - 1] * (1 - promo), 2)
    line_total = qty * item_price
    total = np.round(np.bincount(item_order - 1, weights=line_total, minlength=n), 2)

    has_disc = rng.random(n) < 0.40
    discount = np.where(has_disc, np.round(total * rng.uniform(0.05, 0.25, n), 2), 0.0)
    shipping = np.where(total >= 75, 0.0, rng.choice([4.99, 6.99, 9.99], n))

    orders = pd.DataFrame({
        "order_id": np.arange(1, n + 1).astype(str).astype(object),
        "customer_id": cust.astype(str).astype(object),
        "order_date": mixed_date_strings(rng, odate, with_time=True),
        "order_status": status.astype(object),
        "payment_method": payment.astype(object),
        "total_amount": money(total),
        "discount_amount": money(discount),
        "shipping_cost": money(shipping),
    })
    manifest["orders.base_records"] = n

    # Inconsistent formats
    orders["order_status"], k = case_variants(rng, orders["order_status"], 0.15)
    manifest["orders.status_case_variants"] = k
    m = mask(rng, n, 0.15)
    orders.loc[m, "payment_method"] = [rng.choice(PAYMENT_VARIANTS[p]) for p in orders.loc[m, "payment_method"]]
    manifest["orders.payment_method_variants"] = int(m.sum())
    m = mask(rng, n, 0.02)
    orders.loc[m, "total_amount"] = "$" + orders.loc[m, "total_amount"]
    manifest["orders.amount_with_dollar_sign"] = int(m.sum())

    # Invalid values
    m = mask(rng, n, 0.01)
    orders.loc[m, "total_amount"] = money(-total[m])
    manifest["orders.negative_total_amount"] = int(m.sum())
    m2 = mask(rng, n, 0.01) & ~m & (total > 0)
    orders.loc[m2, "discount_amount"] = money(np.round(total[m2] * rng.uniform(1.1, 2.0, int(m2.sum())), 2))
    manifest["orders.discount_exceeds_total"] = int(m2.sum())
    m = mask(rng, n, 0.005)
    orders.loc[m, "order_date"] = mixed_date_strings(
        rng, random_dates(rng, int(m.sum()), pd.Timestamp("2027-01-01"), pd.Timestamp("2035-12-31"), True),
        with_time=True)
    manifest["orders.future_order_date"] = int(m.sum())
    m = mask(rng, n, 0.002)
    orders.loc[m, "order_date"] = rng.choice(["2024-13-45", "00/00/0000", "not a date", "31/31/2025"],
                                             int(m.sum()))
    manifest["orders.unparseable_order_date"] = int(m.sum())

    # Orphans: customer ids that do not exist
    m = mask(rng, n, 0.015)
    orders.loc[m, "customer_id"] = rng.integers(n_customers + 1, n_customers + 50000, int(m.sum())).astype(str)
    manifest["orders.orphan_customer_id"] = int(m.sum())

    # Near-duplicates: same order_id seen twice, older snapshot / less complete
    idx = rng.choice(n, size=int(n * 0.03), replace=False)
    near = orders.iloc[idx].copy().reset_index(drop=True)
    kind = rng.integers(0, 3, len(near))
    near.loc[kind == 0, "order_status"] = "Pending"                 # stale status snapshot
    near.loc[kind == 1, "payment_method"] = None
    near.loc[kind == 2, "shipping_cost"] = None
    orders = pd.concat([orders, near], ignore_index=True)
    manifest["orders.near_duplicates"] = len(near)

    inject_nulls(rng, orders, {"total_amount": 0.02, "discount_amount": 0.015, "shipping_cost": 0.015,
                               "payment_method": 0.02, "order_status": 0.01, "order_date": 0.005,
                               "customer_id": 0.005, "order_id": 0.003}, manifest, "orders")
    pad_whitespace(rng, orders, ["order_status", "payment_method", "total_amount", "customer_id"], 0.05,
                   manifest, "orders")
    orders = shuffle(rng, add_exact_duplicates(rng, orders, 0.02, manifest, "orders"))

    # ---- order_items quality issues
    items = pd.DataFrame({
        "order_item_id": np.arange(1, m_items + 1).astype(str).astype(object),
        "order_id": item_order.astype(str).astype(object),
        "product_id": item_product.astype(str).astype(object),
        "quantity": qty.astype(str).astype(object),
        "unit_price": money(item_price),
    })
    manifest["order_items.base_records"] = m_items
    m = mask(rng, m_items, 0.005)
    items.loc[m, "product_id"] = rng.integers(n_products + 1, n_products + 20000, int(m.sum())).astype(str)
    manifest["order_items.orphan_product_id"] = int(m.sum())
    m = mask(rng, m_items, 0.005)
    items.loc[m, "quantity"] = rng.choice(["0", "-1", "-2"], int(m.sum()))
    manifest["order_items.invalid_quantity"] = int(m.sum())
    idx = rng.choice(m_items, size=int(m_items * 0.01), replace=False)
    near = items.iloc[idx].copy().reset_index(drop=True)
    near["unit_price"] = None
    items = pd.concat([items, near], ignore_index=True)
    manifest["order_items.near_duplicates"] = len(near)
    inject_nulls(rng, items, {"unit_price": 0.01, "product_id": 0.004, "order_id": 0.002,
                              "order_item_id": 0.002}, manifest, "order_items")
    pad_whitespace(rng, items, ["product_id", "quantity", "unit_price"], 0.03, manifest, "order_items")
    items = shuffle(rng, add_exact_duplicates(rng, items, 0.02, manifest, "order_items"))
    return orders, items


# --------------------------------------------------------------------------- main
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Generate raw retail data with quality issues.")
    p.add_argument("--records", type=int, default=int(os.getenv("PIPELINE_RECORDS", 100000)),
                   help="base records per entity (customers, products, orders)")
    p.add_argument("--output-dir", required=True, help="folder for this run's raw files")
    p.add_argument("--seed", type=int, default=int(os.getenv("PIPELINE_SEED", 42)))
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = parse_args(argv)
    if args.records < 100:
        log.error("--records must be at least 100")
        return 2
    t0 = time.time()
    os.makedirs(args.output_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    fake = Faker("en_US")
    Faker.seed(args.seed)
    manifest: dict = {"generated_at": datetime.utcnow().isoformat() + "Z", "seed": args.seed,
                      "records_per_entity": args.records}

    log.info("Generating %s customers", f"{args.records:,}")
    customers, customer_types = generate_customers(args.records, rng, fake, manifest)
    write_csv_with_corruption(rng, customers, os.path.join(args.output_dir, "customers.csv"),
                              ["THIS,LINE,IS,BROKEN,,,,,,,,,", "corrupted_row_without_delimiters",
                               '1234,"unterminated quote,Smith,a@b.com', "x,y"])
    manifest["customers.malformed_lines"] = 4

    log.info("Generating %s products", f"{args.records:,}")
    product_lines, prices = generate_products(args.records, rng, fake, manifest)
    with open(os.path.join(args.output_dir, "products.json"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(product_lines) + "\n")

    log.info("Generating %s orders + order items", f"{args.records:,}")
    orders, items = generate_orders(args.records, customer_types, prices, rng, manifest)
    write_csv_with_corruption(rng, orders, os.path.join(args.output_dir, "orders.csv"),
                              ["ORDER,BROKEN", "!!!!", "1,2,3,4,5,6,7,8,9,10,11,12"])
    manifest["orders.malformed_lines"] = 3
    write_csv_with_corruption(rng, items, os.path.join(args.output_dir, "order_items.csv"),
                              ["oops", "1,2,3,4,5,6,7"])
    manifest["order_items.malformed_lines"] = 2

    manifest["raw_rows"] = {
        "customers": len(customers) + 4, "products": len(product_lines),
        "orders": len(orders) + 3, "order_items": len(items) + 2,
    }
    with open(os.path.join(args.output_dir, "generation_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)

    for entity, rows in manifest["raw_rows"].items():
        log.info("  %-12s %9s raw rows", entity, f"{rows:,}")
    log.info("Done in %.1fs -> %s", time.time() - t0, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
