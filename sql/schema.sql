-- =====================================================================================
-- DATA 430 Project 2 -- PostgreSQL schema for the retail data warehouse (database retail_dw)
--
--   public.customers / products / orders / order_items   cleaned data from Spark (Parquet)
--   analytics.*                                          metrics computed by spark_analytics.py
--   public.etl_audit                                     one row per Airflow pipeline run
--
-- Idempotent: safe to run on every pipeline run (load_postgres.py does that).
-- The constraints are the last line of defence -- if Spark ever lets a bad record through,
-- the load fails with a constraint violation, the transaction rolls back, and the previous
-- good data stays in place.
-- =====================================================================================

-- ------------------------------------------------------------------ cleaned data
CREATE TABLE IF NOT EXISTS customers (
    customer_id        INTEGER       PRIMARY KEY,
    first_name         VARCHAR(100)  NOT NULL,
    last_name          VARCHAR(100)  NOT NULL,
    email              VARCHAR(255)  UNIQUE,                 -- NULL allowed (missing/invalid email)
    phone              VARCHAR(20),
    city               VARCHAR(100)  NOT NULL,
    registration_date  DATE,
    customer_type      VARCHAR(20)   NOT NULL CHECK (customer_type IN ('Regular', 'Premium', 'VIP')),
    dq_flags           TEXT,                                 -- fixes applied by Spark, ';'-separated
    loaded_at          TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id         INTEGER       PRIMARY KEY,
    product_name       VARCHAR(255)  NOT NULL,
    category           VARCHAR(100)  NOT NULL,
    brand              VARCHAR(100)  NOT NULL,
    unit_price         NUMERIC(10,2) CHECK (unit_price >= 0),
    stock_quantity     INTEGER       NOT NULL CHECK (stock_quantity >= 0),
    is_active          BOOLEAN       NOT NULL,
    created_date       DATE,
    dq_flags           TEXT,
    loaded_at          TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id           INTEGER       PRIMARY KEY,
    customer_id        INTEGER       NOT NULL REFERENCES customers (customer_id),
    order_date         TIMESTAMP     NOT NULL,              -- UTC
    order_status       VARCHAR(20)   NOT NULL CHECK (order_status IN ('Pending', 'Shipped', 'Delivered')),
    payment_method     VARCHAR(30)   NOT NULL,
    total_amount       NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    discount_amount    NUMERIC(12,2) NOT NULL CHECK (discount_amount >= 0),
    shipping_cost      NUMERIC(12,2) NOT NULL CHECK (shipping_cost >= 0),
    net_amount         NUMERIC(12,2) NOT NULL,              -- total_amount - discount_amount
    dq_flags           TEXT,
    loaded_at          TIMESTAMP     NOT NULL DEFAULT now(),
    CONSTRAINT chk_discount_not_above_total CHECK (discount_amount <= total_amount)
);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date  ON orders (order_date);

-- New in Project 2: links orders to products (needed for product / category analytics)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id      INTEGER       PRIMARY KEY,
    order_id           INTEGER       NOT NULL REFERENCES orders (order_id),
    product_id         INTEGER       NOT NULL REFERENCES products (product_id),
    quantity           INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price         NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    line_total         NUMERIC(12,2) NOT NULL,
    dq_flags           TEXT,
    loaded_at          TIMESTAMP     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);

-- ------------------------------------------------------------------ pipeline audit
CREATE TABLE IF NOT EXISTS etl_audit (
    audit_id           SERIAL        PRIMARY KEY,
    dag_id             VARCHAR(250)  NOT NULL,
    run_id             VARCHAR(250)  NOT NULL,              -- Airflow run id
    data_run_id        VARCHAR(50),                         -- folder name under data/*/ (ts_nodash)
    status             VARCHAR(20)   NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    start_time         TIMESTAMPTZ,
    end_time           TIMESTAMPTZ,
    duration_seconds   NUMERIC(10,1),
    records_raw        BIGINT,                              -- rows read from the raw files
    records_clean      BIGINT,                              -- rows written by Spark
    records_rejected   BIGINT,                              -- rows dropped by Spark
    rows_loaded        BIGINT,                              -- rows loaded into PostgreSQL (all tables)
    analytics_tables   INTEGER,
    failed_task        VARCHAR(250),
    error_message      TEXT,
    details            JSONB,                               -- per-entity stats, validation results, KPIs
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_etl_audit_run UNIQUE (dag_id, run_id)
);

-- ------------------------------------------------------------------ analytics
CREATE SCHEMA IF NOT EXISTS analytics;

-- Customer analytics: lifetime value, order frequency, average order value
CREATE TABLE IF NOT EXISTS analytics.customer_metrics (
    customer_id        INTEGER       PRIMARY KEY,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    customer_type      VARCHAR(20),
    city               VARCHAR(100),
    order_count        INTEGER,
    lifetime_value     NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2),
    first_order_date   DATE,
    last_order_date    DATE
);

CREATE TABLE IF NOT EXISTS analytics.customer_type_distribution (
    customer_type      VARCHAR(20)   PRIMARY KEY,
    customer_count     BIGINT,
    pct_of_customers   NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics.top_customers (
    revenue_rank       INTEGER       PRIMARY KEY,
    customer_id        INTEGER,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    customer_type      VARCHAR(20),
    city               VARCHAR(100),
    order_count        INTEGER,
    lifetime_value     NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2)
);

-- Product analytics
CREATE TABLE IF NOT EXISTS analytics.product_metrics (
    product_id         INTEGER       PRIMARY KEY,
    product_name       VARCHAR(255),
    category           VARCHAR(100),
    brand              VARCHAR(100),
    is_active          BOOLEAN,
    order_count        INTEGER,                             -- popularity: # orders containing it
    units_sold         BIGINT,
    revenue            NUMERIC(14,2),
    avg_selling_price  NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS analytics.category_performance (
    category           VARCHAR(100)  PRIMARY KEY,
    product_count      BIGINT,
    order_count        BIGINT,
    units_sold         BIGINT,
    revenue            NUMERIC(14,2),
    avg_selling_price  NUMERIC(10,2),
    pct_of_revenue     NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics.inventory_status (
    status             VARCHAR(10)   PRIMARY KEY,
    product_count      BIGINT,
    total_stock_units  BIGINT,
    pct_of_products    NUMERIC(7,2)
);

-- Order analytics
CREATE TABLE IF NOT EXISTS analytics.sales_monthly (
    month              DATE          PRIMARY KEY,
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2),
    total_discount     NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics.sales_daily (
    order_day          DATE          PRIMARY KEY,
    order_count        BIGINT,
    revenue            NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics.order_status_breakdown (
    order_status       VARCHAR(20)   PRIMARY KEY,
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    pct_of_orders      NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics.payment_method_distribution (
    payment_method     VARCHAR(30)   PRIMARY KEY,
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    pct_of_revenue     NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics.discount_summary (
    order_count                            BIGINT,
    orders_with_discount                   BIGINT,
    pct_orders_discounted                  NUMERIC(7,2),
    avg_discount_rate_pct                  NUMERIC(7,2),
    avg_discount_rate_when_discounted_pct  NUMERIC(7,2),
    total_discount                         NUMERIC(14,2)
);

-- Cross-entity analytics
CREATE TABLE IF NOT EXISTS analytics.revenue_by_customer_type (
    customer_type      VARCHAR(20)   PRIMARY KEY,
    customer_count     BIGINT,
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2),
    pct_of_revenue     NUMERIC(7,2)
);

CREATE TABLE IF NOT EXISTS analytics.top_categories_by_customer_type (
    customer_type      VARCHAR(20),
    category_rank      INTEGER,
    category           VARCHAR(100),
    order_lines        BIGINT,
    units_sold         BIGINT,
    revenue            NUMERIC(14,2),
    PRIMARY KEY (customer_type, category_rank)
);

CREATE TABLE IF NOT EXISTS analytics.city_revenue (
    city               VARCHAR(100)  PRIMARY KEY,
    customer_count     BIGINT,
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2)
);

-- Extra analytics (bonus)
CREATE TABLE IF NOT EXISTS analytics.brand_performance (
    revenue_rank       INTEGER       PRIMARY KEY,
    brand              VARCHAR(100),
    products_sold      INTEGER,
    order_count        INTEGER,
    units_sold         BIGINT,
    revenue            NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics.sales_by_weekday (
    weekday_num        INTEGER       PRIMARY KEY,           -- 1 = Sunday ... 7 = Saturday
    weekday            VARCHAR(10),
    order_count        BIGINT,
    revenue            NUMERIC(14,2),
    avg_order_value    NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS analytics.repeat_customers (
    customers_with_orders    BIGINT,
    repeat_customers         BIGINT,
    one_time_customers       BIGINT,
    repeat_rate_pct          NUMERIC(7,2),
    avg_orders_per_customer  NUMERIC(7,2)
);

-- Handy view for the demo: latest pipeline runs
CREATE OR REPLACE VIEW analytics.v_recent_pipeline_runs AS
SELECT audit_id, run_id, status, start_time, end_time, duration_seconds,
       records_raw, records_clean, records_rejected, rows_loaded, failed_task
FROM etl_audit
ORDER BY start_time DESC NULLS LAST
LIMIT 20;
