-- Runs once, the first time the postgres container starts with an empty volume.
-- The Airflow metadata DB (AIRFLOW_DB) is created by the image itself; this adds the
-- data-warehouse database and its tables. If you change WAREHOUSE_DB in .env, change it here too
-- (the loader also creates the database automatically if it is missing).
CREATE DATABASE retail_dw;
\connect retail_dw
\i /sql/schema.sql
