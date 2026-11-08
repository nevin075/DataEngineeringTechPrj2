# Sample results (reference only)

Produced while testing the pipeline with the default settings (100,000 records per entity, seed 42)
on a 2-core Linux machine. Your own runs write the same files under `data/reports/<run_id>/` and
`data/reports/benchmark/`; with the same seed the cleansing counts should match closely.

- `cleansing_summary.csv` - records in/out/dropped per entity, with reasons and fixes
- `analytics_summary.json` - row count of every analytics output + headline KPIs
- `benchmark_results.*`, `benchmark_chart.png` - pandas vs Spark (local[*]) timings
