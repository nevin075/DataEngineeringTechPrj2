Spark master: `local[*]` - session startup 3.0s (excluded)

| Records | pandas total (s) | Spark total (s) | Spark speed-up (pandas ÷ Spark) | pandas orders out | Spark orders out |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.07 | 2.41 | 0.03x | 935 | 937 |
| 10,000 | 0.27 | 3.02 | 0.09x | 9,274 | 9,277 |
| 100,000 | 3.26 | 8.55 | 0.38x | 92,947 | 92,951 |
| 500,000 | 19.89 | 20.80 | 0.96x | 464,399 | 464,394 |
