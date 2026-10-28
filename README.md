# big_data_fw

Repo for the Big Data labs, done on Onyxia with the `vscode-pyspark` service.
All four labs are completed: uv, S3, DuckDB and dbt.

## Lab 1 – uv

I set up a Python project with `uv` (src layout, dependencies locked in `uv.lock`). It exposes two commands that generate fake datasets:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
```

## Lab 2 – S3

### Checklist

- [x] AWS profile configured from Onyxia, endpoint and bucket variables set
- [x] Bucket `user-m-valiente-ece` checked (SeaweedFS backend)
- [x] `s5cmd` installed in `~/.local/bin` with checksum verification
- [x] Upload, list (`aws s3` and `s5cmd`), download + `diff`, rename (`mv`), delete
- [x] Bucket policy blocking deletes in `bronze/` applied, checked with `get-bucket-policy`, then removed (`s3/policy.json`)
- [x] Cleanup of test objects and local files
- [x] `*.csv` added to `.gitignore`
- [x] ConfigMaps `datasets` and `s3-config`, Secret `s3-credentials` created
- [x] Job `k8s/job-upload-bronze.yaml` written and running (pods start, read the data and reach S3)
- [x] Final upload of `users.csv` and `orders.csv` to `bronze/`, done after the platform fix on October 12, 2026

### Issues

- **Onyxia quota**: at first the Job couldn't create any pod (`must specify limits.cpu`). I fixed it by adding `limits.cpu: 500m` to the YAML.
- **S3 server error**: uploads to my bucket failed with `InternalError` (500), a server-side error. I tested from the terminal, with s5cmd and from the Job, on different prefixes, even with a 6-byte file, while reads, deletes and bucket policies worked. I reported it to the teacher. The platform was fixed on October 12, 2026, and the upload to `bronze/` then worked.

### Questions

**Why a Secret and not a ConfigMap for the credentials?**
A ConfigMap is meant for non-sensitive config and is readable in plain text. A Secret is meant for sensitive data: access can be restricted and the values are hidden by default.

**What happens if the Job runs tomorrow?**
The credentials will have expired, so the Job will fail. In production, you would give the pod its own identity through its ServiceAccount, or use a secret manager like Vault that renews the credentials automatically.

**How to make it a daily ingestion?**
Turn the Job into a `CronJob` with a schedule like `"0 2 * * *"` (every day at 2am).

## Lab 3 – DuckDB

- Queried the bronze CSV files directly on S3 with `read_csv`, using the `s3_onyxia_connection` secret
- Loaded the `users` and `orders` tables in `analytics.duckdb`, data quality checks with `ANTI JOIN`
- Aggregations, joins, window functions (`QUALIFY`, moving average) and `PIVOT`
- Parquet export, footer metadata, Hive partitioning by `product` (only 1 of 6 files scanned)
- CSV vs Parquet comparison on a large dataset: Parquet only downloads the needed columns
- `orders-report` command using the DuckDB Python API with query parameters

```bash
uv run orders-report -p cookie
```

## Lab 4 – dbt

dbt project `lab_medallion` with the `dbt-duckdb` adapter, building the silver layer from the bronze CSV files.

- **Bronze**: `models/bronze/sources.yml`, the path is read from the `BRONZE_PATH` environment variable
- **Silver**: `stg_users` (typed, address split into street, city, state and zip code) and `stg_orders` (typed, deduplicated with `QUALIFY`)
- **Tests**: generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) and 2 singular tests
- **Result**: `dbt build --select silver` gives `PASS=15 WARN=1 ERROR=0`
- **Docs**: generated with `dbt docs generate --static`
- **Export**: silver models exported as Parquet to `s3://user-m-valiente-ece/silver/dbt/`, with the same row count as the views (2829 orders)

```bash
cd lab_medallion
export BRONZE_PATH="s3://$LAB_BUCKET_NAME/bronze"
uv run dbt build --select silver
```

### Issues

- **Bronze layer before the fix**: while S3 uploads failed, I read the bronze CSV files from a local directory through `BRONZE_PATH`. After the fix, I switched it to S3 without changing the dbt code.
- **Expired DuckDB credentials**: after the fix, dbt failed with `403 InvalidAccessKeyId` because the DuckDB secret still held old credentials. I recreated `s3_onyxia_connection` from the `default` AWS profile.
- **`assert_orders_after_user_birth`**: 288 orders are placed before the user's birthdate, because Faker generates birthdates relative to today while orders are dated 2020. The test is set to `severity='warn'`.
- **`states` seed**: the `relationships` test from `state` to `ref('states')` was removed, since the lab never creates this seed.

## Lab: Spark RDD API

Details: [`lab-rdd/lab_rdd.md`](lab-rdd/lab_rdd.md)

- Read bronze `users.csv` and `orders.csv` with `sc.textFile` and observed partitions and lazy evaluation.
- Handled multiline addresses without a schema (101 lines for 50 users) and parsed orders by hand.
- Compared `reduceByKey` and `groupByKey`, deduplicated orders and joined them with users.
- Exercises: `aggregateByKey`, `leftOuterJoin` with an orphan order, DataFrame comparison, cache timing.
