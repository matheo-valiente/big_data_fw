# big_data_fw

Repo for the Big Data labs, done on Onyxia with the `vscode-pyspark` service.
Labs completed: uv, S3, DuckDB, dbt, Spark RDD, Spark DataFrames, Polaris deployment and Polaris governance.

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

## Lab: Bronze to silver with Spark DataFrames

Same silver layer as the dbt lab, built with PySpark (DataFrame API and Spark SQL), written to S3 as Parquet and as Iceberg tables.

- **Session**: `SparkSession` in `local[*]` with the S3A connector (`TemporaryAWSCredentialsProvider`, path-style access) and an Iceberg catalog `lab` of type `hadoop` on `s3a://user-m-valiente-ece/warehouse`
- **Bronze**: `users.csv` and `orders.csv` read directly from S3 (`multiLine=True` for the 2-line addresses), cached
- **Silver**: `stg_users` (address split into street, city, state, zip code with `regexp_extract`, empty city replaced by `NULL`) and `stg_orders` (typed, deduplicated with `row_number()` over a window by `order_id`)
- **Parquet**: `s3://user-m-valiente-ece/silver/spark/{users,orders}.parquet/`
- **Iceberg**: tables `lab.silver.users` (50 rows) and `lab.silver.orders` (2829 rows, same count as dbt)
- **Git**: only the Iceberg metadata is committed (`warehouse/silver/*/metadata`), not the data files

```bash
pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
```

### Issues

- **403 Forbidden from Spark** while the `aws` CLI worked: the S3A connector kept a cached `S3AFileSystem` built with expired credentials, even after `spark.stop()`. Fixed by restarting the JVM, reading fresh credentials and setting `spark.hadoop.fs.s3a.impl.disable.cache=true`.
- **Resources**: the service had 2 CPU / 2000Mi and Spark jobs got stuck. Recreated `vscode-pyspark` with the required limits `cpu: 4000m`, `memory: 4000Mi`.
- **Missing AWS profile** in the new service: used `aws s3 sync ... --endpoint-url "https://$AWS_S3_ENDPOINT"` with the environment credentials.



## Lab: Deploy Polaris with Helm (k8s)

Polaris (Iceberg REST catalog) and its PostgreSQL database deployed in my namespace with a Helm chart, as a prerequisite for the Polaris lab.

- **Resources**: `vscode-pyspark` patched with `kubectl patch` to `limits` 2000m / 2500Mi and `requests` 50m / 200Mi, to leave quota for Polaris
- **Helm chart** `polaris/deployment/`:
  - `Chart.yaml`: dependencies `polaris` 1.8.0 and `postgresql` 18.12.15 (Bitnami)
  - `values.yaml`: JDBC persistence on PostgreSQL, S3 credentials from a Secret, init container that bootstraps the `root` principal
  - `templates/bootstrap-job.yaml`: post-install Job that creates principals, roles and the catalog through the management API
  - `templates/polaris-db-secret.yaml`: database Secret with an auto-generated password
- **Kubernetes objects**: ConfigMap `polaris-config`, Secrets `polaris-credential` (root) and `polaris-s3` (S3 credentials)
- **Deployment**: `helm upgrade --install polaris polaris/deployment --wait` gives `STATUS: deployed`, and `/q/health` returns `UP`
- **Catalog**: `user-m-valiente-ece`, base location `s3://user-m-valiente-ece/warehouse/`
- **RBAC** created by the bootstrap Job:

| Principal | Principal role | Catalog role | Access |
|---|---|---|---|
| `dev` | `data_engineer` | `catalog_contributor` | manage content, metadata and access |
| `analyst` | `data_analyst` | `catalog_reader` | read tables and properties |

- **Credentials**: `dev` and `analyst` credentials extracted from the Job logs to `~/.polaris-credentials.env`, outside the repo and not committed

```bash
set -a; . /home/onyxia/.polaris-credentials.env; set +a
curl -s http://polaris-mgmt:8182/q/health
```

### Issues

- **Patch not applied**: `kubectl rollout status` stayed on `0 out of 1 new pods have been updated` because of the StatefulSet update strategy. Fixed by deleting the pod (`kubectl delete pod vscode-pyspark-228462-0`); the StatefulSet recreated it with the new limits and the volume was kept.
- **Missing AWS profile**: no `default` profile in the new service, so the `aws` CLI was used with `--endpoint-url "https://$AWS_S3_ENDPOINT"` and the environment credentials.

## Lab: Govern Iceberg tables with Polaris

Spark connected to the Polaris catalog through the Iceberg REST API, with two Spark catalogs in the same session: `lab` (Hadoop catalog of the Spark lab) and `polaris` (REST catalog, authenticated as `dev` or `analyst`).

- **Register**: `lab.silver.orders` registered in Polaris with `register_table` on `v1.metadata.json`. No file was written in the bucket, only a pointer in the Polaris database. Count through Polaris: 2829 orders.
- **Create as `dev`**: `polaris.silver.users` created from `lab.silver.users` (50 rows). Polaris wrote it under `warehouse/silver/users/` with its own naming (`00000-<uuid>.metadata.json`).
- **Schema change as `dev`**: `ALTER TABLE ... ADD COLUMN country` created a new metadata file `00001-<uuid>.metadata.json`, without rewriting any data file.
- **Read as `analyst`**: `SHOW TABLES` and `SELECT` work. `INSERT` and `ALTER TABLE` are denied with `ForbiddenException`, for example `not authorized for op ADD_TABLE_SCHEMA` through `data_analyst` → `catalog_reader`. No snapshot was created by the failed `INSERT`.
- **Git**: new Polaris metadata files of `silver/users` committed.
- **Cleanup**: `polaris.silver.users` dropped, `polaris.silver.orders` kept for the Trino lab.

### Issues

- **No `default` AWS profile**: `aws configure export-credentials --profile default` was skipped. The environment credentials of the service were used, with `--endpoint-url "https://$AWS_S3_ENDPOINT"` for the `aws` CLI.
- **Switching principal**: the PySpark shell was exited and relaunched to start a fresh session with the other credential.