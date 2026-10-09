# big_data_fw

Repo for the Big Data labs, done on Onyxia with the `vscode-pyspark` service.

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
- [ ] Final upload to `bronze/`: blocked by a server-side error (see below) but finally worked 

### Issues

- **Onyxia quota**: at first the Job couldn't create any pod (`must specify limits.cpu`). I fixed it by adding `limits.cpu: 500m` to the YAML.
- **S3 server error**: uploads to my bucket failed with `InternalError` (500), which is a server-side error. I tested from the terminal, with s5cmd and from the Job, on different prefixes, even with a 6-byte file. The rest of my setup works (read, delete, bucket policy). I reported it to the teacher and will rerun the Job once it's fixed.

### Questions

**Why a Secret and not a ConfigMap for the credentials?**
A ConfigMap is meant for non-sensitive config and is readable in plain text. A Secret is meant for sensitive data: access can be restricted and the values are hidden by default.

**What happens if the Job runs tomorrow?**
The credentials will have expired, so the Job will fail. In production, you would give the pod its own identity through its ServiceAccount, or use a secret manager like Vault that renews the credentials automatically.

**How to make it a daily ingestion?**
Turn the Job into a `CronJob` with a schedule like `"0 2 * * *"` (every day at 2am).