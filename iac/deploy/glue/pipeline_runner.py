"""AWS Glue Python shell entry point. Runs one batch with the same pipeline code used locally.

Inputs (job arguments, passed by Step Functions):
    BATCH_ID, MANIFEST_KEY (raw bucket key of manifest.json)
Job defaults:
    RAW_BUCKET, LAKE_BUCKET, CONTROL_TABLE

Steps:
    1. Read the manifest and download the batch's source files from the raw bucket.
    2. Download the current Gold state, control files, and KPI snapshots from the lake.
    3. Run the batch (pipeline.run_local.run). Gold is all-or-nothing.
    4. Write the outcome to batch_control, then upload silver, quarantine, gold, control and serving.
"""
import json
import os
import shutil
import sys

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["BATCH_ID", "MANIFEST_KEY", "RAW_BUCKET", "LAKE_BUCKET", "CONTROL_TABLE",
                                     "ARTIFACTS_BUCKET"])

WORK = "/tmp/retailbank"
SRC = f"{WORK}/src"
OUT = f"{WORK}/out"
os.environ["RB_SRC"] = SRC
os.environ["RB_OUT"] = OUT
os.environ["RB_SQL"] = f"{WORK}/app/sql"

s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")


def download_prefix(bucket, prefix, dest):
    """Copy every object under prefix to dest, keeping the relative path."""
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(prefix):]
            if not rel or rel.endswith("/"):
                continue
            path = os.path.join(dest, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            s3.download_file(bucket, obj["Key"], path)


def upload_tree(root, bucket, prefix="", skip=()):
    """Upload every file under root to s3://bucket/prefix/<relative path>.

    prefix keeps the S3 folder when root is a sub-folder (e.g. out/control -> control/).
    skip drops top-level names under root (e.g. bronze) and the local DuckDB file.
    """
    for dirpath, _, files in os.walk(root):
        for name in files:
            local = os.path.join(dirpath, name)
            rel = os.path.relpath(local, root).replace(os.sep, "/")
            if rel.split("/")[0] in skip or rel.endswith(".duckdb"):
                continue
            key = f"{prefix}/{rel}" if prefix else rel
            s3.upload_file(local, bucket, key)


def set_status(batch_id, status, **extra):
    names = {"#s": "status"}
    values = {":s": {"S": status}}
    expr = ["#s = :s"]
    for i, (k, v) in enumerate(extra.items()):
        names[f"#k{i}"] = k
        values[f":v{i}"] = {"S": str(v)}
        expr.append(f"#k{i} = :v{i}")
    ddb.update_item(TableName=args["CONTROL_TABLE"], Key={"batch_id": {"S": batch_id}},
                    UpdateExpression="SET " + ", ".join(expr),
                    ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def main():
    batch_id = args["BATCH_ID"]
    manifest_key = args["MANIFEST_KEY"]
    raw, lake = args["RAW_BUCKET"], args["LAKE_BUCKET"]

    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(SRC)
    os.makedirs(OUT)

    manifest = json.loads(s3.get_object(Bucket=raw, Key=manifest_key)["Body"].read())
    folder = manifest_key.rsplit("/", 1)[0]
    for f in manifest["files"]:
        s3.download_file(raw, f"{folder}/{f['key']}", os.path.join(SRC, f["key"]))

    # Cumulative state from earlier batches.
    download_prefix(lake, "gold/", os.path.join(OUT, "gold"))
    download_prefix(lake, "control/", os.path.join(OUT, "control"))
    download_prefix(lake, "serving/kpi_snapshot/", os.path.join(OUT, "serving", "kpi_snapshot"))

    batch = {
        "batch_id": manifest["batch_id"],
        "load_type": manifest["load_type"],
        "dt": manifest["dt"],
        "cutoff_ts": manifest["cutoff_ts"],
        "files": {f["entity"]: f["key"] for f in manifest["files"]},
    }

    # Python shell jobs do not always put --extra-py-files on sys.path, so load the zip explicitly.
    # The zip holds pipeline/ and sql/. Extract it so the KPI SQL files are real files on disk.
    import zipfile
    zip_path = f"{WORK}/pipeline.zip"
    app_dir = f"{WORK}/app"
    s3.download_file(args["ARTIFACTS_BUCKET"], "glue/pipeline.zip", zip_path)
    zipfile.ZipFile(zip_path).extractall(app_dir)
    sys.path.insert(0, app_dir)

    from pipeline import run_local  # imported after RB_SRC / RB_OUT are set

    try:
        results = run_local.run(batch)
    except Exception as exc:
        set_status(batch_id, "FAILED", error=str(exc)[:900])
        upload_tree(os.path.join(OUT, "control"), lake, prefix="control")
        upload_tree(os.path.join(OUT, "quarantine"), lake, prefix="quarantine")
        raise

    set_status(batch_id, "LOADED", results=json.dumps(results, default=str)[:3500])
    upload_tree(OUT, lake, skip=("bronze", "gold_before_batch"))
    print(f"[{batch_id}] published to s3://{lake}")


main()
