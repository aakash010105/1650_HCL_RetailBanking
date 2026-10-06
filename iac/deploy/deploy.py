"""Deploy and run the RetailBank pipeline on AWS.

Usage (from the retailbank folder):
    python deploy/deploy.py stack              create or update the CloudFormation stack
    python deploy/deploy.py package            upload the Glue script and the pipeline zip
    python deploy/deploy.py upload day1        land a batch in the raw bucket (manifest.json last)
    python deploy/deploy.py upload day2
    python deploy/deploy.py athena             create the Athena database, tables, and KPI views
    python deploy/deploy.py status             show batch_control and recent executions

Run order: stack -> package -> upload day1 -> wait for LOADED -> upload day2 -> wait -> athena.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import config  # noqa: E402
from pipeline.kpi import KPI_VIEWS, _sql  # noqa: E402

REGION = "ap-southeast-2"
ENVIRONMENTS = {"dev": "deploy/template.yaml", "prod": "deploy/template.prod.yaml"}

# Environment is chosen with --env (default dev). Prod runs the hardened template.
ENV = "dev"
if "--env" in sys.argv:
    i = sys.argv.index("--env")
    ENV = sys.argv[i + 1]
    del sys.argv[i:i + 2]
if ENV not in ENVIRONMENTS:
    sys.exit(f"unknown --env {ENV!r}; choose one of {', '.join(ENVIRONMENTS)}")

STACK = f"retailbank-{ENV}"
DATABASE = f"retailbank_{ENV}"
OUT = ROOT / "out"

session = boto3.Session(region_name=REGION)
ACCOUNT = session.client("sts").get_caller_identity()["Account"]
RAW = f"retailbank-raw-{ENV}-{ACCOUNT}"
LAKE = f"retailbank-lake-{ENV}-{ACCOUNT}"
ARTIFACTS = f"retailbank-artifacts-{ENV}-{ACCOUNT}"
CONTROL_TABLE = f"retailbank-{ENV}-batch-control"
JOB = f"retailbank-{ENV}-batch"
STATE_MACHINE = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:retailbank-{ENV}-batch"


def run_aws(*args: str) -> str:
    return subprocess.run(["aws", *args, "--region", REGION], check=True, capture_output=True, text=True).stdout


def cmd_stack():
    params = [f"Env={ENV}"]
    if ENV == "prod":
        # Optional alert address for prod. Set ALERT_EMAIL in the shell to subscribe it.
        import os
        params.append(f"AlertEmail={os.environ.get('ALERT_EMAIL', '')}")
    out = subprocess.run(
        ["aws", "cloudformation", "deploy", "--region", REGION, "--stack-name", STACK,
         "--template-file", str(ROOT / ENVIRONMENTS[ENV]),
         "--parameter-overrides", *params, "--capabilities", "CAPABILITY_IAM",
         "--no-fail-on-empty-changeset"],
        capture_output=True, text=True)
    print(out.stdout or out.stderr)
    if out.returncode != 0:
        sys.exit(out.returncode)


def cmd_package():
    s3 = session.client("s3")
    zip_path = ROOT / "deploy" / "pipeline.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for py in sorted((ROOT / "pipeline").glob("*.py")):
            z.write(py, f"pipeline/{py.name}")
        for sql_file in sorted((ROOT / "sql").glob("*.sql")):
            z.write(sql_file, f"sql/{sql_file.name}")
    s3.upload_file(str(zip_path), ARTIFACTS, "glue/pipeline.zip")
    s3.upload_file(str(ROOT / "deploy" / "glue" / "pipeline_runner.py"), ARTIFACTS, "glue/pipeline_runner.py")
    print(f"uploaded pipeline.zip ({zip_path.stat().st_size} bytes) and pipeline_runner.py to {ARTIFACTS}")


def cmd_upload(day: str):
    batch = config.BATCHES[day]
    s3 = session.client("s3")
    prefix = f"load_type={batch['load_type']}/dt={batch['dt']}/batch_id={batch['batch_id']}"
    files = []
    for entity, fname in batch["files"].items():
        path = ROOT / "Retailbank_SourceData" / fname
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        rows = None
        if fname.endswith(".csv"):
            rows = max(sum(1 for _ in open(path, encoding="utf-8")) - 1, 0)
        files.append({"entity": entity, "key": fname, "sha256": digest, "bytes": len(data), "row_count": rows})
        s3.put_object(Bucket=RAW, Key=f"raw/{prefix}/{fname}", Body=data)
        print(f"  landed {fname} ({len(data)} bytes)")
    manifest = {"batch_id": batch["batch_id"], "load_type": batch["load_type"], "dt": batch["dt"],
                "cutoff_ts": batch["cutoff_ts"], "schema_version": "1.0", "files": files}
    s3.put_object(Bucket=RAW, Key=f"raw/{prefix}/manifest.json",
                  Body=json.dumps(manifest, indent=2).encode("utf-8"))
    print(f"manifest.json written last: s3://{RAW}/raw/{prefix}/manifest.json  (event fires now)")


def arrow_to_hive(t) -> str:
    import pyarrow as pa
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_date(t):
        return "date"
    return "string"


def hive_columns(parquet_file: Path) -> str:
    schema = pq.read_schema(parquet_file)
    return ", ".join(f"`{f.name}` {arrow_to_hive(f.type)}" for f in schema)


def sample(path: Path) -> Path:
    return next(path.glob("*.parquet")) if path.is_dir() else path


def athena(sql: str, database: str | None = None) -> None:
    client = session.client("athena")
    ctx = {"Catalog": "AwsDataCatalog"}
    if database:
        ctx["Database"] = database
    qid = client.start_query_execution(
        QueryString=sql, WorkGroup=f"retailbank-{ENV}",
        QueryExecutionContext=ctx)["QueryExecutionId"]
    for _ in range(120):
        state = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {state['State']}: {state.get('StateChangeReason')}\n{sql[:200]}")


def cmd_athena():
    athena(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    tables = {
        "fact_transaction": (f"gold/fact_transaction/", OUT / "gold" / "fact_transaction" / "part-0000.parquet"),
        "dim_customer": ("gold/dim_customer/", OUT / "gold" / "dim_customer" / "part-0000.parquet"),
        "dim_branch": ("gold/dim_branch/", OUT / "gold" / "dim_branch" / "part-0000.parquet"),
        "dim_product": ("gold/dim_product/", OUT / "gold" / "dim_product" / "part-0000.parquet"),
        "dq_scorecard": ("control/dq_scorecard/", sample(OUT / "control" / "dq_scorecard")),
        "batch_changes": ("serving/batch_changes/", OUT / "serving" / "batch_changes" / "batch_changes.parquet"),
        "kpi_snapshot": ("serving/kpi_snapshot/", OUT / "serving" / "kpi_snapshot" / "kpi_snapshot.parquet"),
    }
    for entity in ["branches", "products", "customers", "transactions"]:
        tables[f"silver_{entity}"] = (f"silver/{entity}/", sample(OUT / "silver" / entity))
        tables[f"quarantine_{entity}"] = (f"quarantine/{entity}/", sample(OUT / "quarantine" / entity))

    # Prod: the tables are owned by the CloudFormation stack, so this script only manages views there.
    for name, (prefix, schema_file) in (tables.items() if ENV == "dev" else []):
        athena(f"DROP TABLE IF EXISTS {DATABASE}.{name}")
        athena(f"CREATE EXTERNAL TABLE {DATABASE}.{name} ({hive_columns(schema_file)}) "
               f"STORED AS PARQUET LOCATION 's3://{LAKE}/{prefix}'")
        print(f"table {name} -> s3://{LAKE}/{prefix}")

    # Views: same SQL as the local DuckDB serving layer, with table names qualified for Athena.
    import re
    names = sorted(tables)
    base = ("SELECT transaction_id, account_id, product_id, branch_id, customer_sk, product_sk, branch_sk, "
            "transaction_date, \"timestamp\" AS txn_ts, amount, status, currency, is_refund, payment_method, "
            "is_correction, first_batch_id, loaded_batch_id FROM fact_transaction")
    athena(f"CREATE OR REPLACE VIEW {DATABASE}.v_fact AS {qualify(base, names, DATABASE)}")
    for view in KPI_VIEWS:
        body = qualify(_sql(view), names + ["v_fact"] + KPI_VIEWS, DATABASE)
        athena(f"CREATE OR REPLACE VIEW {DATABASE}.{view} AS {body}")
        print(f"view {view}")


def qualify(sql: str, names: list[str], database: str) -> str:
    import re
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    text = "\n".join(lines)
    for name in sorted(set(names), key=len, reverse=True):
        text = re.sub(rf"(?<![\w.]){name}(?![\w])", f"{database}.{name}", text)
    return text


def cmd_status():
    ddb = session.client("dynamodb")
    resp = ddb.scan(TableName=CONTROL_TABLE)
    for item in resp.get("Items", []):
        print(item["batch_id"]["S"], item.get("status", {}).get("S"), item.get("error", {}).get("S", ""))
    sfn = session.client("stepfunctions")
    try:
        execs = sfn.list_executions(stateMachineArn=STATE_MACHINE, maxResults=5)["executions"]
        for e in execs:
            print("execution", e["name"], e["status"])
    except sfn.exceptions.StateMachineDoesNotExist:
        print("state machine not deployed yet")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "stack":
        cmd_stack()
    elif cmd == "package":
        cmd_package()
    elif cmd == "upload":
        cmd_upload(sys.argv[2])
    elif cmd == "athena":
        cmd_athena()
    elif cmd == "status":
        cmd_status()
    else:
        sys.exit(__doc__)
