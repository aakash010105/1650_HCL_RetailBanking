# Infrastructure as code

Everything the pipeline needs in AWS (`ap-southeast-2`), defined in this folder.

| Path | Contents |
|---|---|
| `deploy/template.prod.yaml` | Production stack: buckets, KMS key, Object Lock, queues, Lambda, Glue job, Step Functions, Glue tables, Athena workgroup, alarms, and the Athena KPI views |
| `deploy/template.yaml` | Development stack (no Object Lock or KMS) |
| `deploy/deploy.py` | Deploy, package, land batches, and create dev views |
| `deploy/glue/pipeline_runner.py` | Glue job entry point |
| `deploy/build_dashboard.py` | Builds the KPI dashboard from Athena |
| `pipeline/` | Cleansing, SCD2, and fact code packaged for Glue |
| `sql/` | The 13 KPI queries. Applied to Athena as views by the stack. |

## Production: first deploy

Run from this `iac/` folder.

```bash
python deploy/deploy.py --env prod stack          # tables, buckets, queues, job, workflow
python deploy/deploy.py --env prod package        # uploads pipeline code and sql/ to S3
python deploy/deploy.py --env prod stack --with-views   # creates the 13 KPI views and v_fact
```

The first `stack` runs without views because the SQL files are not in S3 yet. `package` uploads them.
`stack --with-views` then creates the views through a custom resource.

## Changing a KPI

1. Edit the file in `sql/`.
2. Run `python deploy/deploy.py --env prod package`.
3. Run `python deploy/deploy.py --env prod stack --with-views`.

The `SqlVersion` parameter is a hash of `sql/`. Any change to the SQL changes it, so the views are re-created.

## Notes

- The views depend on the Glue tables in the same stack, so the stack creates them in order.
- On stack delete, the custom resource does nothing and the views remain in Athena. Drop them manually if the database is removed.
- Dates used by the SQL are parameters: `AsOfDate` (`{as_of}`) and `Day2Cutoff` (`{cutoff}`).
- The development stack still creates views with `deploy.py --env dev athena`.
