# Operations

## Tracker statuses

| Status | Meaning | What to do |
|---|---|---|
| `applied` | Done. `notes` may contain `SKIPPED: ...` for no-op renames | Nothing |
| `backfilling` | Glue backfill running; `notes` holds the run id | Wait. The next apply run continues the table once the job ends |
| `waiting_for_table` | Target table does not exist yet | Create the table; the next pass applies the plan |
| `failed` | DDL or backfill error; `notes` has the message | Fix the cause and re-run apply. Failed plans are retried |

To see a table's state:

```sql
SELECT plan_name, status, applied_at, notes
FROM schema_evolution_metadata.plan_tracker
WHERE table_name = 'demo_customers' ORDER BY plan_name;
```

## Common tasks

**Add a column in every environment.** Add the field to the schema YAML, run `schema-evolution-plan` (or write an `add_column` plan), commit, and deploy.

**Add a column in only some environments, with data.**

1. Mark the field `environments: [qa]` in the YAML.
2. Run `schema-evolution-generate-env-plans --envs qa --account_id <id>` and commit the plan. CI adds the empty column.
3. Run `schema-evolution-trigger-backfill --environment qa --account_id <id> --dry_run`, then run it again without `--dry_run`. This fills the column from Bronze with a MERGE. The table's schema YAML must list `merge_key: true` fields.

The MERGE deduplicates Bronze on `--dedup_col` (default `dms_change_seq`, falling back to `dms_commit_ts`). Pass your CDC sequence column if yours is named differently.

**Change a column's type.**

- Widening only: use `compatible_type_change`.
- Anything else: use `incompatible_type_change`, adding a `transformation` if a plain CAST is not enough. If the column backs a partition field, add `new_partition`.
- Pause the table's writers while the backfill runs. The rebuild aborts rather than lose rows if a writer commits during the copy.

**Rename a table.** Write a plan containing only `rename_table` under the *new* table name's directory.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `LockNotAcquiredError` from the CLI | Another deploy holds `locks/{env}.lock`. It expires after 20 minutes if the holder died. Check that no other deploy is running before deleting the object |
| `checksum mismatch` for an applied plan | Someone edited an applied plan. Revert the edit and add a new plan. Use `--checksum_mode warn` only to recover deliberately |
| `metadata.layer ... does not match` | The plan is in the wrong directory |
| `PartitionSourceColumnError` | The type change targets a partition column. Add `"new_partition": {"transform": ...}` |
| `Unrecognized type: ...` | Unsupported type string. See the type list in plan-reference.md |
| Log says `Backfill still running (Glue run ...)` | Expected while a backfill runs. The deploy still succeeds; the table's remaining plans apply on the next deploy after the job finishes |
| Backfill stays `backfilling` | Check the Glue run in `notes`. A dead run is reset to `failed` and retried on the next apply run. To continue without waiting for a deploy, run `schema-evolution-apply` by hand |
| Backfill failed with `Concurrent write detected` | A writer committed during the rebuild. The original table is intact; pause writers and re-run |
| `Rebuild row-count mismatch` | The rebuilt `<table>_rebuild` is kept for inspection. Drop it after investigating, then re-run |
| `AccessDenied` on S3 Tables | Check the KMS key policy and any Lake Formation grants (installation.md, end of step 2.6) |

## Known limitations

- Two "safe widening" rules exist. The planner (`is_compatible_type_change`) needs strictly larger decimal precision; the backfill (`is_promotable_type_change`) also accepts equal precision. The difference is harmless: equal-precision decimals are already the same type.
- The planner treats `timestamp` and `timestamptz` as equal, so it cannot detect a column that is tz-naive when it should be tz-aware. `apply_ddl` always creates `timestamp` columns as `timestamptz`.
- `set_column_nullability` to required does not validate existing data.
- A rebuild can change only one partition column. If a table is partitioned by several columns, every column you keep needs its own `new_partition`, or the rebuild fails.
- `config-sync` and `trigger-backfill` are manual commands and are not part of `apply`.
- Migrations only run when `schema-evolution-apply` runs. After a backfill finishes, the table's later plans wait for the next deploy (or a manual apply).
- ETL jobs that run right after a deploy may write to a table whose later plans have not applied yet, because a backfill on that table is still running. One plan per table makes this rare.

## Fixing a merged plan that failed

`validate` blocks every edit to a plan that is already on `main`, because it can't tell from Git whether the plan was applied somewhere. If a merged plan **failed** (for example `Cannot backfill partition-source column(s)`), fixing it in place is safe only when it was never applied in **any** environment.

1. In each environment the plan could have reached, check its tracker row:
   ```sql
   SELECT plan_name, status, notes FROM schema_evolution_metadata.plan_tracker
   WHERE table_name = '<table>' AND plan_name = '<plan file name>';
   ```
   Continue only if every row is `failed` or there is no row. If any environment shows `applied` or `backfilling`, don't edit the plan; add a new plan instead.
2. Fix the plan on a branch and open a PR/MR. `validate` fails with `'changes' edited, but the plan exists on main`; that is expected here.
3. Have a second person review the change, then merge it with the required check bypassed: on GitLab, temporarily untick **Settings → Merge requests → Pipelines must succeed**; on GitHub, an administrator merges with **bypass branch protections**. Turn the setting back on straight after.
4. Run apply again. The failed plan is retried with the fixed content, and its checksum is recorded when it reaches `applied`.

