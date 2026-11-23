# Plan and schema file reference

## Plan files

Plans live at `migrations/<layer>/<table>/<timestamp>_<description>.json`. Files for one table are applied in filename order, so start each name with a sortable UTC timestamp (`20260101T090000`). Never edit a plan after it has been applied; add a new one instead. The checksum guard enforces this.

```json
{
  "metadata": {"layer": "silver", "table": "demo_customers", "environments": ["dev", "qa"]},
  "changes": [ { "change_type": "...", "column_name": "...", "...": "..." } ]
}
```

### metadata

| Key | Required | Meaning |
|---|---|---|
| `table` | yes | Target table, which must match the directory name |
| `layer` | no | `bronze`/`silver`/`gold`. If set, must match the directory |
| `environments` | no | String or list. Omit it to apply everywhere. An empty list is rejected |
| `environment` | no | Alias for `environments`. Setting both is an error |
| `database` | deprecated | `{prefix}_{env}_{layer}`. Pins the plan to that one environment |

The namespace is never stored in the plan. It is always derived as `{prefix}_{environment}_{layer}`.

### Change types

| `change_type` | Required keys | Optional keys | Applied by |
|---|---|---|---|
| `add_column` | `column_name`, `target_type` | | PyIceberg |
| `drop_column` | `column_name` | | PyIceberg |
| `rename_column` | `column_name` (new), `previous_name` | | PyIceberg |
| `compatible_type_change` | `column_name`, `target_type` | `current_type` | PyIceberg |
| `incompatible_type_change` | `column_name`, `current_type`, `target_type` | `transformation`, `new_partition` | Glue backfill |
| `set_column_nullability` | `column_name`, `target_not_null` | | PyIceberg |
| `add_partition_field` | `column_name`, `transform` | `transform_param` | PyIceberg |
| `drop_partition_field` | `column_name`, `transform` | `transform_param` | PyIceberg |
| `repartition` | `column_name`, `new_partition` | | Glue rebuild |
| `rebuild_table` | `column_name: "__table__"` | `target_format_version` (`"2"`/`"3"`), `sort_fields` | Glue rebuild |
| `set_sort_order` | `column_name: "__sort_order__"`, `sort_fields` | `rebuild` (default true) | Glue rebuild, or PyIceberg if `rebuild: false` |
| `set_table_properties` | `column_name: "__table_properties__"`, `properties` | | PyIceberg |
| `unset_table_properties` | `column_name: "__table_properties__"`, `property_keys` | | PyIceberg |
| `rename_table` | `column_name: "__table__"`, `previous_name` | | S3 Tables API; must be the only change in its plan |

**Types:**

- `string`
- `int` / `integer`
- `long` / `bigint`
- `float`, `double`
- `boolean`
- `date`
- `decimal(p,s)`
- `timestamp` (stored as `timestamptz`)
- `timestamp_ntz` (time-zone-less)

**Compatible changes** (metadata-only): int to long, float to double, and decimal with the same scale and larger precision. Anything else is incompatible.

**`transformation`** is a Spark SQL expression that uses the column name, for example `to_timestamp(signup_date, 'yyyy-MM-dd')`. If omitted, the job uses `CAST(col AS target_type)`.

**`new_partition`** is `{"transform": "month"}`, `{"transform": "bucket", "param": 16}`, or the same with `"rebuild": false`. With `rebuild: false` the partition spec changes without rewriting existing files; only new writes use it. This option is ignored when a type change on that column forces a rebuild.

**Partition transforms:** `identity`, `year`, `month`, `day`, `hour`, `bucket` (needs `transform_param`), `truncate` (needs `transform_param`).

Only one change per column is allowed in a plan.

See `examples/migrations/` for working examples of every common case.

## Schema files (desired state)

`schema-evolution-plan`, `schema-evolution-config-sync` and `schema-evolution-generate-env-plans` read the same YAML.

```yaml
target_table: demo_customers
fields:
  - table_column: customer_id
    table_data_type: long
    not_null: true                       # Iceberg required
    merge_key: true                      # used by the env-column backfill
  - table_column: email_domain
    table_data_type: string
    previous_table_column: email_host    # string or list; produces rename_column
  - table_column: beta_segment
    table_data_type: string
    environments: [dev, qa]              # field only exists in these envs
partition_strategy:
  partition_by:
    - {column: signup_date, transform: month}      # or "bucket[16]"
  sort_order:
    - {column: customer_id, direction: asc, rebuild: false}
table_properties:
  write.target-file-size-bytes: "268435456"
```

Data-product style keys `technical_name` / `data_type` are accepted as aliases for `table_column` / `table_data_type`.

How the planner classifies differences:

- A field that is not in the table becomes `add_column`, or `rename_column` if a `previous_table_column` exists in the table.
- A type mismatch becomes `compatible_type_change` or `incompatible_type_change`.
- A `not_null` mismatch becomes `set_column_nullability`.
- A table column that is not in the file becomes `drop_column`. Silver metadata columns (`_insert_timestamp`, `_update_timestamp`, `_source`, `_valid_from`, `_valid_to`, `_is_current`) are never dropped.

Always review generated plans, especially drops.

Where the tools look for the file (`config_sync` / `trigger_backfill`):

1. `config/<layer>/<source>/schemas/<table>.yaml`
2. `config/<layer>/<source>/**/schemas/<table>.yaml`
3. `config/<layer>/<source>/**/data_products/<table>.yaml`
4. For silver only, `config/bronze/<source>/schemas/<table>.yaml`
