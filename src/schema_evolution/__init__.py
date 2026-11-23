"""Schema Evolution — PyIceberg migration tool for S3 Tables."""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class ChangeType(Enum):
    """Enumeration of supported schema change types."""

    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    RENAME_TABLE = "rename_table"
    COMPATIBLE_TYPE_CHANGE = "compatible_type_change"
    INCOMPATIBLE_TYPE_CHANGE = "incompatible_type_change"
    SET_COLUMN_NULLABILITY = "set_column_nullability"
    ADD_PARTITION_FIELD = "add_partition_field"
    DROP_PARTITION_FIELD = "drop_partition_field"
    REPARTITION = "repartition"
    REBUILD_TABLE = "rebuild_table"
    SET_SORT_ORDER = "set_sort_order"
    SET_TABLE_PROPERTIES = "set_table_properties"
    UNSET_TABLE_PROPERTIES = "unset_table_properties"


@dataclass
class SchemaChange:
    """A single schema change operation for a column."""

    change_type: ChangeType
    column_name: str
    previous_name: str | None = None
    current_type: str | None = None
    target_type: str | None = None
    transformation: str | None = None
    # Nullability change: target required state for set_column_nullability.
    # True  -> column becomes NOT NULL (required)
    # False -> column becomes nullable (optional)
    target_not_null: bool | None = None
    # Partition fields
    transform: str | None = None  # e.g. "month", "bucket", "identity"
    transform_param: int | None = None  # e.g. 16 for bucket(16)
    # Re-partition instruction for an incompatible type change on a partition-source
    # column: {"transform": "month", "param": None}. Tells the backfill how to
    # partition the new column after the type swap. Add "rebuild": false to request
    # a metadata-only Iceberg partition evolution (drop old field, add new field)
    # instead of a CTAS rebuild -- existing data files keep their old layout and
    # only new writes use the new spec (ignored when a non-promotable type change
    # on an active partition-source column forces a rebuild regardless).
    new_partition: dict | None = None
    # Sort order. ``rebuild_table`` may supply these to set a new physical
    # write order; when absent the standalone rebuild preserves the current one.
    sort_fields: list[dict] | None = None  # [{"column": "x", "direction": "asc"}]
    rebuild: bool | None = None  # whether the change requires a full table rebuild
    # Table properties
    properties: dict[str, str] | None = None  # for set_table_properties
    property_keys: list[str] | None = None  # for unset_table_properties


# Supported Iceberg partition transforms
VALID_TRANSFORMS: set[str] = {"identity", "year", "month", "day", "hour", "bucket", "truncate"}
PARAMETERIZED_TRANSFORMS: set[str] = {"bucket", "truncate"}


LAYER_METADATA_COLUMNS: dict[str, set[str]] = {
    "silver": {
        "_insert_timestamp",
        "_update_timestamp",
        "_source",
        "_valid_from",
        "_valid_to",
        "_is_current",
    },
    "gold": set(),
    "bronze": set(),
}

# ---------------------------------------------------------------------------
# Shared Helpers
# ---------------------------------------------------------------------------


def load_catalog(region: str, account_id: str, target_bucket: str):
    """Initialize PyIceberg REST catalog for S3 Tables."""
    from pyiceberg.catalog import load_catalog as _load

    return _load(
        "s3tables",
        **{
            "type": "rest",
            "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
            "warehouse": f"arn:aws:s3tables:{region}:{account_id}:bucket/{target_bucket}",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": region,
        },
    )


def target_bucket_for_layer(layer: str, bucket_prefix: str, environment: str) -> str:
    """Derive S3 Tables bucket name. Convention: {prefix}-{env}-{layer}-tables."""
    return f"{bucket_prefix}-{environment}-{layer}-tables"


def normalize_type(s: str) -> str:
    """Normalize Iceberg type string for comparison."""
    s = s.strip().lower()
    if "(" in s:
        s = re.sub(r"\s*\(\s*", "(", s)
        s = re.sub(r"\s*\)\s*", ")", s)
        s = re.sub(r"\s*,\s*", ",", s)
    return {"bigint": "long", "int": "integer", "timestamptz": "timestamp"}.get(s, s)


def types_match(a: str, b: str) -> bool:
    """Return True if two type strings are semantically equal."""
    return normalize_type(a) == normalize_type(b)


def is_compatible_type_change(current_type: str, target_type: str) -> bool:
    """Check if a type change can be done via Iceberg metadata (no backfill)."""
    cur = normalize_type(current_type)
    tgt = normalize_type(target_type)
    if cur == "integer" and tgt == "long":
        return True
    if cur == "float" and tgt == "double":
        return True
    cur_m = re.match(r"decimal\((\d+),(\d+)\)", cur)
    tgt_m = re.match(r"decimal\((\d+),(\d+)\)", tgt)
    if cur_m and tgt_m:
        cp, cs = int(cur_m.group(1)), int(cur_m.group(2))
        tp, ts = int(tgt_m.group(1)), int(tgt_m.group(2))
        if ts == cs and tp > cp:
            return True
    return False


def parse_previous_names(value: Any) -> list:
    """Parse previous_table_column field from config YAML."""
    if value is None:
        return []
    if isinstance(value, str):
        if value.strip().lower() == "none" or not value.strip():
            return []
        return [value]
    if isinstance(value, list):
        return [item for item in value if item and isinstance(item, str) and item.strip().lower() != "none"]
    return []
