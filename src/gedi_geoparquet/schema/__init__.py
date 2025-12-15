import json
import typing as t
from functools import cache

import pyarrow as pa


__all__ = [
    "GEOPARQUET_METADATA",
    "abridged_schema",
    "full_schema",
]

GEOPARQUET_METADATA: t.Final[dict[str | bytes, str | bytes]] = {
    "geo": json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "point",
                    "geometry_types": ["Point"],
                },
            },
        }
    )
}


def _normalized_name(short_name: str) -> str:
    return {
        "GEDI_L2A": "gedi_l2a",
        "GEDI_L2B": "gedi_l2b",
        "GEDI_L4A": "gedi_l4a",
        # The "short_name" attribute values of L4A files is inconsistent.  Some
        # are "GEDI_WSCI" and some are "GEDI04_C", so it's also inconsistent
        # with the ones above.  If it were, it would be "GEDI_L4C".
        "GEDI04_C": "gedi_l4c",
        "GEDI_WSCI": "gedi_l4c",
    }[short_name]


@cache
def full_schema(short_name: str) -> pa.Schema:
    from importlib.resources import open_binary

    schema_filename = f"{_normalized_name(short_name)}.arrows"

    with open_binary("gedi_geoparquet.schema.resources", schema_filename) as io:
        return pa.ipc.read_schema(io).with_metadata(GEOPARQUET_METADATA)  # type: ignore


@cache
def abridged_schema(short_name: str) -> pa.Schema:
    from importlib import import_module

    name = _normalized_name(short_name)
    module = import_module(f"gedi_geoparquet.schema.{name}")
    dataset_names: set[str] = module.ABRIDGED_DATASET_NAMES
    fields = (field for field in full_schema(short_name) if field.name in dataset_names)

    return pa.schema(fields, GEOPARQUET_METADATA)
