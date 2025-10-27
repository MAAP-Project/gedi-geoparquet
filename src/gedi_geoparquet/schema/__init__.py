import json
import typing as t
from functools import cache

import polars as pl
import pyarrow as pa

from gedi_geoparquet.schema import gedi_l2a, gedi_l2b, gedi_l4a, gedi_l4c

__all__ = [
    "GEOPARQUET_METADATA",
    "abridged_arrow_schema",
    "abridged_polars_schema",
]

GEOPARQUET_METADATA: t.Final = {
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


@cache
def abridged_arrow_schema(short_name: str) -> pa.Schema:
    return pa.schema(abridged_polars_schema(short_name), GEOPARQUET_METADATA)  # type: ignore


@cache
def abridged_polars_schema(short_name: str) -> pl.Schema:
    schema = {
        "GEDI_L2A": gedi_l2a.SCHEMA,
        "GEDI_L2B": gedi_l2b.SCHEMA,
        "GEDI_L4A": gedi_l4a.SCHEMA,
        # The "short_name" attribute values of L4A files is inconsistent.  Some
        # are "GEDI_WSCI" and some are "GEDI04_C".
        "GEDI_WSCI": gedi_l4c.SCHEMA,
        "GEDI04_C": gedi_l4c.SCHEMA,
    }.get(short_name)

    if not schema:
        msg = f"I don't know the schema for {short_name!r}"
        raise ValueError(msg)

    return schema
