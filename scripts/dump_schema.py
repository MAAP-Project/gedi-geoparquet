#!/usr/bin/env python

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from cyclopts import App

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
def dump_schema(
    file: Path,
    /,
    *,
    truncate_metadata: bool = True,
    show_field_metadata: bool = True,
    show_schema_metadata: bool = True,
) -> None:
    """Dump Arrow schema from an arrows or parquet file.

    Parameters
    ----------
    file
        Path to an Arrow stream (typically `.arrows` extension) or a `.parquet`
        file.  If the file extension is _not_ `.parquet`, it is assumed to be
        an Arrow stream file containing only a schema.
    truncate_metadata
        Limit metadata key/value display to a single line of ~80 characters or less.
    show_field_metadata
        Display field-level key-value metadata.
    show_schema_metadata
        Display schema-level key-value metadata.
    """

    schema = (
        pq.read_schema(file)
        if file.suffix == ".parquet"
        # This call works and type checks:
        #
        #   pa.ipc.read_schema(pa.py_buffer(file.read_bytes()))
        #
        # It is equivalent to the following line, which does NOT type check.
        # Although `str` and `Path` are both supported argument types, they are
        # not covered by the type annotation on `pa.ipc.read_schema`.
        # See https://github.com/zen-xu/pyarrow-stubs/issues/279
        else pa.ipc.read_schema(file)  # type: ignore
    )

    print(
        schema.to_string(
            truncate_metadata=truncate_metadata,
            show_field_metadata=show_field_metadata,
            show_schema_metadata=False,
        )
    )

    if show_schema_metadata and schema.metadata is not None:
        # Even when `truncate_metadata` is `False`, some truncation may still
        # occur, so we'll directly dump the metadata so we can view all of it.
        print("-- schema metadata --")
        print(schema.metadata)

    if file.suffix == ".parquet" and (metadata := pq.read_metadata(file).metadata):
        print("Geo metadata:", metadata[b"geo"])


if __name__ == "__main__":
    app()
