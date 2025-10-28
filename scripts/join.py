#!/usr/bin/env python

import typing as t
from functools import reduce
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from cyclopts import App, Parameter

from gedi_geoparquet import GEOPARQUET_METADATA

type Compression = t.Literal["brotli", "gzip", "lz4", "snappy", "zstd"]

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
def join(
    inputs: t.Annotated[list[Path], Parameter(consume_multiple=True)],
    output: Path,
    /,
    *,
    compression: Compression = "zstd",
    compression_level: int | None = None,
) -> None:
    """Join multiple GEDI parquet files on shot_number.

    Parameters
    ----------
    inputs
        Paths to HDF5 GEDI files (L2A, L2B, L4A, or L4C) to join.
    output
        Path to write resulting .parquet file to.
    compression
        Which compression algorithm to use when writing to the parquet output file.
    compression_level
        Compression level for a codec.  If None is passed, the writer selects the
        compression level for the compression codec in use.  The compression level
        has a different meaning for each codec, so you have to read the documentation
        of the codec you are using.  An exception is thrown if the compression codec
        does not allow specifying a compression level.
    """

    def full_join(left: pl.LazyFrame, right: pl.LazyFrame) -> pl.LazyFrame:
        return left.join(right, on="shot_number", how="full").select(
            pl.exclude("^.*_right$")
        )

    lf = reduce(full_join, map(pl.scan_parquet, inputs))

    # Polars does not seem to write metadata in a manner that Arrow recognizes,
    # so we are forced to collect our Polars LazyFrame into an Arrow Table in
    # order to add the metadata correctly.

    ## This doesn't seem to write the metadata where Arrow recognizes it, or at
    ## least not where it will be attached to the Arrow Schema upon reading of
    ## the schema.
    # lf.sink_parquet(output, metadata=GEOPARQUET_METADATA)

    table = lf.collect().to_arrow().replace_schema_metadata(GEOPARQUET_METADATA)

    pq.write_table(
        table,
        output,
        compression=compression,
        compression_level=compression_level,
    )


if __name__ == "__main__":
    app()
