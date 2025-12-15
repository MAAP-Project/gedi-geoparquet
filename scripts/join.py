#!/usr/bin/env python

import typing as t
from functools import reduce
from pathlib import Path

import polars as pl
from cyclopts import App, Parameter

from gedi_geoparquet import GEOPARQUET_METADATA

type Compression = t.Literal["brotli", "gzip", "lz4", "snappy", "zstd"]

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")

DEGRADE_FLAGS = {0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68}


@app.default
def join(
    inputs: t.Annotated[list[Path], Parameter(consume_multiple=True)],
    output: Path,
    /,
    *,
    compression: Compression = "zstd",
    compression_level: int = 3,
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

    def join_(left: pl.LazyFrame, right: pl.LazyFrame) -> pl.LazyFrame:
        return left.join(right, on="shot_number", how="inner").select(
            pl.exclude("^.*_right$")  # drop duplicate columns
        )

    lf = reduce(join_, map(pl.scan_parquet, inputs))

    # Using LazyFrame.sink_parquet, rather than DataFrame.write_parquet, is more
    # performant and reduces memory pressure, but produces a significantly
    # larger file, so we're avoiding it unless memory usage becomes an issue.
    # I suspect this is because there are some write optimizations that can be
    # leveraged when all of the data is in memory that cannot otherwise be
    # leveraged/determined when data is being streamed.

    lf.filter(
        pl.col("degrade_flag").is_in(DEGRADE_FLAGS),
        pl.col("sensitivity") >= 0.95,
        pl.col("sensitivity_a2") >= 0.95,
        quality_flag=1,
        surface_flag=1,
    ).drop("quality_flag", "surface_flag").collect().write_parquet(
        output,
        compression=compression,
        compression_level=compression_level,
        metadata=GEOPARQUET_METADATA,  # type: ignore[arg-type]
    )


if __name__ == "__main__":
    app()
