#!/usr/bin/env python

import typing as t
from pathlib import Path

import fsspec
import gedi_geoparquet as gedi
import h5py
import pyarrow.parquet as pq
from cyclopts import App

type Compression = t.Literal["brotli", "gzip", "lz4", "snappy", "zstd"]

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
def convert(
    input: str,
    output_dir: Path,
    /,
    *,
    writer: t.Literal["pyarrow", "polars"] = "pyarrow",
    compression: Compression = "zstd",
    compression_level: int | None = None,
) -> None:
    """Convert a GEDI HDF5 file to a (geo)parquet file.

    Parameters
    ----------
    input
        Path or HTTPS URL to an HDF5 GEDI file (L2A, L2B, L4A, or L4C) to convert.
    output_dir
        Directory to write resulting .parquet file to.
    writer
        Which library to use for writing the parquet output file.
    compression
        Which compression algorithm to use when writing to the parquet output file.
    compression_level
        Compression level for a codec.  If None is passed, the writer selects the
        compression level for the compression codec in use.  The compression level
        has a different meaning for each codec, so you have to read the documentation
        of the codec you are using.  An exception is thrown if the compression codec
        does not allow specifying a compression level.
    """
    import os
    from urllib.parse import urlparse

    # Oddly, aiohttp requires NETRC to be set for trust_env=True to cause the
    # netrc file to be read.
    if "NETRC" not in os.environ:
        os.environ["NETRC"] = os.path.expanduser("~/.netrc")

    url: str
    fs: fsspec.AbstractFileSystem
    fs, url = fsspec.url_to_fs(
        input,
        cache_type="mmap",
        block_size=8 * 1024 * 1024,
        client_kwargs=dict(trust_env=True),
    )
    basename = urlparse(url).path.rsplit("/", 1)[-1]
    output = (output_dir / basename).with_suffix(
        f".{compression}-{compression_level}-{writer}.parquet"
    )

    with fs.open(url) as fp, h5py.File(fp) as hdf5:
        convert = convert_via_polars if writer == "polars" else convert_via_pyarrow
        convert(
            hdf5,
            output=output,
            compression=compression,
            compression_level=compression_level,
        )


def convert_via_polars(
    file: h5py.File,
    *,
    output: Path,
    compression: Compression,
    compression_level: int | None,
) -> None:
    schema = gedi.abridged_polars_schema(str(file.attrs["short_name"]))
    lf = gedi.to_polars(file, schema=schema)

    lf.sink_parquet(
        output,
        compression=compression,
        compression_level=compression_level,
        metadata=gedi.GEOPARQUET_METADATA,
    )


def convert_via_pyarrow(
    file: h5py.File,
    *,
    output: Path,
    compression: Compression,
    compression_level: int | None,
) -> None:
    schema = gedi.abridged_arrow_schema(str(file.attrs["short_name"]))
    table = gedi.to_arrow(file, schema=schema)

    pq.write_table(
        table,
        output,
        compression=compression,
        compression_level=compression_level,
    )


if __name__ == "__main__":
    app()
