#!/usr/bin/env python

import os
import typing as t
from pathlib import Path
from urllib.parse import urlparse

import fsspec
import gedi_geoparquet as gedi
import h5py
from cyclopts import App


# Oddly, aiohttp requires NETRC to be set for trust_env=True to cause the netrc
# file to be read.
if "NETRC" not in os.environ:
    import platform

    netrc_name = "_netrc" if platform.system() == "Windows" else ".netrc"
    os.environ["NETRC"] = os.path.expanduser(f"~/{netrc_name}")

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
    compression
        Which compression algorithm to use when writing to the parquet output file.
    compression_level
        Compression level for a codec.  If None is passed, the writer selects the
        compression level for the compression codec in use.  The compression level
        has a different meaning for each codec, so you have to read the documentation
        of the codec you are using.  An exception is thrown if the compression codec
        does not allow specifying a compression level.
    """
    url: str
    fs: fsspec.AbstractFileSystem
    fs, url = fsspec.url_to_fs(
        input,
        cache_type="mmap",
        block_size=8 * 1024 * 1024,
        client_kwargs=dict(trust_env=True),
    )
    basename = urlparse(url).path.rsplit("/", 1)[-1]
    output = (output_dir / basename).with_suffix(".parquet")

    with fs.open(url) as fp, h5py.File(fp) as hdf5:
        collection_name = str(hdf5.attrs["short_name"])
        collection_schema = gedi.abridged_schema(collection_name)

        gedi.to_polars(hdf5, schema=collection_schema).collect().write_parquet(
            output,
            compression=compression,
            compression_level=compression_level,
            metadata=gedi.GEOPARQUET_METADATA,  # type: ignore[arg-type]
        )


if __name__ == "__main__":
    app()
