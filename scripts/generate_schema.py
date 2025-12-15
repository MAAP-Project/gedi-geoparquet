#!/usr/bin/env python

from __future__ import annotations

import os
import typing as t
from pathlib import Path

import fsspec
import h5py
import pyarrow as pa
from cyclopts import App

import gedi_geoparquet.pyarrow as pa_

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")

FSSPEC_KWARGS = dict(
    cache_type="mmap",
    block_size=8 * 1024 * 1024,
    client_kwargs=dict(trust_env=True),
)

# Oddly, aiohttp requires NETRC to be set for trust_env=True to cause the netrc
# file to be read.
if "NETRC" not in os.environ:
    import platform

    netrc_name = "_netrc" if platform.system() == "Windows" else ".netrc"
    os.environ["NETRC"] = os.path.expanduser(f"~/{netrc_name}")


@app.default
def generate_schema(
    input: str,
    output: Path,
    /,
    *,
    group: str = "/BEAM0000",
) -> None:
    """Generate an Arrow schema for a group in an HDF5 file.

    The schema is generated as follows:

    - An Apache Arrow Field is created for each HDF5 Dataset within the
      specified group (recursively) of the specified HDF5 file.
    - Field metadata is constructed from the attributes of the corresponding
      dataset.
    - For a 1D dataset, the field data type is set to the Arrow Data Type
      corresponding to the dataset's numpy dtype (the numpy "object" dtype is
      translated to the Arrow string data type).
    - For a 2D dataset, the field data type is a "list" with the element type
      obtained according to the preceding step.

    Parameters
    ----------
    input
        URL or filesystem path to an HDF5 file.
    output
        Filesystem path to write generated schema to (as bytes).  Missing parent
        directories will be created, if necessary and if permissions allow.
    group
        Path (fully-qualified name) of the group within the file to generate the
        schema for.
    """
    url: str
    fs: fsspec.AbstractFileSystem
    fs, url = fsspec.url_to_fs(input, **FSSPEC_KWARGS)

    with fs.open(url) as fp, h5py.File(fp) as hdf5:
        if not isinstance(hdf5_group := hdf5[group], h5py.Group):
            msg = f"Group not found in {input}: {group!r}"
            raise ValueError(msg)

        schema = t.cast(pa.Schema, pa_.infer_schema(hdf5_group))

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(schema.serialize().to_pybytes())


if __name__ == "__main__":
    app()
