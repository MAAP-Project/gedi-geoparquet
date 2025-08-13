#!/usr/bin/env python

from __future__ import annotations

import argparse
import json
import typing as t
from collections.abc import Iterator, Sequence
from pathlib import Path

import h5py
import numpy as np
import pyarrow as pa

type PyArrowMetadata = dict[str | bytes, str | bytes]


GEO_METADATA = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "point",
            "geometry_types": ["Point"],
            # See https://gedi.umd.edu/instrument/specifications/
            "bbox": [-180.0, -51.6, 180.0, 51.6],
            "covering": {
                "bbox": {
                    "xmax": ["bbox", "xmax"],
                    "xmin": ["bbox", "xmin"],
                    "ymax": ["bbox", "ymax"],
                    "ymin": ["bbox", "ymin"],
                }
            },
        },
    },
}

SCHEMA_METADATA: PyArrowMetadata = {"geo": json.dumps(GEO_METADATA)}


def basename(ds: h5py.Dataset) -> str:
    """Return 'basename' of an HDF5 Dataset's name.

    Since the `name` attribute of an `h5py.Dataset` is the dataset's absolute
    path within its HDF5 file, it is perhaps poorly named.  The basename is the
    last component of the path, which is perhaps what most users would expect
    the `name` attribute to be.

    For example, the basename of a dataset named
    `'/BEAM0000/land_cover_data/leaf_off_doy'` is simply `'leaf_off_doy'`.
    """

    return str(ds.name).rsplit("/")[-1]


def pyarrow_datatype(ds: h5py.Dataset) -> pa.DataType:
    """Return a PyArrow DataType corresponding to an HDF5 Dataset's dtype and shape.

    When `ds` is 1D, return the PyArrow DataType corresponding to `ds.dtype` (a
    numpy dtype), except when `ds.dtype` is the "object" dtype, in which case,
    return `pyarrow.string()`.

    When `ds` is 2D, return a PyArrow list type with elements of the type as
    described above, and a length equal to the number of columns in `ds`, which
    assumes that each value will be a row of data (1D) rather than a scalar.

    Assume `ds` is nothing other than 1D or 2D.
    """

    # We assume dtype "object" indicates a string type, but pyarrow makes no
    # such assumption, so we must be explicit; otherwise from_numpy_dtype
    # raises pyarrow.lib.ArrowNotImplementedError: Unsupported numpy type 17.
    data_type = (
        pa.string()
        if ds.dtype == np.dtype("O")
        # TODO do we want system byte order (=) or little-endian (<)?
        else pa.from_numpy_dtype(ds.dtype.newbyteorder("="))
    )

    # For 2D datasets, each field value will be a list (all values in a dataset
    # row), so the pyarrow data type is "list of data type"; else just scalar.
    # Further, we assume that if we don't have 2D data, we must have 1D data.
    return pa.list_(data_type, ds.shape[1]) if len(ds.shape) == 2 else data_type


def pyarrow_metadata(ds: h5py.Dataset) -> PyArrowMetadata:
    """Convert HDF5 Dataset attributes to PyArrow metadata dict.

    Since an HDF5 attribute value can be of various types, but a PyArrow
    metadata value must be either str or bytes, serialize each attribute value
    to JSON before using it as a metadata value.  Any attribute value that is
    a `numpy.ndarray` is first converted to a list (via its `tolist` method)
    since an `ndarray` is not JSON serializable.
    """
    return {
        name: json.dumps(val.tolist() if isinstance(val, np.ndarray) else val)
        for name, val in ds.attrs.items()
    }


def to_pyarrow_field(ds: h5py.Dataset) -> pa.Field[t.Any]:
    """Create a PyArrow Field describing an HDF5 Dataset."""

    return pa.field(
        name=basename(ds),
        type=pyarrow_datatype(ds),
        metadata=pyarrow_metadata(ds),
    )


def projection(
    group: h5py.Group,
    relative_paths: Sequence[str],
) -> Iterator[h5py.Dataset]:
    """Return an iterator over specific datasets within an `h5py.Group`.

    Iterate over all decendant datasets of `group` as indicated by the specified
    relative paths, which are relative to `group`'s path (`name`), as follows:

    - When a relative path refers to an `h5py.Dataset`, yield it
    - When a relative path refers to an `h5py.Group`, yield all of its
      children that are `h5py.Dataset`s (non-recursively)
    - Otherwise, raise an error

    Parameters
    ----------
    group
        Group to select datasets from.
    relative_paths
        Paths of either `h5py.Dataset`s or `h5py.Group`s relative to the group's
        path.  When a path refers to a subgroup, all of the subgroup's children
        that are `h5py.Dataset`s are selected (non-recursively).

    Raises
    ------
    ValueError
        if there is no value at a particular relative path of `group`
    TypeError
        if the value at a particular relative path of `group` is neither an
        `h5py.Dataset` nor an `h5py.Group`
    """

    for relative_path in relative_paths:
        match group.get(relative_path):
            case h5py.Dataset() as ds:
                yield ds
            case h5py.Group() as subgroup:
                yield from (
                    child
                    for child in subgroup.values()
                    if isinstance(child, h5py.Dataset)
                )
            case None:
                msg = f"{relative_path} is not a descendant of {group.name}"
                raise ValueError(msg)
            case descendant:
                msg = (
                    f"{relative_path} is neither an h5py.Dataset nor an h5py.Group:"
                    f" {type(descendant)}"
                )
                raise TypeError(msg)


def to_pyarrow_schema(
    h5: h5py.File,
    *,
    group_path: str = "/",
    relative_paths: Sequence[str],
    metadata: PyArrowMetadata | None = None,
) -> pa.Schema:
    """Return a PyArrow Schema describing selected datasets from an HDF5 file.

    Within the schema, sort fields by name in ascending order.
    """

    if not isinstance(group := h5.get(group_path), h5py.Group):
        msg = f"No such group within {h5.filename!r}: {group_path!r}"
        raise ValueError(msg)

    datasets = projection(group, relative_paths)
    fields = sorted(map(to_pyarrow_field, datasets), key=lambda field: field.name)

    return pa.schema(fields, metadata=metadata)


class FileLinesReader(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[t.Any] | None,
        option_string: str | None = None,
    ) -> None:
        if not isinstance(values, Path):
            msg = f"expected {self.dest!r} to be a pathlib.Path; got {type(values)}"
            raise TypeError(msg)

        filepath: Path = values
        non_comment_lines = (
            clean_line
            for line in filepath.read_text().splitlines()
            # Eliminate empty lines as well as comment (#) lines
            if (clean_line := line.strip()) and not clean_line.startswith("#")
        )

        setattr(namespace, self.dest, non_comment_lines)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    import textwrap

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
Generate an Apache Arrow Schema from a GEDI HDF5 file to allow constructing \
equivalent GEDI GeoParquet files.

The schema is generated as follows:

- An Apache Arrow Field is created for each HDF5 Dataset listed in the datasets
  input file (or each child dataset within a group specified in the file)
- Field metadata is constructed from the attributes of the corresponding dataset
- For a 1D dataset, the field data type is set to the Arrow Data Type corresponding
  to the dataset's numpy dtype (the numpy "object" dtype is translated to the Arrow
  string data type)
- For a 2D dataset, the field data type is a "list" with the element type obtained
  according to the preceding step
"""
        ),
    )
    parser.add_argument(
        "hdf5_path",
        metavar="HDF5_FILE",
        type=Path,
        help="file system path to an existing GEDI HDF5 file",
    )
    parser.add_argument(
        "-g",
        "--group",
        metavar="GROUP",
        dest="group_path",
        default="/BEAM0000",
        help=(
            "absolute path of group in specified HDF5 file from which"
            " to construct an Apache Arrow Schema (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--datasets",
        required=True,
        metavar="TEXT_FILE",
        dest="relative_paths",
        type=Path,
        action=FileLinesReader,
        help=(
            "absolute path to a text file containing dataset or group paths"
            " (one per line) relative to the specified HDF5 group path (e.g.,"
            " land_cover_data/leaf_on_cycle)"
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        metavar="SCHEMA_FILE",
        dest="output_path",
        type=Path,
        help=("file system directory path to write Apache Arrow Schema to (as bytes)"),
    )

    return parser.parse_args(argv)


def write_schema(schema: pa.Schema, dest: Path) -> None:
    """Write schema bytes to a file, creating parent directories as needed."""

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(schema.serialize().to_pybytes())


def main(
    *,
    hdf5_path: Path,
    group_path: str,
    relative_paths: Sequence[str],
    output_path: Path,
) -> None:
    with h5py.File(hdf5_path) as hdf5:
        schema = to_pyarrow_schema(
            hdf5,
            group_path=group_path,
            relative_paths=relative_paths,
            metadata=SCHEMA_METADATA,
        )

    write_schema(schema, output_path)


if __name__ == "__main__":
    import sys

    ns = parse_args(sys.argv[1:])
    kwargs = dict(ns._get_kwargs())
    main(**kwargs)
