from __future__ import annotations

import typing as t
from collections.abc import Iterator, Sequence

import h5py
import polars as pl
import polars.datatypes
from polars.io.plugins import register_io_source
from polars._typing import ArrowSchemaExportable

import gedi_geoparquet.hdf5 as hdf5_
import gedi_geoparquet.pyarrow as pa_

DEFAULT_BATCH_SIZE = 100_000
DEFAULT_N_ROWS = 2**64 - 1


def scan_hdf5(
    group: h5py.Group,
    *,
    schema: ArrowSchemaExportable | None = None,
) -> pl.LazyFrame:
    """Lazily read from an h5py Group (or File, which is a Group).

    Allows the query optimizer to push down predicates and projections to the
    scan level, typically increasing performance and reducing memory overhead.

    All datasets must be the same height, except for "scalar" datasets (i.e.,
    with a shape of `(1,)`).  A scalar dataset will result in a column filled
    with the singular value to match the height of the non-scalar datasets.  All
    other datasets must have the same number of elements.  Note, however, that
    this function will not raise an error if this is not the case.  Instead,
    when `collect()` is invoked on the result, it will raise a
    `polars.exceptions.ComputeError`, caused by a `ShapeError` due to the
    mismatched column heights.

    Parameters
    ----------
    group
        Group (or h5py.File) to scan.
    schema
        Names and data types of datasets to read from ``group``.  Names may be
        absolute or relative (recommended) to ``group``, and data types must
        correspond to the (numpy) data types of the datasets.  If not supplied,
        a schema is derived from all of the datasets in the "flattened" group.

    Raises
    ------
    KeyError
        If a dataset named in the schema cannot be found in ``group``.

    Examples
    --------
    >>> import h5py
    >>> with h5py.File.in_memory() as f:
    ...     _ = f.create_dataset("scalar", data=[5.0])
    ...     _ = f.create_dataset("two_d", data=[[0, 1, 2], [3, 4, 5]], dtype="f4")
    ...     g = f.create_group("group")
    ...     _ = g.create_dataset("nested", data=[1, 0], dtype="i1")
    ...     f["linked"] = h5py.SoftLink("/group/nested")
    ...     scan_hdf5(f).collect()
    shape: (2, 4)
    ┌──────────────┬────────┬────────┬─────────────────┐
    │ group/nested ┆ linked ┆ scalar ┆ two_d           │
    │ ---          ┆ ---    ┆ ---    ┆ ---             │
    │ i8           ┆ i8     ┆ f64    ┆ array[f32, 3]   │
    ╞══════════════╪════════╪════════╪═════════════════╡
    │ 1            ┆ 1      ┆ 5.0    ┆ [0.0, 1.0, 2.0] │
    │ 0            ┆ 0      ┆ 5.0    ┆ [3.0, 4.0, 5.0] │
    └──────────────┴────────┴────────┴─────────────────┘
    """
    schema = schema or pa_.infer_schema(group)
    pl_schema = pl.Schema(schema)

    def source(
        columns: Sequence[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        columns = columns or tuple(pl_schema.keys())
        partial_schema = pl.Schema({name: pl_schema[name] for name in columns})
        datasets = tuple(t.cast(h5py.Dataset, group[column]) for column in columns)
        scalars = {ds for ds in datasets if ds.shape == (1,)}

        n_rows = n_rows or DEFAULT_N_ROWS
        batch_size = batch_size or DEFAULT_BATCH_SIZE
        start = 0

        while n_rows:
            stop = start + min(batch_size, n_rows)
            data = {
                # For datasets containing only a single scalar value, we want to
                # set the column value to the scalar value so that Polars will
                # automatically fill the column with the value, thus matching
                # the heights of the other columns.  Otherwise, we'll get a
                # polars.exceptions.ComputeError: ShapeError.
                column: dataset[0] if dataset in scalars else dataset[start:stop]
                for column, dataset in zip(partial_schema.names(), datasets)
            }

            if (df := pl.from_dict(data, partial_schema)).is_empty():
                break  # We read all available data

            filtered_df = df if predicate is None else df.filter(predicate)
            n_rows -= len(filtered_df)
            start = stop

            yield filtered_df

    return register_io_source(
        source, schema=pl_schema, validate_schema=True, is_pure=True
    )


def _infer_schema(group: h5py.Group) -> pl.Schema:
    """Infer a Polars Schema for an h5py Group.

    Flatten the group, such that all h5py Datasets at all levels within the
    group are treated as though they are direct children of the group, with
    names relative to the group name.

    Arguments
    ---------
    group
        Group for which to infer a Polars Schema.

    Returns
    -------
    schema
        Polars Schema with one entry per dataset within ``group`` (recursive),
        where each entry name is the name of the corresponding dataset relative
        to the name of ``group``.  The data type of an entry is converted from
        the numpy datatype of the corresponding dataset.  For object types,
        strings are assumed. For multi-dimensional datasets, the Polars data
        type is nested in `pl.List` for each dimension beyond the first (see
        example).

    Examples
    --------
    >>> import h5py

    Notice that the `h5py` string dtype resolves to the numpy object dtype, not
    the numpy string dtype:

    >>> str_dtype = h5py.string_dtype()
    >>> str_dtype
    dtype('O')

    When inferring a schema, object types are translated to string types:

    >>> with h5py.File.in_memory() as f:
    ...     group = f.create_group("group")
    ...     ds_1d = group.create_dataset("ds_1d", shape=(10_000,), dtype="f8")
    ...     ds_2d = group.create_dataset("ds_2d", shape=(10_000, 10), dtype="f8")
    ...     ds_str = group.create_dataset("ds_str", shape=(10_000,), dtype=str_dtype)
    ...     subgroup = group.create_group("subgroup")
    ...     ds_0d = subgroup.create_dataset("ds_0d", dtype="i8")
    ...     ds_3d = subgroup.create_dataset("ds_3d", shape=(10_000, 10, 10), dtype="u1")
    ...     _infer_schema(group)
    Schema({'ds_1d': Float64,
            'ds_2d': List(Float64),
            'ds_str': String,
            'subgroup/ds_0d': Int64,
            'subgroup/ds_3d': List(List(UInt8))})
    """
    return pl.Schema(
        {name: _schema_dtype(ds) for name, ds in hdf5_.flatten(group).items()}
    )


def _schema_dtype(ds: h5py.Dataset) -> pl.DataType | polars.datatypes.DataTypeClass:
    """Determine the Polars DataType for an HDF5 Dataset.

    Parameters
    ----------
    ds
        Dataset to determine Polars DataType for, for use in a Polars Schema,
        taking dimensionality into account.

    Returns
    -------
    datatype
        Polars DataType corresponding to the numpy datatype of ``ds``.  For the
        numpy object dtype, this is `polars.String`.  For multi-dimensional
        datasets, the Polars data type is nested within `pl.List` for each
        dimension beyond the first (see examples).

    Examples
    --------
    >>> import h5py
    >>> import numpy as np

    >>> obj_type = h5py.string_dtype()
    >>> obj_type
    dtype('O')

    >>> with h5py.File.in_memory() as f:
    ...     ds_1d = f.create_dataset("ds_1d", shape=(10_000,), dtype="f8")
    ...     ds_2d = f.create_dataset("ds_2d", shape=(10_000, 10), dtype="f8")
    ...     ds_3d = f.create_dataset("ds_3d", shape=(10_000, 10, 10), dtype="f8")
    ...     ds_obj = f.create_dataset("ds_str", shape=(10_000,), dtype=obj_type)
    ...     _schema_dtype(ds_1d)
    ...     _schema_dtype(ds_2d)
    ...     _schema_dtype(ds_3d)
    ...     _schema_dtype(ds_obj)
    Float64
    List(Float64)
    List(List(Float64))
    String
    """
    from functools import reduce
    from polars.datatypes.convert import numpy_char_code_to_dtype

    # numpy_char_code_to_dtype does not handle numpy Object type ("O"), so we
    # have to handle it ourselves, and we simply assume it represents a string.
    base_dtype = (
        pl.String
        if (numpy_char_code := ds.dtype.char) == "O"
        else numpy_char_code_to_dtype(numpy_char_code)
    )

    return reduce(lambda dtype, _: pl.List(dtype), range(1, ds.ndim), base_dtype)
