from __future__ import annotations

import typing as t
from collections.abc import Iterator, Sequence

import h5py
import polars as pl
from polars._typing import ArrowSchemaExportable
from polars.io.plugins import register_io_source

import gedi_geoparquet.hdf5 as h5py_
import gedi_geoparquet.pyarrow as pa_


def scan_hdf5(
    group: h5py.Group,
    schema: ArrowSchemaExportable | None = None,
) -> pl.LazyFrame:
    """Lazily read from an h5py Group (or File, which is a Group).

    Allows the query optimizer to push down predicates and projections to the
    scan level, typically increasing performance and reducing memory overhead.

    All datasets must be the same height (length), except for single-row
    datasets. A single-row dataset will result in a column filled with the
    singular value to match the height of the taller (longer) datasets. All
    other datasets must have the same number of rows (length); otherwise, during
    collection, Polars will raise a `polars.exceptions.ComputeError`, caused by
    a `ShapeError` due to the mismatched column heights.

    Parameters
    ----------
    group
        HDF5 Group (or HDF5 File) to scan.
    schema
        Names and data types of datasets to read from ``group``.  Names may be
        absolute or relative (recommended) to ``group``, and data types must
        correspond to the (numpy) data types of the datasets.  If not supplied,
        a schema is derived from all of the datasets in the "flattened" group.

    Returns
    -------
    polars.LazyFrame

    Notes
    -----
    Since this function returns a Polars LazyFrame, it will not raise an error
    if a schema is supplied that does not properly align with the supplied
    group. Specifically, if the name of a field in the schema is not the name of
    an item in the group, or the item in the group is not a Dataset, such an
    error will not result in an error until compute (collect) time. Only then
    will a `ComputeError` be raised, wrapping either a `ValueError` or
    `TypeError`, respectively.

    Examples
    --------
    >>> import h5py

    >>> f = h5py.File.in_memory()
    >>> _ = f.create_dataset("scalar", data=[5.0])
    >>> _ = f.create_dataset("two_d", data=[[0, 1, 2], [3, 4, 5]], dtype="f4")
    >>> g = f.create_group("group")
    >>> _ = g.create_dataset("nested", data=[1, 0], dtype="i1")
    >>> f["linked"] = h5py.SoftLink("/group/nested")

    >>> lf = scan_hdf5(f)
    >>> lf.collect()
    shape: (2, 4)
    ┌──────────────┬────────┬────────┬─────────────────┐
    │ group/nested ┆ linked ┆ scalar ┆ two_d           │
    │ ---          ┆ ---    ┆ ---    ┆ ---             │
    │ i8           ┆ i8     ┆ f64    ┆ array[f32, 3]   │
    ╞══════════════╪════════╪════════╪═════════════════╡
    │ 1            ┆ 1      ┆ 5.0    ┆ [0.0, 1.0, 2.0] │
    │ 0            ┆ 0      ┆ 5.0    ┆ [3.0, 4.0, 5.0] │
    └──────────────┴────────┴────────┴─────────────────┘

    >>> lf.limit(1).collect()
    shape: (1, 4)
    ┌──────────────┬────────┬────────┬─────────────────┐
    │ group/nested ┆ linked ┆ scalar ┆ two_d           │
    │ ---          ┆ ---    ┆ ---    ┆ ---             │
    │ i8           ┆ i8     ┆ f64    ┆ array[f32, 3]   │
    ╞══════════════╪════════╪════════╪═════════════════╡
    │ 1            ┆ 1      ┆ 5.0    ┆ [0.0, 1.0, 2.0] │
    └──────────────┴────────┴────────┴─────────────────┘

    Oddly, Polars ARRAY_* functions don't work on arrays, so we must first
    convert an array to a list in order to use any of the array functions:

    >>> lf.with_columns(pl.col("two_d").arr.to_list()).sql(
    ...     # SQL uses 1-based list indexing, not 0-based indexing.
    ...     "SELECT linked, scalar, two_d FROM self WHERE two_d[1] == 0.0"
    ... ).collect()
    shape: (1, 3)
    ┌────────┬────────┬─────────────────┐
    │ linked ┆ scalar ┆ two_d           │
    │ ---    ┆ ---    ┆ ---             │
    │ i8     ┆ f64    ┆ list[f32]       │
    ╞════════╪════════╪═════════════════╡
    │ 1      ┆ 5.0    ┆ [0.0, 1.0, 2.0] │
    └────────┴────────┴─────────────────┘
    """

    schema = pl.Schema(schema or pa_.infer_schema(group))

    def source(
        columns: Sequence[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        # Lazily fetch data in batches, each batch in its own polars.DataFrame.
        dfs = unfiltered_source(columns or tuple(schema.keys()), batch_size or 100_000)

        # Lazily filter data in each batch, if a predicate was provided.
        filtered_dfs = (df if predicate is None else df.filter(predicate) for df in dfs)

        # Yield each batch.  If n_rows was provided, limit the size of the last
        # batch to guarantee that no more than n_rows in total are yielded
        # (across all yielded batches).
        yield from (filtered_dfs if n_rows is None else limit(filtered_dfs, n_rows))

    def unfiltered_source(
        columns: Sequence[str],
        batch_size: int,
    ) -> Iterator[pl.DataFrame]:
        dss = tuple(t.cast(h5py.Dataset, group[column]) for column in columns)
        batched_dss = (h5py_.batched(ds, batch_size) for ds in dss)

        data_batches = (dict(zip(columns, batch)) for batch in zip(*batched_dss))
        data_schema = {name: schema[name] for name in columns}

        return (pl.from_dict(data, data_schema) for data in data_batches)

    def limit(dfs: Iterator[pl.DataFrame], n_rows: int) -> Iterator[pl.DataFrame]:
        """Yield DataFrames, limiting the total number of yielded data rows."""
        while n_rows > 0 and (df := next(dfs, None)) is not None:
            yield df if len(df) <= n_rows else df.head(n_rows)
            n_rows -= len(df)

    return register_io_source(source, schema=schema, validate_schema=True, is_pure=True)
