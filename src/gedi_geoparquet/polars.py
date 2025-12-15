from __future__ import annotations

import typing as t
from collections.abc import Iterator, Sequence

import h5py
import polars as pl
from polars._typing import ArrowSchemaExportable
from polars.io.plugins import register_io_source

import gedi_geoparquet.pyarrow as pa_

DEFAULT_BATCH_SIZE = 100_000
DEFAULT_N_ROWS = 2**64 - 1


def scan_hdf5(
    group: h5py.Group,
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

    return register_io_source(source, schema=schema, validate_schema=True, is_pure=True)
