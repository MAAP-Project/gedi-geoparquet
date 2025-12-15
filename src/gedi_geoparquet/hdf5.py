from __future__ import annotations

from collections.abc import Iterator, Mapping
import typing as t
from enum import StrEnum, auto

import h5py
import numpy as np
import polars as pl
import pyarrow as pa
from polars._typing import ArrowSchemaExportable

import gedi_geoparquet.polars as pl_


class BeamType(StrEnum):
    """GEDI beam types.

    Within each HDF5 file, regardless of GEDI collection, there are eight top
    level groups (see ``BeamName`` for their names), each falling into one of
    two beam types, which are named in the "description" attribute of each beam
    group as either "Coverage beam" or "Full power beam".

    Here we enumerate them more succinctly as either "coverage" or "power",
    following what users commonly term them.
    """

    COVERAGE = auto()
    POWER = auto()


class BeamName(StrEnum):
    """GEDI Beam names.

    Across all GEDI collections, there are eight commonly named beams, appearing
    as top level groups within GEDI HDF5 files.

    They are always associated with specific beam types (see ``BeamType``).
    """

    type: BeamType

    BEAM0000 = auto(), BeamType.COVERAGE
    BEAM0001 = auto(), BeamType.COVERAGE
    BEAM0010 = auto(), BeamType.COVERAGE
    BEAM0011 = auto(), BeamType.COVERAGE
    BEAM0101 = auto(), BeamType.POWER
    BEAM0110 = auto(), BeamType.POWER
    BEAM1000 = auto(), BeamType.POWER
    BEAM1011 = auto(), BeamType.POWER

    def __new__(cls, value: str, type_: BeamType) -> t.Self:
        member = str.__new__(cls, value)
        # For StrEnum types, `auto()` sets values to the lowercase of the keys,
        # so we're overriding that here to ensure uppercase keys, because the
        # group names in the HDF5 files are uppercase.
        member._value_ = value.upper()
        member.type = type_
        return member


EPOCH_NS = np.datetime64("2018-01-01T00:00:00", "ns")
"""Epoch for the GEDI mission.

All `delta_time` datasets are time deltas (in seconds) since this epoch.  When
converting `delta_time` float values to timedelta values to add to this epoch to
compute absolute times, multiply by 1e9 (to convert from float seconds to
nanoseconds), convert to `int`, and use `ns` (nanosecond) resolution.
"""


def to_arrow(file: h5py.File, schema: ArrowSchemaExportable) -> pa.Table:
    """Read a GEDI HDF5 file into an Arrow Table.

    This is the Arrow equivalent of materializing the Polars LazyFrame produced
    by the ``to_polars`` function.  See that function for details.
    """

    schema_metadata = pa.schema(schema).metadata if schema else None  # type: ignore

    return (
        to_polars(file, schema=schema)
        .collect()
        .to_arrow()
        # Polars does not support schema metadata, so we need to add back any
        # such metadata that might have been supplied with the schema argument.
        .replace_schema_metadata(schema_metadata)
    )


def to_polars(file: h5py.File, schema: ArrowSchemaExportable) -> pl.LazyFrame:
    """Lazily read a GEDI HDF5 file into a Polars LazyFrame.

    The specified schema indicates which datasets from each of the beam groups
    within the file to read and populate into the lazyframe (upon collection),
    where the column name of each entry in the schema is expected to be the
    relative name of a dataset within the beam groups.

    A lazyframe is constructed from each beam group using the given schema,
    under the assumption that every beam adheres to the same schema (which
    should be the case).  The resulting beam lazyframes are vertically
    concatenated into a single lazyframe, which is returned.

    Note that the resulting column names are the "basenames" of the column names
    specified in the schema, with the following exceptions: `rx_processing_a?/*`
    is always renamed to `*_a?` to avoid name conflicts.  For example,
    `rx_processing_a1/zcross` and `rx_processing_a2/zcross` are renamed to
    `zcross_a1` and `zcross_a2`, respectively, since they both have the same
    basename of `zcross`.

    Parameters
    ----------
    file
        HDF5 file to lazily read into a Polars LazyFrame.
    schema
        Schema describing which datasets (columns) to read from the input file.
        At a minimum, the schema is expected to include the following fields
        (basenames): `delta_time`, `lat_lowestmode`, and `lon_lowestmode`.

    Returns
    -------
    lazyframe
        A lazyily read dataframe containing columns obtained by reading the
        datasets from the input file, as specified by the schema.  In addition,
        the following columns will be included:

        - `beam_name`: `pl.String` (e.g., "BEAM0000")
        - `beam_type`: `pl.String` ("coverage" or "power")
        - `time`: `pl.Datetime` (GEDI Epoch of 2018-01-01 plus `delta_time`
          seconds, with `ns` resolution in UTC)
        - `geometry`: `pl.Struct` (point structs constructed from the
          `lon_lowestmode` and `lat_lowestmode` datasets, used as `x` and `y`
          values, respectively).

        Further, the `delta_time`, `lat_lowestmode`, and `lon_lowestmode`
        columns are _not_ included, since they are simply the basis for
        computing the `time` and `geometry` columns.

    Examples
    --------
    >>> import h5py
    >>> import polars as pl
    >>> with h5py.File.in_memory() as f:
    ...     beam = f.create_group("BEAM0000")
    ...     _ = beam.create_dataset("agbd", data=[0.9, 2.5], dtype="f8")
    ...     _ = beam.create_dataset("lon_lowestmode",
    ...             data=[-70.51330701882061, -70.51248198865636], dtype="f8")
    ...     _ = beam.create_dataset("lat_lowestmode",
    ...             data=[-51.77306938985101, -51.77306720114365], dtype="f8")
    ...     _ = beam.create_dataset("delta_time",
    ...             data=[44113308.00229415, 44113308.01055815], dtype="f8")
    ...     beam = f.create_group("BEAM1000")
    ...     _ = beam.create_dataset("agbd", data=[3.9, 1.5], dtype="f8")
    ...     _ = beam.create_dataset("lon_lowestmode",
    ...             data=[-70.54086976614937, -70.54004432851809], dtype="f8")
    ...     _ = beam.create_dataset("lat_lowestmode",
    ...             data=[-51.80907508732351, -51.80907305598579], dtype="f8")
    ...     _ = beam.create_dataset("delta_time",
    ...             data=[44113308.0064256, 44113308.0146896], dtype="f8")
    ...     # Pass empty schema to force deriving it from the data.
    ...     lf = to_polars(f, schema=pl.Schema({}))
    ...     lf.collect_schema()
    ...     lf.collect()
    Schema({'agbd': Float64,
            'beam_name': String,
            'beam_type': String,
            'time': Datetime(time_unit='ns', time_zone='UTC'),
            'geometry': Struct({'x': Float64, 'y': Float64})})
    shape: (4, 5)
    ┌──────┬───────────┬───────────┬─────────────────────────────────┬─────────────────────────┐
    │ agbd ┆ beam_name ┆ beam_type ┆ time                            ┆ geometry                │
    │ ---  ┆ ---       ┆ ---       ┆ ---                             ┆ ---                     │
    │ f64  ┆ str       ┆ str       ┆ datetime[ns, UTC]               ┆ struct[2]               │
    ╞══════╪═══════════╪═══════════╪═════════════════════════════════╪═════════════════════════╡
    │ 0.9  ┆ BEAM0000  ┆ coverage  ┆ 2019-05-26 13:41:48.002294152 … ┆ {-70.513307,-51.773069} │
    │ 2.5  ┆ BEAM0000  ┆ coverage  ┆ 2019-05-26 13:41:48.010558152 … ┆ {-70.512482,-51.773067} │
    │ 3.9  ┆ BEAM1000  ┆ power     ┆ 2019-05-26 13:41:48.006425600 … ┆ {-70.54087,-51.809075}  │
    │ 1.5  ┆ BEAM1000  ┆ power     ┆ 2019-05-26 13:41:48.014689600 … ┆ {-70.540044,-51.809073} │
    └──────┴───────────┴───────────┴─────────────────────────────────┴─────────────────────────┘
    """
    beams = filter(_is_beam, file.values())
    beam_lfs = (_beam_to_polars(beam, schema=schema) for beam in beams)

    time = pl.lit(EPOCH_NS) + (pl.col("delta_time") * 1e9).cast(pl.Duration("ns"))
    geometry = pl.struct(x=pl.col("lon_lowestmode"), y=pl.col("lat_lowestmode"))

    return (
        pl.concat(beam_lfs, rechunk=False, parallel=True)
        .rename(_rename_column, strict=False)
        .with_columns(time=time.cast(pl.Datetime("ns", "UTC")), geometry=geometry)
        .drop("delta_time", "lon_lowestmode", "lat_lowestmode")
    )


def _beam_to_polars(
    beam: h5py.Group,
    *,
    schema: ArrowSchemaExportable | None = None,
) -> pl.LazyFrame:
    """Lazily read a GEDI beam group into a Polars LazyFrame.

    The resulting LazyFrame will include all columns specified in ``schema``,
    plus two additional columns: (a) `beam_name` (e.g., `"BEAM0000"`), and (b)
    `beam_type` (either `"coverage"` or `"power"`).  These values are constant
    for the entire beam, and thus will be repeated to fill their columns to
    match the height of the other columns.

    Parameters
    ----------
    beam
        Beam group to read lazily.
    schema
        Schema describing which columns from the beam to lazily read.  Field
        names in the schema may be either names of datasets at the top level of
        ``group``, or relative paths to datasets nested within ``group`` at
        deeper levels.  Absolute paths are supported, but discouraged, as they
        will be less "portable".

        Data types must be compatible with the numpy data types of the
        corresponding datasets.  When a schema data type does not map precisely
        to the dataset's data type, values will be coerced into the data type
        specified by the schema, when possible, which may be lossy.

        If no schema is supplied, one will be derived from _all_ of the columns
        in ``group`` (recursively).

    Returns
    -------
    lazyframe
        Polars LazyFrame for lazily reading data from ``group``.  Note that
        errors in ``schema`` will not be surfaced until the ``collect`` method
        is invoked on this lazyframe.

    Raises
    ------
    ValueError
        If the base name of ``beam`` is not in ``BeamName``.

    Examples
    --------
    >>> import h5py
    >>> import polars as pl
    >>> with h5py.File.in_memory() as f:
    ...     beam = f.create_group("BEAM0000")
    ...     _ = beam.create_dataset("agbd", data=[0.9, 2.5, 1.54], dtype="f8")
    ...     _ = beam.create_dataset("quality_flag", data=[0, 1, 1], dtype="i1")
    ...     # No schema supplied, thus a full schema is precisely derived.
    ...     _beam_to_polars(beam).collect()
    ...     # Schema supplied, but specified data type results in lossy conversion.
    ...     _beam_to_polars(beam, schema=pl.Schema({"agbd": pl.UInt8})).collect()
    shape: (3, 4)
    ┌──────┬──────────────┬───────────┬───────────┐
    │ agbd ┆ quality_flag ┆ beam_name ┆ beam_type │
    │ ---  ┆ ---          ┆ ---       ┆ ---       │
    │ f64  ┆ i8           ┆ str       ┆ str       │
    ╞══════╪══════════════╪═══════════╪═══════════╡
    │ 0.9  ┆ 0            ┆ BEAM0000  ┆ coverage  │
    │ 2.5  ┆ 1            ┆ BEAM0000  ┆ coverage  │
    │ 1.54 ┆ 1            ┆ BEAM0000  ┆ coverage  │
    └──────┴──────────────┴───────────┴───────────┘
    shape: (3, 3)
    ┌──────┬───────────┬───────────┐
    │ agbd ┆ beam_name ┆ beam_type │
    │ ---  ┆ ---       ┆ ---       │
    │ u8   ┆ str       ┆ str       │
    ╞══════╪═══════════╪═══════════╡
    │ 0    ┆ BEAM0000  ┆ coverage  │
    │ 2    ┆ BEAM0000  ┆ coverage  │
    │ 1    ┆ BEAM0000  ┆ coverage  │
    └──────┴───────────┴───────────┘
    """
    beam_name = BeamName[_basename(beam)]

    return pl_.scan_hdf5(beam, schema=schema).with_columns(
        # Converting from a PyArrow table to a Pandas DataFrame (via pl.Table.to_pandas)
        # fails with the following error when using enum columns, so we're
        # sticking with string columns.  (The thought with enum was that perhaps
        # they're stored and queried more efficiently.)
        #
        #     ArrowTypeError: Converting unsigned dictionary indices to pandas not yet
        #     supported, index type: uint8
        #
        # This indicates that it doesn't like this part of the schema (notice that both
        # column types have dictionary indices of typt uint8, corresponding to the
        # error message above, but using string instead of enum resolves the error):
        #
        #     beam_name: dictionary<values=string, indices=uint8, ordered=0>
        #      -- field metadata --
        #      _PL_ENUM_VALUES2: '8;BEAM00008;BEAM00018;BEAM00108;BEAM00118;BEAM01018;' + 28
        #      beam_type: dictionary<values=string, indices=uint8, ordered=0>
        #      -- field metadata --
        #      _PL_ENUM_VALUES2: '8;coverage5;power'
        #
        # beam_name=pl.lit(beam_name).cast(pl.Enum(BeamName)),
        # beam_type=pl.lit(beam_name.type).cast(pl.Enum(BeamType)),
        beam_name=pl.lit(beam_name).cast(pl.String),
        beam_type=pl.lit(beam_name.type).cast(pl.String),
    )


def _is_beam(obj: h5py.HLObject) -> t.TypeGuard[h5py.Group]:
    """Determine if an h5py object is a GEDI BEAMXXXX Group.

    Examples
    --------
    >>> import h5py
    >>> with h5py.File.in_memory() as f:
    ...     aux = f.create_group("auxilliary")
    ...     beam = f.create_group("BEAM0000")
    ...     assert not _is_beam(f)
    ...     assert not _is_beam(aux)
    ...     assert _is_beam(beam)
    """
    return isinstance(obj, h5py.Group) and _basename(obj) in BeamName


def _basename(obj: h5py.Group | h5py.Dataset) -> str:
    """Get the basename of an h5py Group or Dataset.

    Examples
    --------
    >>> import h5py
    >>> with h5py.File.in_memory() as f:
    ...     group = f.create_group("group")
    ...     subgroup = group.create_group("subgroup")
    ...     ds = subgroup.create_dataset("data", shape=(10,))
    ...     f.name, _basename(f)
    ...     group.name, _basename(group)
    ...     subgroup.name, _basename(subgroup)
    ...     ds.name, _basename(ds)
    ('/', '')
    ('/group', 'group')
    ('/group/subgroup', 'subgroup')
    ('/group/subgroup/data', 'data')
    """
    return str(obj.name).rsplit("/", 1)[-1]


def _rename_column(column: str) -> str:
    """Rename a column.

    All columns are renamed to their basename, with the following exception:
    `"rx_processing_a?/*"` is always renamed to `"*_a?"` to avoid name conflicts
    between basenames.

    Examples
    --------
    >>> _rename_column("path/to/my_dataset")
    'my_dataset'
    >>> _rename_column("rx_processing_a1/zcross")
    'zcross_a1'
    >>> _rename_column("rx_processing_a2/zcross")
    'zcross_a2'
    """
    import re

    basename = column.rsplit("/", 1)[-1]

    if match := re.match(r"^rx_processing_a(?P<algnum>\d+)/", column):
        return f"{basename}_a{match['algnum']}"

    return basename


def flatten(group: h5py.Group) -> Mapping[str, h5py.Dataset]:
    """Flatten (recursively) all h5py Datasets within an h5py Group.

    Entries in the group that are soft links to datasets are included in the
    result (see example).

    Arguments
    ---------
    group
        Group to flatten.

    Returns
    -------
    mapping
        Mapping from relative dataset name to dataset for every dataset (at all
        depths) within ``group``.  Each relative name is the name of a dataset
        relative to ``group``'s name.

    Examples
    --------
    >>> import h5py
    >>> with h5py.File.in_memory() as f:
    ...     group = f.create_group("group")
    ...     group_ds = group.create_dataset("group_ds", dtype="f8")
    ...     subgroup = group.create_group("subgroup")
    ...     subgroup_ds = subgroup.create_dataset("subgroup_ds", dtype="i8")
    ...     f["group/subgroup_ds"] = h5py.SoftLink("/group/subgroup/subgroup_ds")
    ...     group_ds.name
    ...     subgroup_ds.name
    ...     flatten(group)
    '/group/group_ds'
    '/group/subgroup/subgroup_ds'
    {'group_ds': <HDF5 dataset "group_ds": shape None, type "<f8">,
     'subgroup/subgroup_ds': <HDF5 dataset "subgroup_ds": shape None, type "<i8">,
     'subgroup_ds': <HDF5 dataset "subgroup_ds": shape None, type "<i8">}
    """
    objects = {}
    group.visit_links(lambda relname: objects.update({relname: group[relname]}))

    return {name: obj for name, obj in objects.items() if isinstance(obj, h5py.Dataset)}


def batched(ds: h5py.Dataset, n: int) -> Iterator[t.Any]:
    """Lazily batch data from a dataset into slices of length ``n``.

    For a dataset with only a single row, each "batch" consists solely of the
    single row. The returned iterator will infinitely repeat the single row
    value so that it can be conveniently zipped with other batched datasets,
    thus automatically filling a "column" to match the heights (lengths) of the
    other batches.

    For a dataset with multiple rows, the last batch will contain `len(ds) % n`
    elements.

    Parameters
    ----------
    ds
        Dataset to create batches from.
    n
        Maximum number of elements in each batch.  All batches will contain
        this many elements, except perhaps for the last batch, which may
        contain fewer.

    Returns
    -------
    Iterator
        Iterator of slices of elements from ``ds``, each containing at most
        ``n`` elements.  The last slice will contain `len(ds) % n` elements.
        If ``ds`` contains only a single row, the single row value will be
        repeated infinitely.

    Examples
    --------
    >>> import h5py

    Given a file with a "scalar" dataset, a 2D dataset with a single row,
    and a 2D dataset with multiple rows:

    >>> f = h5py.File.in_memory()
    >>> scalar = f.create_dataset("scalar", data=[5.0])
    >>> one_row = f.create_dataset("one_row", data=[[0, 1, 2]])
    >>> n_rows = f.create_dataset("n_rows", data=[0, 1, 2, 3, 4])

    Batching the scalar dataset simply repeats the scalar value forever,
    regardless of the value of ``n`` (which is ignored in this case):

    >>> b = batched(scalar, 10_000)
    >>> next(b)
    np.float64(5.0)
    >>> next(b)
    np.float64(5.0)

    The same is also true for the 2D dataset with only a single row (i.e.,
    the single row is repeated forever):

    >>> b = batched(one_row, 10_000)
    >>> next(b)
    array([0, 1, 2])
    >>> next(b)
    array([0, 1, 2])

    Only the dataset with multiple rows gets batched into a finite number of
    slices:

    >>> list(batched(n_rows, 2))
    [array([0, 1]), array([2, 3]), array([4])]
    """
    from itertools import repeat

    slices = (slice(start, start + n) for start in range(0, len(ds), n))

    return repeat(ds[0]) if len(ds) == 1 else (ds[s] for s in slices)
