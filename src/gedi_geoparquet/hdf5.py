from __future__ import annotations

import typing as t
from enum import StrEnum, auto

import h5py
import numpy as np
import polars as pl
import pyarrow as pa

import gedi_geoparquet.polars_ as pl_


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


def to_arrow(file: h5py.File, *, schema: pa.Schema) -> pa.Table:
    """Read a GEDI HDF5 file into an Arrow Table.

    This is the Arrow equivalent of the materialized Polars LazyFrame produced
    by the ``to_polars`` function.  See that function for details.
    """

    schema_metadata = schema.metadata if schema else None
    polars_schema = pl.Schema(schema)

    # WARNING: When Polars converts to Arrow, it defaults to using LargeString and
    # LargeList even when String and List will suffice.  This might be problematic.
    # See https://github.com/pola-rs/polars/issues/15047.
    return (
        to_polars(file, schema=polars_schema)
        .collect()
        .to_arrow()
        # Polars does not support schema metadata, so we need to add back any
        # such metadata that might have been supplied with the Arrow schema.
        .replace_schema_metadata(schema_metadata)
    )


def to_polars(file: h5py.File, *, schema: pl.Schema) -> pl.LazyFrame:
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

        - `beam_name`: `pl.Enum` (e.g., "BEAM0000")
        - `beam_type`: `pl.Enum` ("coverage" or "power")
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
            'beam_name': Enum(categories=['BEAM0000', ..., 'BEAM1011']),
            'beam_type': Enum(categories=['coverage', 'power']),
            'time': Datetime(time_unit='ns', time_zone='UTC'),
            'geometry': Struct({'x': Float64, 'y': Float64})})
    shape: (4, 5)
    ┌──────┬───────────┬───────────┬─────────────────────────────────┬─────────────────────────┐
    │ agbd ┆ beam_name ┆ beam_type ┆ time                            ┆ geometry                │
    │ ---  ┆ ---       ┆ ---       ┆ ---                             ┆ ---                     │
    │ f64  ┆ enum      ┆ enum      ┆ datetime[ns, UTC]               ┆ struct[2]               │
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
    schema: pl.Schema | None = None,
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
    │ f64  ┆ i8           ┆ enum      ┆ enum      │
    ╞══════╪══════════════╪═══════════╪═══════════╡
    │ 0.9  ┆ 0            ┆ BEAM0000  ┆ coverage  │
    │ 2.5  ┆ 1            ┆ BEAM0000  ┆ coverage  │
    │ 1.54 ┆ 1            ┆ BEAM0000  ┆ coverage  │
    └──────┴──────────────┴───────────┴───────────┘
    shape: (3, 3)
    ┌──────┬───────────┬───────────┐
    │ agbd ┆ beam_name ┆ beam_type │
    │ ---  ┆ ---       ┆ ---       │
    │ u8   ┆ enum      ┆ enum      │
    ╞══════╪═══════════╪═══════════╡
    │ 0    ┆ BEAM0000  ┆ coverage  │
    │ 2    ┆ BEAM0000  ┆ coverage  │
    │ 1    ┆ BEAM0000  ┆ coverage  │
    └──────┴───────────┴───────────┘
    """
    beam_name = BeamName[_basename(beam)]

    return pl_.scan_hdf5(beam, schema=schema).with_columns(
        beam_name=pl.lit(beam_name).cast(pl.Enum(BeamName)),
        beam_type=pl.lit(beam_name.type).cast(pl.Enum(BeamType)),
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
    *_parent, name = str(obj.name).rsplit("/", 1)
    return name


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
