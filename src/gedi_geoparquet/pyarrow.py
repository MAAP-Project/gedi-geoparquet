from __future__ import annotations

import typing as t

import h5py
import pyarrow as pa
from polars._typing import ArrowSchemaExportable

import gedi_geoparquet.hdf5 as hdf5_


def infer_schema(group: h5py.Group) -> ArrowSchemaExportable:
    """Infer an Arrow Schema for an h5py Group.

    Flatten the group, such that all h5py Datasets at all levels within the
    group are treated as though they are direct children of the group, with
    names relative to the group name.

    Attributes of ``group`` are translated to metadata on the returned schema,
    and the attributes of each dataset are translated to metadata on each
    corresponding field.  Every attribute value is converted to a JSON string,
    with numpy array attribute values first converted to lists.

    Arguments
    ---------
    group
        Group for which to infer an Arrow Schema.

    Returns
    -------
    schema
        Arrow Schema with one field per dataset within ``group`` (recursive),
        where each entry name is the name of the corresponding dataset relative
        to the name of ``group``.  The data type of a field is converted from
        the numpy datatype of the corresponding dataset.  For object types,
        strings are assumed. For multi-dimensional datasets, the data type is
        nested in `pyarrow.list_()` for each dimension beyond the first (see
        example).

        The attributes of a dataset are JSON-serialized into metadata on the
        corresponding field, with numpy array attribute values converted to
        plain lists first (as they do not directly serialize to JSON strings).

        Attributes on the group are ignored (i.e., they are not converted to
        schema metadata).  This is because it is assumed that the group is
        simply a representative for multiple groups containing identical
        structure, so there is no way to know attribute values that are common
        across all "sibling" groups.

    Examples
    --------
    >>> import h5py
    >>> import numpy as np

    Notice that the `h5py` string dtype resolves to the numpy object dtype, not
    the numpy string dtype:

    >>> str_dtype = h5py.string_dtype()
    >>> str_dtype
    dtype('O')

    When inferring a schema, object types are translated to string types:

    >>> with h5py.File.in_memory() as f:
    ...     group = f.create_group("group")
    ...     group.attrs["description"] = "group description"
    ...     ds_1d = group.create_dataset("ds_1d", shape=(10_000,), dtype="f8")
    ...     ds_1d.attrs["range"] = np.array([0.0, 1.0])
    ...     ds_2d = group.create_dataset("ds_2d", shape=(10_000, 10), dtype="f8")
    ...     ds_str = group.create_dataset("ds_str", shape=(10_000,), dtype=str_dtype)
    ...     subgroup = group.create_group("subgroup")
    ...     ds_0d = subgroup.create_dataset("ds_0d", dtype="i8")
    ...     ds_3d = subgroup.create_dataset("ds_3d", shape=(10_000, 10, 3), dtype="u1")
    ...     infer_schema(group)
    ds_1d: double not null
      -- field metadata --
      range: '[0.0, 1.0]'
    ds_2d: fixed_size_list<item: double>[10] not null
      child 0, item: double
    ds_str: string not null
    subgroup/ds_0d: int64 not null
    subgroup/ds_3d: fixed_size_list<item: fixed_size_list<item: uint8>[10]>[3] not null
      child 0, item: fixed_size_list<item: uint8>[10]
          child 0, item: uint8
    """
    datasets = hdf5_.flatten(group)
    fields = (_field_from_dataset(name, ds) for name, ds in datasets.items())

    return t.cast(ArrowSchemaExportable, pa.schema(fields))


def _field_from_dataset(name: str, ds: h5py.Dataset) -> pa.Field[t.Any]:
    """
    Examples
    --------
    """
    return pa.field(
        name,
        _schema_dtype(ds),
        nullable=False,
        metadata=_metadata_from_attributes(ds),
    )


def _metadata_from_attributes(
    obj: h5py.HLObject,
) -> dict[str | bytes, str | bytes] | None:
    """
    Examples
    --------
    """
    import json
    import numpy as np

    return {
        name: json.dumps(value.tolist() if isinstance(value, np.ndarray) else value)
        for name, value in obj.attrs.items()
    } or None


def _schema_dtype(ds: h5py.Dataset) -> pa.DataType:
    """Determine the Arrow DataType for an HDF5 Dataset.

    Parameters
    ----------
    ds
        Dataset to determine Arrow DataType for, for use in an Arrow Schema,
        taking dimensionality into account.

    Returns
    -------
    datatype
        Arrow DataType corresponding to the numpy datatype of ``ds``.  For the
        numpy object dtype, this is `pyarrow.string()`.  For multi-dimensional
        datasets, the Arrow data type is nested within `pyarrow.list_()` for
        each dimension beyond the first (see examples).

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
    ...     ds_3d = f.create_dataset("ds_3d", shape=(10_000, 10, 3), dtype="f8")
    ...     ds_obj = f.create_dataset("ds_str", shape=(10_000,), dtype=obj_type)
    ...     _schema_dtype(ds_1d)
    ...     _schema_dtype(ds_2d)
    ...     _schema_dtype(ds_3d)
    ...     _schema_dtype(ds_obj)
    DataType(double)
    FixedSizeListType(fixed_size_list<item: double>[10])
    FixedSizeListType(fixed_size_list<item: fixed_size_list<item: double>[10]>[3])
    DataType(string)
    """
    from functools import reduce

    # pyarrow.from_numpy_dtype does not handle numpy Object type ("O"), so we
    # have to handle it ourselves, and we simply assume it represents a string.
    base_dtype = pa.string() if ds.dtype.char == "O" else pa.from_numpy_dtype(ds.dtype)

    return reduce(
        lambda dtype, i: pa.list_(dtype, ds.shape[i]),
        range(1, ds.ndim),
        base_dtype,
    )
