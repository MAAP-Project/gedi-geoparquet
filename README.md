# GEDI GeoParquet

Library for converting [GEDI] HDF5 files into GeoParquet files, for the four
GEDI "footprint" collections:

1. [GEDI L2A Elevation and Height Metrics Data Global Footprint Level V002]
1. [GEDI L2B Canopy Cover and Vertical Profile Metrics Data Global Footprint Level V002]
1. [GEDI L4A Footprint Level Aboveground Biomass Density, Version 2.1]
1. [GEDI L4C Footprint Level Waveform Structural Complexity Index, Version 2]

## Contributing

### Setup

This project uses `uv` for dependency management, so you must [install uv].

Once `uv` is installed, install git pre-commit hooks as follows:

```plain
uv run pre-commit install --install-hooks
```

### Generating Apache Arrow Schema

In order to convert individual HDF5 files from each GEDI collection into
equivalent GeoParquet files, we must first construct an Apache Arrow schema file
per collection.

Since there are hundreds of datasets defined for each collection, we'll specify
which particular HDF5 datasets we're interested in converting.  These are
specified in text files in the `datasets` directory, where each file is a simple
list of relative dataset paths to be converted, relative to the `/BEAMxxxx`
top-level groups within each HDF5 file.

Therefore, constructing a schema file for a collection requires 2 inputs:

- A list of desired datasets from the collection
- A sample HDF5 file from which to construct an Arrow Field for each desired
  dataset (and collect all fields into a schema)

Since each HDF5 file is 10s to 100s of megabytes, no sample files are contained
in this repository.  Instead, when a schema is generated for a collection, a
sample HDF5 file is downloaded to use as input.

However, in order to download a sample file, you must have an [Earthdata Login]
account, and your `~/.netrc` file must contain an entry like the following,
replacing `USERNAME` and `PASSWORD` with your Earthdata Login credentials:

```plain
machine urs.earthdata.nasa.gov login USERNAME password PASSWORD
```

Then, to generate the Arrow Schemas for all collections, simply run the
following command:

```plain
make
```

This will do the following for each collection:

- Download a sample HDF5 file (see `Makefile`)
- Construct an Arrow Schema using the sample file and the corresponding file
  from the `datasets` directory.
- Write the schema (in binary form) to the `schemas` directory.

### Examining a Schema File

To examine a schema file, run the following command, which will read the binary
schema file and dump it to the terminal in human-readable form, where `FILE` is
the name of the binary schema files:

```plain
uv run scripts/dump_schema.py schemas/FILE
```

This will output a list of all of the fields in the schema (each constructed
from a dataset listed in the corresponding datasets file), along with metadata.

If you would like to list fields (names and data types) in the schema without
showing metadata, run the following instead:

```plain
uv run scripts/dump_schema.py --no-show-field-metadata --no-show-schema-metadata schemas/FILE
```

### Converting an HDF5 File to a GeoParquet File

TBD

[Earthdata Login]:
  https://urs.earthdata.nasa.gov/
[GEDI]:
  https://gedi.umd.edu/
[GEDI L2A Elevation and Height Metrics Data Global Footprint Level V002]:
  https://www.earthdata.nasa.gov/data/catalog/lpcloud-gedi02-a-002
[GEDI L2B Canopy Cover and Vertical Profile Metrics Data Global Footprint Level V002]:
  https://www.earthdata.nasa.gov/data/catalog/lpcloud-gedi02-b-002
[GEDI L4A Footprint Level Aboveground Biomass Density, Version 2.1]:
  https://www.earthdata.nasa.gov/data/catalog/ornl-cloud-gedi-l4a-agb-density-v2-1-2056-2.1
[GEDI L4C Footprint Level Waveform Structural Complexity Index, Version 2]:
  https://www.earthdata.nasa.gov/data/catalog/ornl-cloud-gedi-l4c-wsci-2338-2
[Install uv]:
  https://docs.astral.sh/uv/getting-started/installation/
