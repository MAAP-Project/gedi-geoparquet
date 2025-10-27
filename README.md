# GEDI GeoParquet

Library for converting [GEDI] HDF5 files into GeoParquet files, for the four
GEDI "footprint" collections:

1. [GEDI L2A Elevation and Height Metrics Data Global Footprint Level V002]
1. [GEDI L2B Canopy Cover and Vertical Profile Metrics Data Global Footprint Level V002]
1. [GEDI L4A Footprint Level Aboveground Biomass Density, Version 2.1]
1. [GEDI L4C Footprint Level Waveform Structural Complexity Index, Version 2]

## Setup

This project uses `uv` for dependency management, so you must [install uv].

Once `uv` is installed, install git pre-commit hooks as follows:

```plain
uv run pre-commit install --install-hooks
```

## Generating Apache Arrow Schema

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

## Examining a Schema File

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

## Converting an HDF5 File to a GeoParquet File

To convert a GEDI HDF5 file to a (geo)parquet file, run the following command:

```plain
uv run scripts/convert.py [OPTIONS] INPUT OUTPUT_DIR
```

where:

- Available `OPTIONS` are:
  - `--writer {pyarrow,polars}` (default: `pyarrow`)
  - `--compression {brotli,gzip,lz4,snappy,zstd}` (default: `zstd`)
  - `--compression-level INT` (default: `None`, determined by writer)
- `INPUT` is either a file system path or an HTTP URL to a GEDI HDF5 file.
- `OUTPUT` is a path to a directory in which to write the resulting parquet
  file.  The name of the output file will be the same as the input file, but
  replacing the `.h5` extension with the extension
  `.{compression}-{compression_level}-{writer}.parquet` to accommodate being
  able to compare the sizes of outputs produced by different options.

As examples, here are a few reference URLs that can be used as inputs, one per
collection.  These files are joinable on `shot_number`:

- <https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_A.002/GEDI02_A_2025050224403_O35067_01_T03238_02_004_02_V002/GEDI02_A_2025050224403_O35067_01_T03238_02_004_02_V002.h5>
- <https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_B.002/GEDI02_B_2025050224403_O35067_01_T03238_02_004_01_V002/GEDI02_B_2025050224403_O35067_01_T03238_02_004_01_V002.h5>
- <https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2025050224403_O35067_01_T03238_02_004_01_V002.h5>
- <https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4C_WSCI/data/GEDI04_C_2025050224403_O35067_01_T03238_02_001_01_V002.h5>

Note that this requires that you have a `~/.netrc` populated appropriately, as
described above.

For example:

```plain
uv run scripts/convert.py https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2025050224403_O35067_01_T03238_02_004_01_V002.h5 .
```

which will produce a geoparquet file named:

```plain
GEDI04_A_2025050224403_O35067_01_T03238_02_004_01_V002.zstd-None-pyarrow.parquet
```

> [!NOTE]
>
> Depending upon the size of the input file, when using an HTTP URL, the
> conversion script may take anywhere from about a minute up to several minutes
> to complete.

## Joining Corresponding Files on Shot Number

To join resulting parquet files produced from converting corresponding files
across the 4 GEDI collections, use the `join` script.

For example, the HTTP URLs listed above are links to a set of corresponding
granule files across the collections.  They all contain the same column of shot
number values, so joining them all by shot number will produce a complete
result.

If you run the conversion script on all of them, and produce results in the
current directory, then the following command will join them all together on
`shot_number`, from left to right in the order provided on the command line,
such that duplicate columns from the right are dropped (i.e., the leftmost
column among a set of duplicate input columns "wins"):

```plain
uv run scripts/join.py GEDI0*2025050224403_O35067_01_T03238* GEDI_2025050224403_O35067_01_T03238.parquet
```

The general form of the command is the following, where globbing can be used
for providing the inputs (as shown above):

```plain
uv run scripts/join.py INPUT [INPUT ...] OUTPUT
```

> [!WARNING]
>
> When globbing, take care to ensure that the glob pattern does NOT match the
> output filename.  Otherwise, if you run the same command again, the output
> file itself will be included as an input.  The result should be no different,
> since duplicate columns are dropped, but the join will take longer to perform
> since the total size of the inputs increases significantly (nearly double).

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
