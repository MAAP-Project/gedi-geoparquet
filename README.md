# GEDI GeoParquet

Library for converting [GEDI] HDF5 files into GeoParquet files, for the four
GEDI "footprint" collections:

1. [GEDI L2A Elevation and Height Metrics Data Global Footprint Level V002]
1. [GEDI L2B Canopy Cover and Vertical Profile Metrics Data Global Footprint Level V002]
1. [GEDI L4A Footprint Level Aboveground Biomass Density, Version 2.1]
1. [GEDI L4C Footprint Level Waveform Structural Complexity Index, Version 2]

## TL;DR

If you want to get started quickly:

1. Follow the setup instructions in the next section.
1. Run `make join`, which will convert 1 granule file (`.h5`) per collection
   into a GeoParquet (`.parquet`) file, and join the resulting parquet files on
   `shot_number` into a single parquet file, writing all parquet files to the
   `data` directory (ignored by git). (See `Makefile` for the specific granule.)
1. As desired, run `scripts/dump_schema.py` on any of the generated parquet
   files or on the `.arrows` files under `src/gedi_geoparquet/schema/resources`
   to examine the various schema (see [Examining Schema](#examining-schema)).

## Setup

### Development Preparation

This project uses `uv` for dependency management, so you must [install uv].

Once `uv` is installed, install git pre-commit hooks as follows:

```plain
uv run pre-commit install --install-hooks
```

### Script Execution Preparation

In order to be able to run most of the scripts (or `make` commands, which run
the scripts for you) described in the remaining sections, you must do the
following:

1. Create an [Earthdata Login] account, if you don't have one.
1. Add an entry to your `netrc` file like the following, replacing `USERNAME`
   and `PASSWORD` with your Earthdata Login credentials:

   ```plain
   machine urs.earthdata.nasa.gov login USERNAME password PASSWORD
   ```

## Generating Apache Arrow Schema

In order to convert individual HDF5 files from each GEDI collection into
equivalent GeoParquet files, we must first construct an Apache Arrow schema file
per collection, which requires a sample HDF5 file per collection.

Since each HDF5 file is 10s to 100s of megabytes, no sample files are contained
in this repository.  Instead, to generate a schema for a collection, a sample
HDF5 file is read remotely from NASA's Eartdata archive.

Then, to generate the Arrow Schemas for all collections, simply run the
following command:

```plain
make schema
```

This will do the following for each collection:

- Read a sample HDF5 file (see `Makefile`)
- Construct an Arrow Schema using the sample file (from group `/BEAM0000`).
- Write the schema (in binary form) to the
  `src/gedi_geoparquet/schema/resources` directory, but only if changes have
  been made to `scripts/generate_schema.py` or any python files in the
  `gedi_geoparquet` package.  These schema files _are_ committed to git.

## Examining Schema

To examine a schema file (arrow stream), or the schema in a parquet file (see
below for converting an HDF5 GEDI file to a parquet file), run the following
command, which will read the schema and dump it to the terminal in
human-readable form, where `FILE` is the name of binary schema file or parquet
file:

```plain
uv run scripts/dump_schema.py FILE
```

This will output a list of all of the fields in the schema, along with metadata.
If you would like to list fields (names and data types) in the schema without
showing metadata, run the following instead:

```plain
uv run scripts/dump_schema.py --no-show-field-metadata --no-show-schema-metadata FILE
```

## Converting an HDF5 File to a GeoParquet File

> [!NOTE] TL;DR
>
> If you want to easily convert a set of granule files (one per collection,
> joinable on shot_number), you can run the following command, which will take a
> few minutes to complete:
>
> ```plain
> make convert
> ```
>
> This will run the convert command described below, so you don't have to do so
> yourself, if you just want to produce a sample set of parquet files.  This
> will produce a parquet file for 1 granule from each collection, written to the
> `data` directory (ignored by git).

To convert a GEDI HDF5 file to a (geo)parquet file, run the following command:

```plain
uv run scripts/convert.py [OPTIONS] INPUT OUTPUT_DIR
```

where:

- Available `OPTIONS` are:
  - `--compression {brotli,gzip,lz4,snappy,zstd}` (default: `zstd`)
  - `--compression-level INT` (default: `None`, determined by writer)
- `INPUT` is either a file system path or an HTTP URL to a GEDI HDF5 file.
- `OUTPUT` is a path to a directory in which to write the resulting parquet
  file.  The name of the output file will be the same as the input file, but
  replacing the `.h5` extension with the extension `.parquet`.

Note that this requires that you have a `~/.netrc` populated appropriately, as
described above.

For example:

```plain
uv run scripts/convert.py https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2025050224403_O35067_01_T03238_02_004_01_V002.h5 .
```

which will produce a geoparquet file named:

```plain
GEDI04_A_2025050224403_O35067_01_T03238_02_004_01_V002.parquet
```

> [!NOTE]
>
> Depending upon the size of the input file, when using an HTTP URL, the
> conversion script may take anywhere from about a minute up to several minutes
> to complete.

## Joining Corresponding Files on Shot Number

> [!NOTE] TL;DR
>
> If you ran the `make convert` command described in the previous section, or
> even if you skipped doing so, and you want to join a set of corresponding
> granule files (1 from each collection) into a single parquet file joined on
> `shot_number`, simply run the following command:
>
> ```plain
> make join
> ```
>
> If you haven't already run `make convert`, this will automatically do so for
> you. It will then join the granule files by `shot_number` to produce a single
> resulting `.parquet` file in the `data` directory.
>
> This will take several minutes to complete, and requires an appropriate entry
> in a `netrc` file, as described above.

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
