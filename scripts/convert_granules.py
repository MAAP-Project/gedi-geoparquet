#!/usr/bin/env python

from __future__ import annotations

import asyncio
import itertools
import json
import os
import re
import sys
import tempfile
import typing as t
from collections.abc import Sequence
from contextlib import ExitStack
from functools import reduce
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiomultiprocess
import cachetools
import fsspec
import fsspec.implementations
import fsspec.implementations.dirfs
import h5py
import polars as pl
import s3fs
from aiofile import async_open
from cyclopts import App

import gedi_geoparquet as gedi
from gedi_geoparquet.schema import GEOPARQUET_METADATA

_session: aiohttp.ClientSession | None = None


class UmmG(t.TypedDict, total=True):
    GranuleUR: str
    CollectionReference: CollectionReference
    RelatedUrls: Sequence[RelatedUrl]


class CollectionReference(t.TypedDict, total=True):
    ShortName: str
    Version: str


class RelatedUrl(t.TypedDict, total=True):
    URL: str
    Type: str


# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
async def convert_granules(
    metadata: Path | None = None,
    /,
    *,
    limit: int | None = None,
    output: str,
    overwrite: bool = False,
    protocol: t.Literal["s3", "https"] = "https",
) -> None:
    """Group GEDI data by orbit segment and join on `shot_number`.

    This is intentionally a simple conversion of each group of 4 GEDI `.h5`
    files (1 from each GEDI footprint collection) for an orbit segment into a
    single parquet file for the orbit segment, and is non-partitioned so that
    downstream partitioning processes can use the parquet files as input to
    partition the data as desired.

    Groups granules by their "segment identifiers" (e.g.,
    `2019115174243_O02079_04_T03463`, but technically only the `O02079_04` part
    is the orbit number and orbit segment number [`01`-`04`]). Each such group
    should contain data for the same set of shots, thus representing a "unified"
    view of GEDI data for the shots across the collections for the orbit
    segment.

    To avoid missing data, discards all segment groups that do not contain 4
    granules (one from each of the 4 collections), then performs an inner join
    on the data (again, to avoid missing data) from each granule data file on
    `shot_number`, and writes the result to a single, "unified" GeoParquet file
    for the group's orbit segment (e.g.,
    `GEDI_2019115174243_O02079_04_T03463.parquet`).

    Assumes the JSON input is the umm-json-formatted result of a CMR granule
    query for granules from all of the GEDI L2A, L2B, L4A, and L4C collections
    combined. More specifically, the query must have been made by supplying the
    key (e.g., concept_id, short_name, doi) for all 4 collections, so the search
    results include granules from all 4 collections.

    Parameters
    ----------
    metadata
        UMM-G JSON of the results of a CMR query for GEDI granules across all
        four GEDI footprint collections (L2A, L2B, L4A, L4C).  If supplied,
        read from specified file; otherwise, read from stdin.
    limit
        Limited number of segments to "unify" from the given metadata.  If not
        specified, unify all complete groups found within the given metadata.
    output
        Directory to write "unified" GeoParquet files to. Files will be named
        the same as the "segment", with the prefix `GEDI` and extension
        `.parquet` (e.g., `GEDI_2019115174243_O02079_04_T03463.parquet`).
        Existing files will not be overwritten, unless ``overwrite`` is true.
    overwrite
        Indicates whether or not existing parquet files will be overwritten.
    """
    download_url = https_download_url if protocol == "https" else s3_access_urls
    ummgs = parse_metadata(metadata)
    ummg_groups = group_by_suborbit(ummgs)
    sub_download_urls = tuple(
        (sub, tuple(map(download_url, sub_ummgs))) for sub, sub_ummgs in ummg_groups
    )
    subs, urlss = zip(*itertools.islice(sub_download_urls, limit))
    ofs = open_files(output, subs)
    processes = (os.cpu_count() or 4) // 2

    async with aiomultiprocess.Pool(
        processes=processes,
        queuecount=processes,
        childconcurrency=2,
    ) as pool:
        overwrites = itertools.repeat(overwrite)
        argss = tuple(zip(urlss, ofs, overwrites))
        _ = await pool.starmap(join_group, argss)


def parse_metadata(metadata: Path | None) -> tuple[UmmG, ...]:
    # Read metadata (results from CMR granule query), convert to dict, and
    # extract "umm" entries from items.
    contents = sys.stdin.read() if metadata is None else metadata.read_text()
    ummg_results = json.loads(contents)

    return tuple(item["umm"] for item in ummg_results["items"])


def group_by_suborbit(ummgs: Sequence[UmmG]) -> tuple[tuple[str, Sequence[UmmG]], ...]:
    # Sort granule metadata by sub-orbit, then group by sub-orbit.
    groups = itertools.groupby(sorted(ummgs, key=suborbit), suborbit)

    # Keep only full groups, and sort their UMM-G values by collection short name
    # so that duplicate columns are retained from lower level granules upon joining.
    return tuple(
        (sorbit, tuple(sorbit_ummgs))
        for sorbit, isorbit_ummgs in groups
        if len(sorbit_ummgs := sorted(isorbit_ummgs, key=short_name)) == 4
    )


def suborbit(ummg: UmmG) -> str:
    granule_ur = ummg["GranuleUR"]

    # Example: 2019115174243_O02079_04_T03463
    if match := re.search(r"(?P<sub_orbit>[0-9]+_O[0-9]+_0[1-4]_T[0-9]+)", granule_ur):
        return match["sub_orbit"]

    msg = f"Unable to extract sub-orbit from {granule_ur}"
    raise RuntimeError(msg)


def https_download_url(ummg: UmmG) -> str:
    return next(
        related_url["URL"]
        for related_url in ummg["RelatedUrls"]
        if related_url["Type"] == "GET DATA"
    )


def s3_access_urls(ummg: UmmG) -> tuple[str, str]:
    data_url = next(
        related_url["URL"]
        for related_url in ummg["RelatedUrls"]
        if related_url["Type"] == "GET DATA VIA DIRECT ACCESS"
    )
    credentials_url = next(
        url
        for related_url in ummg["RelatedUrls"]
        if (url := related_url["URL"]).endswith("/s3credentials")
    )

    return data_url, credentials_url


def short_name(ummg: UmmG) -> str:
    return ummg["CollectionReference"]["ShortName"]


def open_files(dest: str, sorbits: Sequence[str]) -> Sequence[fsspec.core.OpenFile]:
    fs: fsspec.AbstractFileSystem
    fs, dest = fsspec.url_to_fs(dest)
    fs = fsspec.implementations.dirfs.DirFileSystem(dest, fs=fs)

    filenames = (f"GEDI_{sorbit}.parquet" for sorbit in sorbits)

    return tuple(fsspec.core.OpenFile(fs, filename, "wb") for filename in filenames)


async def join_group(
    urls: Sequence[str] | Sequence[tuple[str, str]],
    of: fsspec.core.OpenFile,
    overwrite: bool = False,
) -> str:
    output_name: str = of.full_name

    if not overwrite and of.fs.exists(output_name):
        return output_name

    files = await (
        https_fetch_group(t.cast(Sequence[str], urls))
        if any(isinstance(url, str) for url in urls)
        else s3_fetch_group(t.cast(Sequence[tuple[str, str]], urls))
    )

    print(f"Joining results to {output_name}")

    tmp = tempfile.NamedTemporaryFile(delete_on_close=False)

    try:
        with ExitStack() as stack, tmp as fp, of as dst:
            h5s = [stack.enter_context(h5py.File(file)) for file in files]
            schemas = [gedi.abridged_schema(f"{h5.attrs['short_name']}") for h5 in h5s]
            lfs = (gedi.to_polars(h5, schema) for h5, schema in zip(h5s, schemas))
            lf = apply_quality_filters(reduce(join_on_shot_number, lfs))

            # Write to temporary location in case something goes sideways before
            # we're finished.
            await asyncio.to_thread(lf.sink_parquet, fp, metadata=GEOPARQUET_METADATA)
            fp.close()

            # Copy tempoary file to final destination.
            async with async_open(tmp.name, mode="rb") as src:
                async for chunk in src.iter_chunked(8 * 1024 * 1024):
                    dst.write(chunk)
    finally:
        for file in files:
            print(f"Deleting {file}")
            Path(file).unlink(missing_ok=True)

    print(f"Finished joining results to {output_name}")

    return output_name


async def https_fetch_group(download_urls: Sequence[str]) -> Sequence[str]:
    """Fetch granule files, writing to temporary local files."""
    session = get_session()
    return await asyncio.gather(
        *[https_fetch_granule(session, url) for url in download_urls]
    )


async def https_fetch_granule(session: aiohttp.ClientSession, url: str) -> str:
    print(f"Fetching {url}")

    temp_filename = get_tempfile(url)

    async with (
        session.get(url, timeout=aiohttp.ClientTimeout()) as response,
        async_open(temp_filename, mode="wb") as local_file,
    ):
        async for chunk in response.content.iter_chunked(8 * 1024 * 1024):
            await local_file.write(chunk)

    print(f"Finished fetching {temp_filename}")

    return temp_filename


def get_tempfile(url: str) -> str:
    _, filename = urlparse(url).path.rsplit("/", 1)
    filepath = Path(filename)
    f = tempfile.NamedTemporaryFile(
        delete=False,
        prefix=f"{filepath.stem}.",
        suffix=filepath.suffix,
    )
    f.close()

    return f.name


async def s3_fetch_group(
    download_url_pairs: Sequence[tuple[str, str]],
) -> Sequence[str]:
    """Fetch granule files, writing to temporary local files."""
    return await asyncio.gather(
        *[
            s3_fetch_granule(data_url=data_url, credentials_url=credentials_url)
            for data_url, credentials_url in download_url_pairs
        ]
    )


async def s3_fetch_granule(
    *,
    data_url: str,
    credentials_url: str,
) -> str:
    print(f"Fetching {data_url}")

    temp_filename = get_tempfile(data_url)
    s3 = await get_s3fs(credentials_url)

    async with (
        await s3.open_async(data_url) as remote_file,
        async_open(temp_filename, mode="wb") as local_file,
    ):
        while chunk := await remote_file.read(8 * 1024 * 1024):
            await local_file.write(chunk)

    print(f"Finished fetching {temp_filename}")
    return temp_filename


async def get_s3fs(s3_credentials_url: str) -> s3fs.S3FileSystem:
    creds = await get_s3_credentials(s3_credentials_url)
    s3 = s3fs.S3FileSystem(
        asynchronous=True,
        key=creds["accessKeyId"],
        secret=creds["secretAccessKey"],
        token=creds["sessionToken"],
    )
    await s3.set_session()

    return s3


_creds_cache = cachetools.TTLCache(maxsize=10, ttl=50 * 60)
_lock = asyncio.Lock()


async def get_s3_credentials(s3_credentials_url: str) -> dict[str, str]:
    if s3_credentials_url in _creds_cache:
        return t.cast(dict[str, str], _creds_cache[s3_credentials_url])

    async with _lock:
        if s3_credentials_url in _creds_cache:
            return t.cast(dict[str, str], _creds_cache[s3_credentials_url])

        print(f"Fetching credentials from {s3_credentials_url}")

        async with get_session().get(
            s3_credentials_url,
            # If EARTHDATA_TOKEN not set, netrc file will be checked.
            headers={"Authorization": f"Bearer {os.environ['EARTHDATA_TOKEN']}"},
            timeout=aiohttp.ClientTimeout(),
        ) as response:
            creds = await response.json(content_type="text/html")
            _creds_cache[s3_credentials_url] = creds

        return t.cast(dict[str, str], _creds_cache[s3_credentials_url])


def get_session() -> aiohttp.ClientSession:
    global _session

    if _session is None:
        _session = aiohttp.ClientSession(raise_for_status=True, trust_env=True)

    return _session


def join_on_shot_number(left: pl.LazyFrame, right: pl.LazyFrame) -> pl.LazyFrame:
    return left.join(right, on="shot_number", how="inner", coalesce=True).select(
        # Drop duplicate columns (oddly, coalesce=True doesn't appear to avoid
        # this duplication; perhaps I'm missing something).
        pl.exclude("^.*_right$")
    )


def apply_quality_filters(lf: pl.LazyFrame) -> pl.LazyFrame:
    degrades = {0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68}

    return lf.filter(
        pl.col("degrade_flag").is_in(degrades),
        pl.col("sensitivity") >= 0.95,
        pl.col("sensitivity_a2") >= 0.95,
        quality_flag=1,
        surface_flag=1,
    ).drop("quality_flag", "surface_flag")


if __name__ == "__main__":
    app()
