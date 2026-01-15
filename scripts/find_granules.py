#!/usr/bin/env python

from __future__ import annotations

import io
import sys
from pathlib import Path

from cyclopts import App
import httpx

CMR_MAX_PAGE_SIZE = 2000

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
def find_granules(
    aoi: Path | None = None,
    /,
    *,
    limit: int = CMR_MAX_PAGE_SIZE,
) -> None:
    """Search the CMR for all GEDI L2A, L2B, L4A, and L4C granules within an AOI.

    Parameters
    ----------
    aoi
        GeoJSON text for an area of interest (AOI).  If supplied, read from
        specified file; otherwise, read from stdin.
    """

    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    params = (
        ("concept_id", "C2142771958-LPCLOUD"),
        ("concept_id", "C2142776747-LPCLOUD"),
        ("concept_id", "C2237824918-ORNL_CLOUD"),
        ("concept_id", "C3049900163-ORNL_CLOUD"),
        ("page_size", min(CMR_MAX_PAGE_SIZE, limit)),
    )

    with (
        io.BytesIO(sys.stdin.buffer.read()) if aoi is None else open(aoi, "rb") as f,
        httpx.Client(timeout=30) as client,
    ):
        files = {"shapefile": ("shapefile", f, "application/geo+json")}
        r = client.post(url, params=params, files=files)
        r.raise_for_status()
        hits = int(r.headers["CMR-Hits"])
        results = r.text

    if hits > CMR_MAX_PAGE_SIZE:
        print(
            f"WARNING: Fetched only the first {CMR_MAX_PAGE_SIZE} of {hits}",
            file=sys.stderr,
        )

    print(results)


if __name__ == "__main__":
    try:
        app()
    except httpx.HTTPStatusError as e:
        print(e.response.json()["errors"], file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
