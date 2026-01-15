#!/usr/bin/env python

from __future__ import annotations

import json
import sys
import typing as t

from cyclopts import App
import httpx

# Use a built-in formatter by name: {"default", "plain"}
# The "plain" formatter's line break logic is a bit off, so sticking with
# "default" for now (see https://github.com/BrianPugh/cyclopts/issues/655).
app = App(help_formatter="default")


@app.default
def fetch_geojson(
    country_code: str,
    level: t.Literal["country", "state", "county"],
    boundary_code: str,
    /,
    *,
    simplified: bool = True,
) -> None:
    """Fetch a GeoJSON from <geoboundaries.org> and print to standard output.

    Parameters
    ----------
    country_code
        ISO-3166-1 (Alpha 3) code for the country of interest.
    level
        Administrative level of area (boundary) of interest.
    boundary_code
        ISO code for the area (boundary) of interest.
    simplified
        Indicates whether or not to fetch geojson with simplified geometry.
    """
    adm = ["country", "state", "county"].index(level)
    geojson = fetch_geoboundaries(country_code, adm, simplified)

    if not (boundary := find_boundary(geojson, boundary_code)):
        msg = f"{level.capitalize()} not found in {country_code}: {boundary_code}"
        raise RuntimeError(msg)

    print(json.dumps({**geojson, "features": [boundary]}, indent=4))


def fetch_geoboundaries(
    country_code: str,
    level: int,
    simplified: bool,
) -> dict[str, t.Any]:
    with httpx.Client(follow_redirects=True, timeout=15) as client:
        url = fetch_download_url(client, country_code, level, simplified)
        r = client.get(url)
        r.raise_for_status()
        geojson = r.json()

    return t.cast(dict[str, t.Any], geojson)


def fetch_download_url(
    client: httpx.Client,
    country_code: str,
    level: int,
    simplified: bool,
) -> str:
    # See https://www.geoboundaries.org/api.html
    url = f"https://www.geoboundaries.org/api/current/gbOpen/{country_code}/ADM{level}"
    r = client.get(url)
    r.raise_for_status()
    metadata = r.json()

    return str(metadata["simplifiedGeometryGeoJSON" if simplified else "gjDownloadURL"])


def find_boundary(
    geojson: dict[str, t.Any],
    boundary_code: str,
) -> dict[str, t.Any] | None:
    boundaries = geojson["features"]

    return next(
        (
            boundary
            for boundary in boundaries
            if boundary["properties"]["shapeISO"] == boundary_code
        ),
        None,
    )


if __name__ == "__main__":
    try:
        app()
    except httpx.HTTPStatusError as e:
        print(e.response.json()["errors"], file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
