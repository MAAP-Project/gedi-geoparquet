#!/usr/bin/env python

from __future__ import annotations

import argparse
import typing as t
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa


class BooleanAction(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: str,
        **kwargs: t.Any,
    ):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[t.Any] | None,
        option_string: str | None = None,
    ) -> None:
        if option_string:
            setattr(namespace, self.dest, not option_string.startswith("--no"))


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Dump contents of a binary PyArrow schema file in human-readable form."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "schema_file",
        type=Path,
        help="Path to file containing PyArrow schema (as bytes)",
    )
    parser.add_argument(
        "--truncate-metadata",
        "--no-truncate-metadata",
        dest="truncate_metadata",
        action=BooleanAction,
        default=False,
        help="limit metadata key/value display to a single line of ~80 characters",
    )
    parser.add_argument(
        "--show-field-metadata",
        "--no-show-field-metadata",
        dest="show_field_metadata",
        action=BooleanAction,
        default=True,
        help="display field-level key/value metadata",
    )
    parser.add_argument(
        "--show-schema-metadata",
        "--no-show-schema-metadata",
        dest="show_schema_metadata",
        action=BooleanAction,
        default=True,
        help="display schema-level key/value metadata",
    )

    return parser.parse_args(argv)


def main(
    *,
    schema_file: Path,
    truncate_metadata: bool,
    show_field_metadata: bool,
    show_schema_metadata: bool,
) -> None:
    # This line works and type checks:
    #
    #   schema = pa.ipc.read_schema(pa.py_buffer(schema_file.read_bytes()))
    #
    # It is equivalent to the following line, but the following line does not
    # type check.  Although `str` and `Path` are both supported argument types,
    # they are not covered by the type annotation on `read_schema`.
    schema = pa.ipc.read_schema(schema_file)  # type: ignore

    print(
        schema.to_string(
            truncate_metadata=truncate_metadata,
            show_field_metadata=show_field_metadata,
            show_schema_metadata=True,
        )
    )

    if show_schema_metadata:
        # Even when `truncate_metadata` is `False`, some truncation may still
        # occur, so we'll directly dumpy the metadata so we can view it all.
        print("-- schema metadata --")
        print("geo:", schema.metadata[b"geo"].decode())


if __name__ == "__main__":
    import sys

    args = parse_args(sys.argv[1:])
    main(**dict(args._get_kwargs()))
