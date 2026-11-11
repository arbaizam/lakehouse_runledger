"""Command-line entry point: ``runledger <command>``.

Only ``ddl`` exists so far; ``doctor``, ``setup``, ``manifest`` and
``reconcile`` follow the roadmap in docs/design.md.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from runledger._version import __version__


def _cmd_ddl(args: argparse.Namespace) -> int:
    from runledger.schema.ddl import render_tables_script

    script = render_tables_script(args.catalog, args.schema, args.prefix)
    if args.out:
        Path(args.out).write_text(script, encoding="utf-8")
    else:
        sys.stdout.write(script)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runledger")
    parser.add_argument("--version", action="version", version=f"runledger {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    ddl = sub.add_parser("ddl", help="Print CREATE TABLE statements for all runledger tables.")
    ddl.add_argument("--catalog", required=True)
    ddl.add_argument("--schema", required=True)
    ddl.add_argument("--prefix", default="rl_")
    ddl.add_argument("--out", help="Write to this file instead of stdout.")
    ddl.set_defaults(func=_cmd_ddl)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ValueError as exc:
        print(f"runledger: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
