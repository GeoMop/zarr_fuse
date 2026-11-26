"""Generate a separate, read-only-dashboard overview for a configured variable."""

import argparse
from pathlib import Path

from dashboard.config import load_environment_from_config
from dashboard.data import LocalClient


def main() -> None:
    """Read from the configured source and write a new local sidecar."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("views", type=Path)
    parser.add_argument("view")
    parser.add_argument("group")
    parser.add_argument("variable")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--bins", type=int, default=750)
    args = parser.parse_args()
    load_environment_from_config(args.views)
    result = LocalClient(args.views).generate_timeseries_overview(
        args.view,
        args.group,
        args.variable,
        args.destination,
        bins=args.bins,
    )
    print(result)


if __name__ == "__main__":
    main()
