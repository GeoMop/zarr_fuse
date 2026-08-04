from __future__ import annotations

import argparse
import fnmatch
import sys
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from dotenv import load_dotenv

import xarray as xr
import zarr_fuse as zf



def peeloff_dot_args(argv: List[str], prefix: str) -> Tuple[Dict[str, str], List[str]]:
    """
    Peel off `argv` items in form:
        <prefix>.<KEY>=<VALUE>
    """
    kwargs: Dict[str, str] = {}
    remaining: List[str] = []
    raw_prefix = prefix
    prefix = f"{prefix}."
    plen = len(prefix)
    for arg in argv:
        if arg.startswith(prefix):
            key_val = arg[plen:]
            try:
                key, value = key_val.split("=", 1)
            except ValueError as exc:
                raise SystemExit(
                    f"Invalid option '{arg}'. Expected format {raw_prefix}.<KEY>=<VALUE>."
                ) from exc
            kwargs[key] = value
        else:
            remaining.append(arg)
    return kwargs, remaining


def _iter_matching_nodes(*, node, store_name: str, path_glob: str) -> Iterator[tuple[str, object]]:
    node_path = node.group_path
    full_path = store_name if node_path == "" else f"{store_name}/{node_path}"

    if fnmatch.fnmatch(full_path, path_glob) or fnmatch.fnmatch(node_path or "", path_glob):
        yield full_path, node

    for _, child in sorted(node.items()):
        yield from _iter_matching_nodes(node=child, store_name=store_name, path_glob=path_glob)


def _format_group_line(*, full_path: str, node) -> str:
    child_count = len(node.children)
    array_count = len(node.dataset.data_vars) + len(node.dataset.coords)
    return f"  {full_path} [group] groups={child_count} arrays={array_count}"


def dataset_summary(*, node) -> Iterator[str]:
    with xr.set_options(
            display_width=180,  # wider lines
            display_max_rows=80,  # more coords/data_vars/attrs rows
            display_values_threshold=10,  # allow more array values before summarizing
            display_expand_coords=True,
            display_expand_data_vars=True,
            display_style="text",  # useful in notebooks if HTML repr collapses things
    ):
        summary = str(node.dataset)
    return summary


def _store_name_from_url(store_url: str) -> str:
    if "://" in store_url:
        store_url = store_url.removeprefix("file://")
        store_url = store_url.removeprefix("zip://")
        store_url = store_url.removeprefix("s3://")
    return Path(store_url).name or store_url


def _open_list_store(store_kwargs: Dict[str, str]) -> tuple[Node, str]:
    options = _zarr_fuse_options(None, **store_kwargs)
    store_url = options.get("STORE_URL")
    if not store_url:
        raise SystemExit("list requires --store.STORE_URL=<store-url>")
    store = _zarr_store_open(options)
    return Node.read_store(store), _store_name_from_url(str(store_url))


def _open_source_root(schema_path: Path, src_kwargs: dict) -> Node:
    src_schema = zarr_schema.deserialize(schema_path)
    options = _zarr_fuse_options(src_schema, **src_kwargs)
    store = _zarr_store_open(options)
    return Node.read_store(store)


def _open_destination_root(schema_path: Path, dst_kwargs: dict) -> Node:
    return open_store(schema_path, **dst_kwargs)


def _copy_tree(src_node: Node, dst_node: Node):
    ds_src = src_node.dataset
    dst_node.update_from_ds(ds_src)

    for name, src_child in src_node.items():
        if name not in dst_node.children:
            dst_node.logger.warning(
                f"Destination schema has no node '{dst_node.group_path}/{name}', skipping."
            )
            continue

        _copy_tree(src_child, dst_node.children[name])


def cmd_cp(args, src_kwargs: dict, dst_kwargs: dict) -> int:
    src_root = _open_source_root(Path(args.src_schema), src_kwargs)
    dst_root = _open_destination_root(Path(args.dst_schema), dst_kwargs)
    _copy_tree(src_root, dst_root)
    return 0


def cmd_list(args) -> int:
    schema = zf.schema.deserialize(Path(args.schema_path[0]))
    schema_attrs = schema.ds.ATTRS
    store_name = schema_attrs['STORE_URL']
    root = zf.open_store(schema)



    print(f"store: {store_name}")
    for full_path, node in _iter_matching_nodes(
        node=root,
        store_name=store_name,
        path_glob=args.path_glob,
    ):
        print(_format_group_line(full_path=full_path, node=node))
        if args.print_dataset:
            lines = dataset_summary(node=node)
            print(lines)

    return 0


def arg_parser():
    parser = argparse.ArgumentParser(prog="zf", description="Zarr-fuse command line tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cp_parser = subparsers.add_parser(
        "cp",
        help="Copy all datasets from a source store into a destination store using their schemas.",
    )
    cp_parser.add_argument("src_schema", help="Path to source schema YAML file")
    cp_parser.add_argument("dst_schema", help="Path to destination schema YAML file")

    list_parser = subparsers.add_parser(
        "list",
        help="Inspect a zarr store and print its tree.",
    )
    list_parser.add_argument(
        "schema_path",
        nargs=1,        
        help='Schema path.',
    )
    list_parser.add_argument(
        "path_glob",
        nargs="?",
        default="*",
        help='Optional full-path glob like "profiles.zarr/Uhelna/*". Default is "*".',
    )
    list_parser.add_argument(
        "-p",
        "--print-dataset",
        action="store_true",
        help="Print full xarray dataset repr for matched nodes.",
    )
    return parser


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    load_dotenv()
    load_dotenv('.secrets_env')

    src_kwargs, remaining = peeloff_dot_args(argv, "--src")
    dst_kwargs, remaining = peeloff_dot_args(remaining, "--dst")
    #store_kwargs, remaining = peeloff_dot_args(remaining, "--store")

    parser = arg_parser()
    args = parser.parse_args(remaining)

    if args.command == "cp":
        return cmd_cp(args, src_kwargs, dst_kwargs)
    if args.command == "list":
        return cmd_list(args)

    parser.error("No command given.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
