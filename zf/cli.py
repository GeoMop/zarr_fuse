from __future__ import annotations

import argparse
import fnmatch
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from dotenv import load_dotenv

import xarray as xr
import zarr
import zarr_fuse as zf
from zarr_fuse.zarr_storage import _zarr_fuse_options, _zarr_store_open



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


@dataclass
class DimConflict:
    """Arrays of one group that disagree on the length of a named dimension."""

    group: zarr.Group
    dim: str
    target: int
    short: List[Tuple[str, int]]
    long: List[Tuple[str, int]]


def _dimension_names(array: zarr.Array) -> Tuple[str | None, ...]:
    # zarr v3 keeps the names in the metadata, xarray writes them as an attribute in v2.
    names = getattr(array.metadata, "dimension_names", None) or array.attrs.get("_ARRAY_DIMENSIONS")
    return tuple(names or ())


def find_dim_conflicts(group: zarr.Group) -> List[DimConflict]:
    """
    Find the dimensions whose length differs between the arrays of a group, in the
    whole group tree. The target length is the length of the coordinate array named
    after the dimension, or the maximum length when there is no such array.
    """
    lengths: Dict[str, Dict[str, int]] = {}
    for name, array in group.arrays():
        for axis, dim in enumerate(_dimension_names(array)):
            if dim is not None:
                lengths.setdefault(dim, {})[name] = array.shape[axis]

    conflicts = []
    for dim, by_array in sorted(lengths.items()):
        if len(set(by_array.values())) < 2:
            continue
        target = by_array.get(dim, max(by_array.values()))
        conflicts.append(DimConflict(
            group=group,
            dim=dim,
            target=target,
            short=sorted((name, n) for name, n in by_array.items() if n < target),
            long=sorted((name, n) for name, n in by_array.items() if n > target),
        ))

    for _, child in sorted(group.groups()):
        conflicts.extend(find_dim_conflicts(child))
    return conflicts


def extend_array(array: zarr.Array, dim: str, length: int) -> None:
    """Grow `array` along `dim` to `length` and fill the new part with the array fill value."""
    axis = _dimension_names(array).index(dim)
    old_length = array.shape[axis]
    shape = list(array.shape)
    shape[axis] = length
    array.resize(tuple(shape))
    # The grown region may lie in an existing edge chunk; write it explicitly
    # instead of relying on what that chunk holds past the old shape.
    region = [slice(None)] * array.ndim
    region[axis] = slice(old_length, length)
    array[tuple(region)] = array.fill_value


def cmd_fix_dims(args, store_kwargs: dict) -> int:
    """
    Report (and with --apply repair) arrays left shorter than their dimension,
    e.g. by a write interrupted between the arrays of one dataset. Arrays longer
    than the target are only reported, shrinking them would drop data.
    Return 1 when some conflict is left unresolved.
    """
    options = _zarr_fuse_options(None, **store_kwargs)
    # Explicit --store.* options win over ZF_* environment variables, so a repair
    # never silently lands in the store some .env points to.
    options.update(store_kwargs)
    if not options.get("STORE_URL"):
        raise SystemExit("fix-dims requires --store.STORE_URL=<store-url>")
    root = zarr.open_group(_zarr_store_open(options), mode="r+" if args.apply else "r")

    conflicts = find_dim_conflicts(root)
    for conflict in conflicts:
        print(f"{conflict.group.path or '/'} dim={conflict.dim} target={conflict.target}")
        for name, length in conflict.short:
            print(f"  extend {name}: {length} -> {conflict.target}")
            if args.apply:
                extend_array(conflict.group[name], conflict.dim, conflict.target)
        for name, length in conflict.long:
            print(f"  skip {name}: {length} > {conflict.target}, shrinking would drop data")

    if not conflicts:
        print("no dimension conflicts")
        return 0
    if not args.apply:
        print("dry run, pass --apply to extend the short arrays")
        return 1
    return 1 if any(conflict.long for conflict in conflicts) else 0


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

    fix_dims_parser = subparsers.add_parser(
        "fix-dims",
        help="Find arrays shorter than their dimension and extend them (store from --store.STORE_URL=...).",
    )
    fix_dims_parser.add_argument(
        "--apply",
        action="store_true",
        help="Extend the short arrays with the fill value. Without it only report.",
    )
    return parser


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    load_dotenv()
    load_dotenv('.secrets_env')

    src_kwargs, remaining = peeloff_dot_args(argv, "--src")
    dst_kwargs, remaining = peeloff_dot_args(remaining, "--dst")
    store_kwargs, remaining = peeloff_dot_args(remaining, "--store")

    parser = arg_parser()
    args = parser.parse_args(remaining)

    if args.command == "cp":
        return cmd_cp(args, src_kwargs, dst_kwargs)
    if args.command == "list":
        return cmd_list(args)
    if args.command == "fix-dims":
        return cmd_fix_dims(args, store_kwargs)

    parser.error("No command given.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
