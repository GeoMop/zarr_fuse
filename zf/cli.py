# zarr_fuse/cli.py
from typing import *
import sys
import argparse
from pathlib import Path


from zarr_fuse import zarr_schema, Node, open_store
#, _zarr_fuse_options, _zarr_store_open)  # adjust module name if needed


def peeloff_dot_args(argv: List[str], prefix: str) -> Tuple[Dict[str, str], List[str]]:
    """
    Peel off `argv` items in form
        <prefix>.<KEY>=<VALUE>

    Return:
    kwargs : dict
        Mapping KEY -> VALUE (strings).
    remaining : list[str]
        The argv list without the peeled-off options.

    Example:
        prefix="--src."
        argument: "--src.STORE_URL=s3://bucket/path"
    """
    kwargs = {}
    remaining = []
    prefix = f"{prefix}."
    plen = len(prefix)
    for arg in argv:
        if arg.startswith(prefix):
            key_val = arg[plen:]
            kv_pair = key_val.split("=", 1)
            try:
                key, value = kv_pair
            except ValueError:
                raise SystemExit(
                    f"Invalid option '{arg}'. Expected format {prefix}.<KEY>=<VALUE>."
                )
            kwargs[key] = value
        else:
            remaining.append(arg)

    return kwargs, remaining

def _open_source_root(schema_path: Path, src_kwargs: dict) -> Node:
    """
    Open the source store using only the root-level ATTRS of the given schema file.

    We deliberately do *not* pass the NodeSchema into `open_store`, so that the
    existing store layout/schema is taken from the zarr metadata itself.
    """
    src_schema = zarr_schema.deserialize(schema_path)

    # Build options purely from ATTRS + kwargs + env (via your helper)
    options = _zarr_fuse_options(src_schema, **src_kwargs)
    store = _zarr_store_open(options)

    # Use storage-derived structure (no new_schema passed)
    root = Node.read_store(store)
    return root


def _open_destination_root(schema_path: Path, dst_kwargs: dict) -> Node:
    """
    Open/create the destination store according to the provided schema (new metadata),
    plus any CLI kwargs/environment variables.
    """
    # `open_store` does exactly what we want for the destination:
    # - merges schema attrs + kwargs + env
    # - initializes empty datasets according to schema
    return open_store(schema_path, **dst_kwargs)


def _copy_tree(src_node: Node, dst_node: Node):
    """
    Recursively copy datasets from src_node's tree into dst_node's tree.

    For each node where a matching destination node exists:
      - copy the dataset via `update_from_ds`.
      - recurse into children.
    Nodes present in the source but missing in the destination are skipped
    with a warning.
    """
    # Copy this node's dataset
    ds_src = src_node.dataset
    dst_node.update_from_ds(ds_src)

    # Copy children
    for name, src_child in src_node.items():
        if name not in dst_node.children:
            dst_node.logger.warning(
                f"Destination schema has no node '{dst_node.group_path}/{name}', skipping."
            )
            continue

        dst_child = dst_node.children[name]
        _copy_tree(src_child, dst_child)


def cmd_cp(args, src_kwargs: dict, dst_kwargs: dict) -> int:
    src_schema_path = Path(args.src_schema)
    dst_schema_path = Path(args.dst_schema)

    # Open source and destination roots
    src_root = _open_source_root(src_schema_path, src_kwargs)
    dst_root = _open_destination_root(dst_schema_path, dst_kwargs)

    # Perform recursive copy
    _copy_tree(src_root, dst_root)
    return 0


def arg_parser():
    parser = argparse.ArgumentParser(prog="zf", description="Zarr-fuse command line tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # cp command
    cp_parser = subparsers.add_parser(
        "cp",
        help="Copy all datasets from a source store into a destination store "
             "using their schemas."
    )
    cp_parser.add_argument("src_schema", help="Path to source schema YAML file")
    cp_parser.add_argument("dst_schema", help="Path to destination schema YAML file")
    return parser


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    # First peel off --src.* and --dst.* into kwargs
    src_kwargs, remaining = peeloff_dot_args(argv, '--src')
    dst_kwargs, reamining = peeloff_dot_args(remaining, '--dst')


    parser = arg_parser()
    args = parser.parse_args(remaining)

    if args.command == "cp":
        return cmd_cp(args, src_kwargs, dst_kwargs)

    # Should never get here because of required=True
    parser.error("No command given.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
