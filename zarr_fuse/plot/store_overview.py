from typing import Dict, Any, List, Optional, Union
import attrs

import zarr_fuse
import xarray as xr

@attrs.define
class DSOverview:
    """
    DataOverview class to store information about the data.
    """
    coordinates: xr.Coordinates
    cumul_coordinates: xr.Coordinates



OverviewDict = Dict[str, DSOverview]
def build_overview(node: zarr_fuse.Node) -> OverviewDict:
    """
    Recursively build an overview of the subtree starting from the given node.

    Args:
        node (Node): The current node in the storage tree.

    Returns:
        dict: A flattened structure with nodes indexed by their group_path.
    """
    # Extract coordinates from the current node's dataset
    node_coords = node.dataset.coords
    cumul_coordinates = [node_coords.copy(deep=True).to_dataset()]
    flattened = {}
    # Process child nodes recursively, collect:
    # -  flattend dict
    # -  datasets for merging to cumul_coordinates
    for child_name, child_node in node.items():
        child_overview_dict = build_overview(child_node)
        flattened.update(child_overview_dict)
        child_overview = flattened[child_node.group_path]
        cumul_coordinates.append(child_overview.cumul_coordinates.to_dataset())
    cumul_coordinates = xr.merge(cumul_coordinates).coords
    node_overview = DSOverview(node_coords, cumul_coordinates)
    flattened[node.group_path] = node_overview
    return flattened


def get_key_for_value(dct, value):
    """
    Return key(s) in `dct` that map to `value`.

    Parameters:
    - dct         : dict — the dictionary to search
    - value       : any — the value to look up
    - first_only  : bool — if True, return the first matching key (or None if not found);
                          if False, return a list of all matching keys (empty list if none)

    Returns:
    - single key (when first_only=True) or list of keys (when first_only=False)
    """
    for k, v in dct.items():
         if v == value:
             return k
    return None
