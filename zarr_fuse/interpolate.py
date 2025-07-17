from typing import Tuple, List, Dict
import numpy as np
import xarray as xr
import attrs

from .zarr_schema import Coord
from . import units
from .tools import adjust_grid


@attrs.define(frozen=True)
class PartialOverlapError(ValueError):
    coord_name: str
    idx_split: int
    coord_len: int

    def __str__(self) -> str:
        # Build your message from the fields
        return (f"The updating coord {self.coord_name} has"
                f"overlap size [{self.idx_split}] < existing coord length [{self.coord_len}].")


def sort_by_single_coord(new_values:np.ndarray, old_values:np.ndarray, schema: Coord)\
    -> Tuple[np.ndarray, int]:
    """
    Return (idx_sort, idx_split)
    idx_sort : is the sorting index array for `new_values`
    ids_split: is the splitting index such that
      new_sorted[:idx_split][-1] is minimum value from new_value greater (-- or equal --) to max of old_values.

    For `sorted==True` the new_values are sorted in ascending order according to their values.
    For `sorted==False` the `new_values` in `old_values` comes first in their order in the `old_values`
     then other values from `new_values` are appended.
    """
    assert np.unique(new_values).size == new_values.size, f"New coordinates must be unique, got {new_values}"
    sorted = schema.sorted
    if sorted:
        idx_sort = np.argsort(new_values)
        new_sorted = new_values[idx_sort]
        if len(old_values) > 0:
            max_old = np.max(old_values)
            idx_split = np.searchsorted(new_sorted, max_old, side='left') + 1
        else:
            idx_split = 0
    else:
        pos_in_old = {v: i for i, v in enumerate(old_values)}
        keys = np.array([
            pos_in_old.get(v, len(old_values) + i)
            for i, v in enumerate(new_values)
        ])
        idx_sort = np.argsort(keys)
        idx_split = np.sum(np.array(keys) < len(old_values))


        # if idx_split > 0:
        #     # full overlap, verify that the values are the same
        #     update_overlap = new_values[idx_sort][:idx_split]
        #     assert np.all(update_overlap == old_values)

    # temporary assert the idx_split is for the value  STRICTLY GREATER.
    # if not (len(old_values) == 0 or new_values[idx_sort][idx_split] > old_values[-1]):
    #     assert False, f"Not a strict greater"

    return (idx_sort, idx_split)


def _interpolate_coord_old_part(new_old_part:np.ndarray, old_full:np.ndarray, schema: Coord) -> np.ndarray:
    """
    Get minimum part of the old coordinates covering the new coordinates that are overlapping the old coordinates.
    :param new_old_part: new coordinate values to interpolate
    :param old_full: full range of old coordinate values
    :return: Continuous block of old coordinates to update.
    """

    # We assume idx_split determining the new_old_part is index of a first new_value STRICTLY larger than old_values.
    old_values = old_full
    if len(old_values) == 0:
        return np.array([], dtype=new_old_part.dtype)
    if len(new_old_part) == 0:
        # no new values to update, return empty array
        return np.array([], dtype=old_values.dtype)

    # Guaranteed both new_old_part and old_full nonempty.
    if schema.sorted:
        old_part_min = new_old_part[0]
        old_range_min = np.searchsorted(old_values, old_part_min, side='left')
        old_part_max = new_old_part[-1]
        old_range_max = np.searchsorted(old_values, old_part_max, side='right')
        update_old_part = np.array(old_values[old_range_min:old_range_max])

        assert old_part_max <= np.max(old_values)
        # if old_part_max > np.max(old_values):
        #     extension_start -= 1  # include the first value out of old_values to the extension


    else:
        # Unsorted coords, no interpolation
        if schema.step_limits is None:
            # no extension allowed
            assert np.all(new_sorted == old_values)
            assert extension_start == len(new_sorted)
            update_old_part = new_old_part
        elif schema.step_limits == []:
            # default case, determine minimum continuous block of existing coords to update
            # + add all new coords
            # Block update could be prohibitively slow.
            # TODO: add warning for large updated blocks (more then 3x values to read then to
            # overwrite)
            if idx_split > 0:
                # has overlap -> determine its extend

                start = np.find(new_sorted[0])
                end = np.find(new_sorted[idx_split - 1])
                update_old_part =
            else:
                # no overlap
                update_old_part = np.array([])
            assert extension_start == 0 or np.all(new_sorted[:extension_start] == old_values)
        else:
            assert False, r"Interpolation not supported for non-sorted coordinates (step_limits={schema.step_limits}\n)"


def _interpolate_coord_new_part(new_append_part:np.ndarray, old_max:float, schema:Coord) -> np.ndarray:
    """
    Possibly modify the appended part of the coordinate values.
    :param new_append_part: new coordinate values to interpolate
    :param old_max: maximum value of the old coordinate values, for better interpolation.
    :param schema: schema dictionary
    :return: interpolated values
    """
    if schema.step_limits is None:
        # no extension allowed
        #assert np.all(new_append_part == old_max) # this is some probably wrong assert, new_
        assert len(new_append_part) == 0, f"New coordinates {new_append_part} must be empty when step_limits is None"
        return np.array([], dtype=new_append_part.dtype)
    elif schema.step_limits == []:
        # default case, add all new coords
        return new_append_part
    else:
        # Constrained coordinates step.
        # Construct adjusted coordinates grid.
        min_step, max_step, unit = schema.step_limits
        step_range = np.array([min_step, max_step])
        step_range = units.create_quantity(step_range, unit)
        coord_unit = schema.step_unit()
        step_range = step_range.to(coord_unit).magnitude

        if old_max < new_append_part[0]:
            extension_part = np.concatenate([
                np.array([old_max]),
                new_append_part])
        else:
            assert False, f"Old maximum {old_max} must be less than new append part first value {new_append_part}"
            extension_part = new_append_part

        return adjust_grid(extension_part, step_range)[1:]




def interpolate_coord(new_values:np.ndarray, old_values:np.ndarray,
                      idx_sorter:np.ndarray, schema: Coord) -> Tuple[np.ndarray, int]:
    """
    Interpolate new coordinates to existing coordinates.
    :param dim_name: name of the coordinate dimension
    :param new_coords: coordinate values in the updating dataset
    :param old_coords: coordinate values in the existing dataset
    :param schema: schema dictionary
    :return:
        merged_coord_values - np.array with coord values of the merged dataset block
        idx_split_idx - coord index within the merged coord block marking start of the
        new coord values:
        merged_coord_values[:idx_split] - all existing coord values
        merged_coord_values[idx_split:] - all new coord values
    1. asserts for non-sorted case
    2. replace part before split index by old values in same range
    3. for step_limit modify part after split index
    """

    # Phase 1.
    # determine:
    # `update_old_part` - part of old_values to update, must be continuous block
    # `extension_start` -

    idx_sort, idx_split = idx_sorter
    extension_start = idx_split
    new_sorted = new_values[idx_sort]


    if schema.sorted:
        assert np.all(new_sorted[:-1] <= new_sorted[1:])
        assert (
                idx_split == len(new_sorted)  # no extension
                or len(old_values) == 0  # no old values, no overlap, idx_split should be 0
                or new_sorted[idx_split] > np.max(old_values)  # idx_split is the first value greater than old values
            ), f"First extension value {new_sorted[idx_split]} <= max old values: {np.max(old_values)}"
    else:
        if schema.step_limits is None:
            # no extension allowed
            assert np.all(new_sorted == old_values)
            assert extension_start == len(new_sorted)


    update_old_part = _interpolate_coord_old_part(new_sorted[:idx_split], old_values)
    # Last of old values is used for interpolation only, so independent of the overlapping block
    last_old = old_values[-1] if len(old_values) > 0 else new_values[idx_split]
    update_new_part = _interpolate_coord_new_part(new_sorted[idx_split:], last_old, schema)

    idx_split = len(update_old_part)
    merged_coord_values = np.concatenate((update_old_part, update_new_part))
    return merged_coord_values, idx_split


def interpolate_ds(ds_update: xr.Dataset, ds_existing: xr.Dataset, schema:Dict[str, Coord]) \
        -> Tuple[xr.Dataset, Dict[str, int]]:
    """
    Interpolate ds_update to existing coords.
    :param ds_update:
    :param ds_existing:
    :return:
    1. sort the updating dataset along sorted coords
    2. for each coord determine overlap part of existing DS and correct extending part of the updating DS
       produces:  dict: dim_name -> new_coord_values, idx in produced coords where extension starts
    3. interpolate updating DS to new coords
    return: ds_intrepolated, dict: dim_name -> splitting idx
    """
    def get_vals(ds, d, default):
        try:
            return ds[d].values
        except KeyError:
            return default

    # Phase 1: sort pormutations for coordinatespdating dataset
    dim_idx_sort = {
        d: sort_by_single_coord(
                ds_update[d].values,
                get_vals(ds_existing, d, []),
                schema[d])
        for d in ds_update.dims
        if d in schema
    }

    # Phase 2: determine overlap and extension
    coords_new = [
        (d,
         interpolate_coord(
             ds_update[d].values,
             get_vals(ds_existing, d, []),
             dim_sort,
             schema[d])
        )
        for d, dim_sort in dim_idx_sort.items()
    ]
    interp_coords = {d: c for d, (c, idx) in coords_new if schema[d].sorted}
    split_indices = [(d, idx) for d, (c, idx) in coords_new]

    # Phase 3: interpolate ds_sorted to new coords
    dim_sorters = {d: sorter
                   for d, (sorter, _) in dim_idx_sort.items()}
    ds_sorted = ds_update.isel(**dim_sorters)

    # first attempt linear interpolation
    ds_linear = ds_sorted.interp(
        interp_coords,
        method='linear',
        assume_sorted=True
    )
    # then nearest-neighbor fallback to fill any NaNs
    ds_nearest = ds_sorted.interp(
        interp_coords,
        method='nearest',
        assume_sorted=True
    )
    # combine: use linear where available, else nearest
    #ds_interpolated = ds_linear.combine_first(ds_nearest)
    ds_interpolated = ds_linear
    # meaningful methods available for multidim data:
    # “nearest”, “linear”, “pchip” (Piecewise Cubic Hermite Interpolating Polynomial)
    #
    # other methods:
    # “slinear” equal to "linear" but uses order 1 spline machinery
    # "cubic" similar to "pchip" but dos not preserve monotnicity, could have overshoots
    # TODO: better user choice of interpolation method and fixing nans


    return ds_interpolated, split_indices
