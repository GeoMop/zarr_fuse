import logging
dflt_logger = logging.getLogger(__name__)
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


def sort_by_coord(new_values:np.ndarray, old_values:np.ndarray,
                  schema: Coord, log:logging.Logger)\
    -> Tuple[np.ndarray, int]:
    """
    Return (idx_sort, idx_split)
    idx_sort : is the sorting index array for `new_values`
    ids_split: is the splitting index such that
      new_sorted[:idx_split][-1] is minimum value from new_value greater or equal to max of old_values.

    For `sorted==True` the new_values are sorted in ascending order according to their values.
    For `sorted==False` the `new_values` in `old_values` comes first in their order in the `old_values`
     then other values from `new_values` are appended.
    """

    # Uniqueness should be tested when coordinate is formed in _coord_variable.
    # Both pivot_nd and dataset_from_np use that function to form coordinates.
    # We assert here just for code consistency.
    assert np.unique(new_values).size == new_values.size, f"Code consistency error. New coordinates must be unique, got {new_values}"

    sorted = schema.sorted
    if sorted:
        idx_sort = np.argsort(new_values)
        new_sorted = new_values[idx_sort]
        if len(old_values) > 0:
            max_old = np.max(old_values)
            idx_split = np.searchsorted(new_sorted, max_old, side='right')
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

    return (idx_sort, idx_split)


def interpolate_coord(new_values:np.ndarray, old_values:np.ndarray,
                      idx_sorter:np.ndarray,
                      schema: Coord, log:logging.Logger) -> Tuple[np.ndarray, int]:
    """
    Interpolate new coordinates to existing coordinates.
    :param dim_name: name of the coordinate dimension
    :param new_coords: coordinate values in the updating dataset
    :param old_coords: coordinate values in the existing dataset
    :param schema: schema dictionary
    :return:
    1. asserts for non-sorted case
    2. replace part before split index by old values in same range
    3. for step_limit modify part after split index
    """
    idx_sort, idx_split = idx_sorter
    new_sorted = new_values[idx_sort]
    if len(new_sorted) == 0:
        return np.array([]), 0

    # Process overlap part
    if schema.sorted:
        # Determine part of old_values that are covered by the range of new_values so
        # the range of old_values we can potentialy interpolate to.
        # new_values[0] <= old_values[i_min] and ald_values[i_max-1] <= new_values[-1]

        assert np.all(np.diff(new_sorted) >= 0)
        old_part_min = new_sorted[0]
        old_range_min = np.searchsorted(old_values, old_part_min, side='left')
        old_part_max = new_sorted[-1]
        old_range_max = np.searchsorted(old_values, old_part_max, side='right')
        update_old_part = np.array(old_values[old_range_min:old_range_max])

    else:
        if schema.step_limits is None:
            # no extension allowed
            assert len(new_sorted[idx_split:]) == 0
        elif schema.step_limits == []:
            # default case, add all new coords
            pass
        else:
            # Same as default case.
            log.error(f"Ignoring unsupported interpolation for non-sorted coordinates (step_limits={schema.step_limits}\n)")
        update_old_part = new_sorted[:idx_split]

    # Phase 2: determine extension

    new_append = new_sorted[idx_split:]
    last_old = old_values[-1] if len(old_values) > 0 else new_values[idx_split]
    if schema.step_limits is None:
        # no extension allowed
        if len(new_append) > 1:
            # Non-fatal error.
            log.error(f"Dimension {schema.name}: extension not allowed (step_limits=None). "
                      f"Appended coordinates ignored: {new_append[1:]}.")
        # one value in extension is allowed, but used only to interpolate
        update_new_part = np.array([], dtype=new_append.dtype)
    elif schema.step_limits == [] or not schema.sorted:
        # deafult case, add all new coords
        update_new_part = new_append
    else:
        # Constrained coordinates step.
        # Construct adjusted coordinates grid.
        step_range = schema.step_limits
        step_range = np.array([step_range.start, step_range.end])
        step_range = units.create_quantity(step_range, step_range.unit)
        coord_unit = schema.step_unit()
        step_range = step_range.to(coord_unit).magnitude

        if last_old < new_append[0]:
            extension_part = np.concatenate(
                [np.array([last_old]),new_append]
            )
        else:
            assert False, f"Old maximum {last_old} must be less than new append part first value {new_append}"
            extension_part = new_append

        update_new_part = adjust_grid(extension_part, step_range)[1:]

    idx_split = len(update_old_part)
    merged_coord_values = np.concatenate((update_old_part, update_new_part))
    return merged_coord_values, idx_split


def interpolate_ds(ds_update: xr.Dataset, ds_existing: xr.Dataset,
                   schema:Dict[str, Coord], log: logging.Logger = dflt_logger) \
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

    # Phase 1: sort pormutations for coordinatespdating dataset
    dim_idx_sort = {
        d: sort_by_coord(
            ds_update[d].values,
            ds_existing[d].values,
            schema[d], log)
        for d in ds_update.dims
    }

    # Phase 2: determine overlap and extension
    coords_new = [
        (d,
         interpolate_coord(
             ds_update[d].values,
             ds_existing[d].values,
             dim_idx_sort[d],
             schema[d], log)
        )
        for d in ds_update.dims
    ]
    interp_coords = {d: c for d, (c, idx) in coords_new if schema[d].sorted and len(c) > 0}
    split_indices = [(d, idx) for d, (c, idx) in coords_new]

    # Phase 3: interpolate ds_sorted to new coords
    dim_sorters = {d: sorter for d, (sorter, _) in dim_idx_sort.items()}
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
