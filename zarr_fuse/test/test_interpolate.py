import pytest
import numpy as np
import xarray as xr
import re
try:
    import matplotlib.pyplot as plt
except ImportError:
    pass

# ---- adjust this import to match your module path! ----
from zarr_fuse.interpolate import interpolate_ds, sort_by_coord, interpolate_coord, dflt_logger
from zarr_fuse.zarr_schema import Coord
#from ds_interpolate import sort_by_coord, interpolate_coord, interpolate_ds

# a tiny dummy schema object in lieu of Coord
class DummySchema:
    def __init__(self, *, sorted, step_limits=None, unit=None, step_unit=None):
        self.name = "test_coord"
        self.sorted = sorted
        self.step_limits = step_limits
        self.unit = unit
        self._step_unit = step_unit

    def step_unit(self):
        return self._step_unit

# -- low‐level tests for sort_by_coord & interpolate_coord --

def test_sort_by_coord_sorted():
    schema = DummySchema(sorted=True)
    old = np.array([0, 10, 20, 30])
    new_ref = np.array([15, 25, 30, 35, 40, 50])
    new = new_ref.copy()
    np.random.shuffle(new)
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    # should sort new ascending and find split where overlap ends

    sorted = new[idx_sort]
    steps = sorted[1:] - sorted[:-1]
    assert np.all(steps >=0)
    # overlap with old max=30 => split at new index of 30 (right side)
    assert idx_split == 3

    old = np.array([0, 10, 20, 30])
    new_ref = np.array([15, 25, 31, 35, 40, 50])
    new = new_ref.copy()
    np.random.shuffle(new)
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    # overlap with old max=30 => split at new index of 30 (right side)
    assert idx_split == 2


def test_sort_by_coord_unsorted():
    schema = DummySchema(sorted=False)
    # empty extension
    old = np.array([5, 15, 25])
    new = np.array([25, 5, 15])
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    # keys map 25->2,5->0,15->1 so sorting gives positions [1,2,0]
    assert np.all(old == new[idx_sort])
    # full overlap => split == len(old)
    assert idx_split == 3

    # non-empty extension, no overlap
    old = np.array([5, 15, 25])
    new = np.array([4, 30])
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    # keys map 25->2,5->0,15->1 so sorting gives positions [1,2,0]
    assert idx_split == 0

    # non-empty extension, full overlap
    old = np.array([5, 15, 25])
    new = np.array([25, 5, 15, 4, 30])
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    # keys map 25->2,5->0,15->1 so sorting gives positions [1,2,0]
    assert idx_split == len(old)
    assert np.all(old == new[idx_sort][:idx_split])

    # Test partial overlap
    old = np.array([5, 15, 25])
    new = np.array([25, 5, 6, 35])
    idx_sort, idx_split = sort_by_coord(new, old, schema, dflt_logger)
    assert idx_split == 2
    assert np.all([5, 25, 6, 35] == new[idx_sort])


def run_interp(old, new, step_limits, sort=True,  unit='', step_unit='hour'):
    schema = DummySchema(sorted=sort, step_limits=step_limits, unit=unit, step_unit=step_unit)

    new = np.array(new)
    old = np.array(old)
    idx_sorter = sort_by_coord(new, old, schema, dflt_logger)
    return interpolate_coord( new, old, idx_sorter, schema, dflt_logger)


def test_interpolate_coord_sorted():
    old = [0, 1, 2]
    # sorted, step_limits - no extension
    merged, split = run_interp(old, [1, 1.5, 2.1], step_limits=None)
    assert split == 2
    np.allclose(merged, [1, 2])

    new = [1, 1.5, 2.1, 3, 10]
    merged, split = run_interp(old, new, step_limits=None)
    # apendend coordinates ignored. Non-fatal error.
    assert np.allclose(merged, [1, 2])

    # sorted, step_limits - full extension
    merged, split = run_interp(old, new, step_limits=[])
    assert split == 2
    np.allclose(merged, [1, 2, 2.1, 3, 10])

    new = [1, 1.5, 2, 3, 10]
    merged, split = run_interp(old, new, step_limits=[])
    assert split == 2
    np.allclose(merged, [1, 2, 3, 10])

    # sorted, step_limits - unexact step limits
    new = [1, 1.5, 2, 3, 4, 7.5, 10]
    merged, split = run_interp(old, new,
                    step_limits=[72, 126, 'minutes']) # 1.2 h, 2.1 h
    assert split == 2
    np.allclose(merged, [1, 2, 4, 5.75, 7.5, 8.75,  10])

    new = [1, 1.5, 2, 3, 4, 7.5, 10]
    merged, split = run_interp(old, new,
                    step_limits=[150, 150, 'minutes']) # 2.5 h
    assert split == 2
    np.allclose(merged, [1, 2, 3.5 + 1/3.0, 5 + 2/3.0, 7.5,  10])



def test_interpolate_coord_unsorted():
    old = [0, 1, 2]
    # sorted, step_limits - no extension
    merged, split = run_interp(old, [0, 1, 2], sort=False, step_limits=None)
    assert split == 3
    np.allclose(merged, [0, 1, 2])

    with pytest.raises(AssertionError) as excinfo:
        # unable to interpolate
        merged, split = run_interp(old, [1, 1.5, 2.1], sort=False, step_limits=None)

    #with pytest.raises(AssertionError) as excinfo:
    ## only able to update all coords
    merged, split = run_interp(old, [1, 2], sort=False, step_limits=None)

    # sorted, step_limits - full extension, full overlap
    new = [0, 1, 2, 10, 3]
    merged, split = run_interp(old, new, sort=False, step_limits=[])
    assert split == 3
    np.allclose(merged, [0, 1, 2, 10, 3])

    new = [10, 3]   # no overlap
    merged, split = run_interp(old, new, sort=False, step_limits=[])
    assert split == 0
    np.allclose(merged, [10, 3])

    new = [0, 1, 1.5, 2, 3, 10]
    # unable to interpolate, add all new values
    merged, split = run_interp(old, new, sort=False, step_limits=[])
    assert split == 3
    np.allclose(merged, [0, 1, 2, 1.5, 10, 3])

    new = [1, 2, 10, 3]
    # unable to interpolate
    merged, split = run_interp(old, new, sort=False, step_limits=[1, 2])
    np.allclose(merged, [1, 2,  10, 3])




def check_preserve_old(existing_ds, update_ds, ds_int, splits):
    """
    Validate preservation of existing coords in the interpolated dataset:
    - 'x' (sorted): ensure preserved coords fall within the numeric overlap interval
      and are from the original existing coords.
    - 'p' (unsorted): ensure exact match of all existing coords in order.
    """
    # Extract split indices for each dimension
    split_map = dict(splits)

    # ---- Check sorted 'x' dimension ----
    split_x = split_map['x']
    old_x = existing_ds.coords['x'].values
    upd_x = update_ds.coords['x'].values
    preserved_x = ds_int.coords['x'].values[:split_x]

    # membership in original old coords
    assert np.all(np.isin(preserved_x, old_x)), (
        f"x: preserved values {preserved_x} not subset of old {old_x}"
    )

    # ---- Check unsorted 'p' dimension ----
    split_p = split_map['p']
    old_p = existing_ds.coords['p'].values
    preserved_p = ds_int.coords['p'].values[:split_p]
    assert np.array_equal(preserved_p, old_p), (
        f"p: preserved {preserved_p} != old {old_p}"
    )


def plot_heatmaps(existing_ds, update_ds, ds_int, merged_ds=None):
    """Draw heatmaps of existing, update, interpolated, and optional merged datasets
    in a single figure with a common colormap and a shared colorbar to the right."""
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    layers = [
        (existing_ds, "Existing Dataset"),
        (update_ds, "Update Dataset"),
        (ds_int, "Interpolated Dataset"),
    ]
    if merged_ds is not None:
        layers.append((merged_ds, "Merged Dataset"))

    n = len(layers)
    fig, axes = plt.subplots(1, n, figsize=(4 * n, 4))

    # global color range
    all_vals = np.concatenate([ds["data"].values.flatten() for ds, _ in layers])
    vmin, vmax = np.nanmin(all_vals), np.nanmax(all_vals)

    for ax, (ds, title) in zip(axes, layers):
        im = ax.imshow(ds["data"].values, aspect="auto", vmin=vmin, vmax=vmax)
        ax.set_title(title)
        # labels and ticks from coords
        x_dim, y_dim = 'p', 'x'
        ax.set_xlabel(x_dim)
        ax.set_ylabel(y_dim)
        x_vals = ds.coords[x_dim].values
        y_vals = ds.coords[y_dim].values
        ax.set_xticks(np.arange(len(x_vals)))
        ax.set_xticklabels(x_vals)
        ax.set_yticks(np.arange(len(y_vals)))
        ax.set_yticklabels(y_vals)

    # shared colorbar at right of last plot
    last_ax = axes[-1]
    divider = make_axes_locatable(last_ax)
    cax = divider.append_axes("right", size="5%", pad=0.1)
    fig.colorbar(im, cax=cax, orientation='vertical')
    plt.tight_layout()
    plt.show()

def ds_merge_mock(existing_ds, update_ds):
    """
    Mock function to simulate the merge of two datasets.
    This is a placeholder for the actual merge logic.
    """

    # 1) Build merged 'x' as sorted union
    merged_x = np.sort(
        np.unique(
            np.concatenate((existing_ds.coords['x'].values,
                            update_ds.coords['x'].values))
        )
    )

    # For simplicity, just combine the datasets
    # 1) Build the custom 'p' coord:
    old_p = existing_ds.coords['p'].values
    new_p = update_ds.coords['p'].values

    # take only those in new_p that aren't in old_p
    extra = [v for v in new_p if v not in old_p]

    # final ordering: all old labels, then the extras in ds_int_nan order
    merged_p = np.concatenate([old_p, extra])

    # 2) Reindex both onto this new 'p' (and onto ds_int_nan.x for the sorted dim):
    #    we use `reindex` with fill_value=np.nan so they line up
    existing_expanded = existing_ds.reindex({'p': merged_p, 'x': merged_x}, fill_value=np.nan)
    interpolated_expanded = update_ds.reindex({'p': merged_p, 'x': merged_x}, fill_value=np.nan)

    # 3) Now merge values: take interpolated where it’s not NaN, else fall back to existing
    merged_data = xr.where(~np.isnan(interpolated_expanded['data']),
                           interpolated_expanded['data'],
                           existing_expanded['data'])

    # 4) Rebuild a combined Dataset
    merged_ds = xr.Dataset(
        {'data': (('x', 'p'), merged_data.values)},
        coords={
            'x': interpolated_expanded.coords['x'].values,
            'p': merged_p
        }
    )
    return merged_ds

def test_interpolate_ds():
    # --- Build a clear 2D example ---
    # existing coords
    existing_x = np.array([  0,  1,  3.0])
    existing_p = np.array([10, 12.0])
    # update coords include all existing plus new ones (and unsorted for p)
    update_x = np.array([ 11, 1.5, 5, 10])             # sorted dim
    update_p = np.array([12, 11, 10, 13])               # unsorted dim

    # simple “sum” data so heatmaps show a gradient
    existing_ds = xr.Dataset(
        {"data": (("x", "p"), np.add.outer(existing_x, existing_p))},
        coords={"x": existing_x, "p": existing_p},
    )
    update_array = np.add.outer(update_x, update_p) + 10
    update_ds = xr.Dataset(
        {"data": (("x", "p"), update_array)},
        coords={"x": update_x,   "p": update_p},
    )

    schema = {
        "x": DummySchema(sorted=True,  step_limits=[], unit='h', step_unit='h'),
        "p": DummySchema(sorted=False, step_limits=[], unit='deg', step_unit='deg'),
    }

    # --- Run the interpolation under test ---
    ds_int, splits = interpolate_ds(update_ds, existing_ds, schema)

    # --- 1) check that each axis preserves the old coords up front ---
    check_preserve_old(existing_ds, update_ds, ds_int, splits)

    # --- 2) plot all three for manual/visual verification ---
    if 'plt' in globals():
        plot_heatmaps(existing_ds, update_ds, ds_int)

    # --- Introduce NaNs into update_ds and re-interpolate ---
    rng = np.random.default_rng(42)
    mask = rng.choice([True, False], size=update_ds['data'].shape, p=[0.3, 0.7])
    update_ds_nan = update_ds.copy()
    update_ds_nan['data'] = update_ds['data'].where(~mask)

    ds_int_nan, splits_nan = interpolate_ds(update_ds_nan, existing_ds, schema)
    # merge: take non-NaN from interpolated, else existing
    merged_ds = ds_merge_mock(existing_ds, ds_int_nan)

    # plot all four layers including merged result
    if 'plt' in globals():
        plot_heatmaps(existing_ds, update_ds_nan, ds_int_nan, merged_ds)

    # ensure coords merged correctly
    for d in ['x', 'p']:
        merged = merged_ds.coords[d].values
        ref = [*ds_int_nan.coords[d].values, *existing_ds.coords[d].values]
        assert set(merged) == set(ref)

    # ensure data merged correctly
    int_vals = ds_int_nan['data'].reindex_like(merged_ds).values
    exist_vals = existing_ds['data'].reindex_like(merged_ds).values
    merged_vals = merged_ds['data'].values

    int_nan_mask = np.isnan(int_vals)
    # merged is int_vals where it is not nan
    assert np.array_equal(merged_vals[~int_nan_mask], int_vals[~int_nan_mask])

    # merged defaults to exist_vals where int_vals are nan
    ex_nan_mask = np.isnan(exist_vals)
    valid_mask = int_nan_mask & ~ex_nan_mask
    nan_mask = int_nan_mask & ex_nan_mask
    assert np.array_equal(merged_vals[valid_mask], exist_vals[valid_mask])
    assert np.all(np.isnan(merged_vals[nan_mask]))


