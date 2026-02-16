import numpy as np
import xarray as xr

def print_ar(name, ar):
    print(f"{name}:")
    print(ar)
    print()

def test_combine_basic():
    # ar0, with a NaN "hole"
    ar0 = xr.DataArray(
        np.array([[0, 0], [0, 0]], dtype=float),
        dims=("x", "y"),
        coords={"x": ["a", "b"], "y": [-1, 0]},
    )
    # ar1: overlapping and extending coords
    ar1 = xr.DataArray(
        np.array([[1, 1], [1, 1]], dtype=float),
        dims=("x", "y"),
        coords={"x": ["b", "c"], "y": [0, 1]},
    )

    # combine_first as in the docs: ar0 has priority where not NaN
    ar2 = ar1.combine_first(ar0)

    # expected result (from the docs example)
    expected = xr.DataArray(
        np.array(
            [
                [0.0, 0.0, np.nan],
                [0.0, 1.0, 1.0],
                [np.nan, 1.0, 1.0],
            ]
        ),
        dims=("x", "y"),
        coords={"x": ["a", "b", "c"], "y": [-1, 0, 1]},
    )
    print_ar("ar2", ar2)

    xr.testing.assert_identical(ar2, expected)

def coords_len(coords: dict):
    return [len(v) for v in coords.values()]

def make_xr(coords, fill):
    return xr.DataArray(
        np.full(coords_len(coords), fill, dtype=float),
        dims=tuple(coords.keys()),
        coords=coords
    )

def test_combine_sparse():
    # ar0, with a NaN "hole"
    coords_0 = dict(
        x=["a", "b", "c"],
        y=[-1, 0, 1, 2])
    ar0 = make_xr(coords_0, 0.0)

    # ar1: overlapping and extending coords
    coords_1 = dict(
        x=["c", "d"],
        y=[-1, 2, 3])

    ar1 = make_xr(coords_1, 1.0)

    # combine_first as in the docs: ar0 has priority where not NaN
    ar2 = ar1.combine_first(ar0)

    # expected result (from the docs example)
    coords = dict(
        x=["a", "b", "c", "d"],
        y=[-1, 0, 1, 2, 3])

    expected = make_xr(coords, np.nan)
    expected.loc[coords_0] = 0.0
    expected.loc[coords_1] = 1.0
    print_ar("ar2", ar2)

    xr.testing.assert_identical(ar2, expected)


if __name__ == "__main__":
    test_combine_basic()
    test_combine_sparse()