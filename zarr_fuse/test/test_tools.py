import numpy as np
import copy
from zarr_fuse.tools import adjust_grid, recursive_update


def plot_adjust_grid_transitions(grids):
    """
    Apply adjust_grid in two sequential transitions:
      1) min_step from 0 → a (3 values), max_step fixed at b
      2) max_step from b → a (3 values), min_step fixed at a
    for each of the 3×3 = 9 combinations, then plot the resulting grids.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    fig, ax = plt.subplots()

    case_idx = 0
    for min1, max2, x in grids:
            y_vals = np.full_like(x, case_idx, dtype=float)
            ax.plot(x, y_vals, marker='o', linestyle='-',
                    label=f"min1={min1:.2f}, max2={max2:.2f}")
            case_idx += 1

    ax.set_xlabel('Grid Values')
    ax.set_ylabel('Case Index')
    ax.set_title('Grids from Two-Stage adjust_grid Transitions')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

def check_grid(a, b, grid):
    steps = grid[1:] - grid[:-1]
    assert np.all(steps <= b)
    assert np.all(steps >= a/2)
    return a, b, grid

def test_adjust_grid():
    # Example usage
    x_original = np.array([0, 0.3, 1.0, 2.5, 5.0])
    min_step = 0.8
    max_step = 2.5

    first_mins = np.linspace(0, min_step, 5)
    second_maxs = np.linspace(max_step, min_step, 5)
    grids = [
        check_grid(min1, max2,
            adjust_grid(x_original, (min1, max2))
        )
        for min1 in first_mins
        for max2 in second_maxs
    ]
    try:
        import matplotlib.pyplot as plt
        plot_adjust_grid_transitions(grids)
    except ImportError:
        pass



def test_recursive_update():
    # --- Basic merge ---
    a = {"x": 1, "y": {"a": 10, "b": 20}, "z": {"nested": {"k": "old"}}}
    b = {"y": {"b": 99, "c": 30}, "z": {"nested": {"new": "val"}}, "w": 42}

    out = recursive_update(a, b)
    assert a["y"] == {"a": 10, "b": 99, "c": 30}
    assert a["z"] == {"nested": {"k": "old", "new": "val"}}
    assert a["w"] == 42
    assert a["x"] == 1
    assert out is a

    # --- Overwrite when types differ ---
    a = {"k": {"sub": 1}, "m": 5}
    b = {"k": 7, "m": {"x": 1}}
    recursive_update(a, b)
    assert a["k"] == 7
    assert a["m"] == {"x": 1}

    # --- In-place behavior ---
    a = {"outer": {"inner": {"v": 1}}}
    b = {"outer": {"inner": {"w": 2}}}
    inner_before = a["outer"]["inner"]
    recursive_update(a, b)
    assert a["outer"]["inner"] is inner_before
    assert a["outer"]["inner"] == {"v": 1, "w": 2}

    # --- Empty updates ---
    a = {"x": 1, "y": {"z": 2}}
    a_before = copy.deepcopy(a)
    recursive_update(a, {})
    assert a == a_before
    d = {}
    recursive_update(d, {"n": {"m": 3}})
    assert d == {"n": {"m": 3}}

    # --- Deep merge ---
    a = {"a": {"b": {"c": 1, "d": 2}}}
    b = {"a": {"b": {"d": 999, "e": 3}}}
    recursive_update(a, b)
    assert a == {"a": {"b": {"c": 1, "d": 999, "e": 3}}}
