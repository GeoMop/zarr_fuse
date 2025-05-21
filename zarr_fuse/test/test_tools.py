import numpy as np
from zarr_fuse.tools import adjust_grid


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