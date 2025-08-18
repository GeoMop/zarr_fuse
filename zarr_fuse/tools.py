import numpy as np

def adjust_grid(x:np.ndarray, step_range:np.array) -> np.ndarray:
    """
    Given a 1D array `x` (irregular grid), return a new 1D array
    whose consecutive differences all lie within [min_step, max_step]
    by dropping points that would create a step < min_step and
    inserting evenly-spaced points whenever a step > max_step.
    """
    min_step, max_step = step_range
    x = np.asarray(x, float)
    if x.ndim != 1:
        raise ValueError("`x` must be 1D")
    xs = np.unique(x)  # sort & remove duplicates

    out = [xs[0]]
    last = xs[0]
    for xi in xs[1:]:
        d = xi - last
        if d < min_step:
            continue
        if d > max_step:
            n = int(np.ceil(d / max_step))
            step = d / n
            for k in range(1, n):
                out.append(last + k * step)
        out.append(xi)
        last = xi
    return np.array(out)
