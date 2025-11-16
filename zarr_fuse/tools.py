import numpy as np

from functools import wraps
import logging
import time

def adjust_grid(x:np.ndarray, step_range:np.array) -> np.ndarray:
    """
    Given a 1D array `x` (irregular grid), return a new 1D array
    whose consecutive differences all lie within [min_step, max_step]
    by dropping points that would create a step < min_step and
    inserting evenly-spaced points whenever a step > max_step.
    """
    min_step, max_step = step_range
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


def recursive_update(d, u):
    """
    Recursively update dictionary `d` with values from dictionary `u`.

    If both d[k] and u[k] are dicts, merge them recursively.
    Otherwise, overwrite d[k] with u[k].
    """
    for k, v in u.items():
        if isinstance(v, dict) and isinstance(d.get(k), dict):
            recursive_update(d[k], v)
        else:
            d[k] = v
    return d



__report_indent_level = 0

def report(fn):
    @wraps(fn)
    def do_report(*args, **kwargs):
        global __report_indent_level
        __report_indent_level += 1
        init_time = time.perf_counter()
        result = fn(*args, **kwargs)
        duration = time.perf_counter() - init_time
        __report_indent_level -= 1
        indent = (__report_indent_level * 2) * " "
        logging.info(f"{indent}DONE {fn.__module__}.{fn.__name__} @ {duration}")
        return result
    return do_report