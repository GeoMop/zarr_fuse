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
        while True:
            d = xi - last
            if d < min_step:
                break
            if d <= max_step:
                out.append(xi)
                last = xi
                break

            # Choose one step that leaves a remainder no greater than the
            # maximum step. Datetime division may round this step down to the
            # coordinate resolution, so enforce the minimum before advancing.
            n = int(np.ceil(d / max_step))
            step = d / n
            if step < min_step:
                step = min_step
            out.append(last + step)
            last = out[-1]
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
