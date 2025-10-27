import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union
import polars as pl
import re

JSONLike = Union[dict, list]

_PLACEHOLDER_RE = re.compile(r"^\{([a-zA-Z_]\w*)\}$")

def _load_json(json_obj_or_path: Union[str, Path, JSONLike]) -> JSONLike:
    if isinstance(json_obj_or_path, (str, Path)):
        with open(json_obj_or_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return json_obj_or_path

def _split_path(path: str) -> Iterable[str]:
    """
    Split an absolute path like '/a/b/{x}/c' into ['a', 'b', '{x}', 'c'].
    """
    if not path.startswith("/"):
        raise ValueError(f"Paths must be absolute from root and start with '/': {path!r}")
    # filter out possible empty segments from '//' and leading '/'
    return [seg for seg in path.split("/") if seg]

def _descend(node: Any, key: str) -> Any:
    """
    Descend one step into node using key which may be an int (for lists) or str (for dicts).
    """
    if isinstance(node, dict):
        return node.get(key)
    if isinstance(node, list):
        try:
            idx = int(key)
        except (TypeError, ValueError):
            return None
        return node[idx] if 0 <= idx < len(node) else None
    return None

def _resolve_path(root: JSONLike, path_template: str, ctx: Dict[str, Any]) -> Any:
    """
    Resolve an absolute path template by substituting {placeholders} from ctx,
    then walking the JSON. Returns None if any step is missing.
    """
    # Substitute placeholders (e.g., '{idx}' -> '0', '{key2}' -> 'foo')
    def sub(match: re.Match) -> str:
        name = match.group(1)
        val = ctx.get(name)
        return str(val) if val is not None else ""
    concrete = re.sub(r"\{([a-zA-Z_]\w*)\}", sub, path_template)
    node = root
    for seg in _split_path(concrete):
        node = _descend(node, seg)
        if node is None:
            return None
    return node

def _is_literal_placeholder(s: str) -> Union[str, None]:
    """
    If s is exactly a single placeholder like '{key2}', return the name 'key2'; else None.
    """
    m = _PLACEHOLDER_RE.match(s)
    return m.group(1) if m else None

def _match_pattern(root: JSONLike, pattern: str) -> Iterable[Tuple[Any, Dict[str, Any]]]:
    """
    Yield (matched_node, ctx) for every JSON node that matches the pattern.
    pattern example: '/the-key/{idx}/{key2}'
    ctx will contain values for 'idx' and 'key2'.
    """
    segs = list(_split_path(pattern))

    def dfs(node: Any, i: int, ctx: Dict[str, Any]):
        if i == len(segs):
            yield (node, ctx)
            return

        seg = segs[i]
        m = re.fullmatch(r"\{([a-zA-Z_]\w*)\}", seg)
        if not m:
            # literal segment
            next_node = _descend(node, seg)
            if next_node is not None:
                yield from dfs(next_node, i + 1, ctx)
            return

        # placeholder segment
        var = m.group(1)
        if isinstance(node, list):
            for idx, child in enumerate(node):
                new_ctx = dict(ctx)
                new_ctx[var] = idx
                yield from dfs(child, i + 1, new_ctx)
        elif isinstance(node, dict):
            for k, child in node.items():
                new_ctx = dict(ctx)
                new_ctx[var] = k
                yield from dfs(child, i + 1, new_ctx)
        # if node is neither list nor dict: no matches

    yield from dfs(root, 0, {})

def extract_polars_table_from_json(
    json_obj_or_path: Union[str, Path, JSONLike],
    pattern: str,
    columns: Dict[str, str],
) -> pl.DataFrame:
    """
    Scan JSON for all matches of `pattern` and build a Polars DataFrame with columns defined by `columns`.

    Parameters
    ----------
    json_obj_or_path : str | Path | dict | list
        Path to a JSON file or an already-loaded JSON object (dict/list).
    pattern : str
        Absolute JSON path with placeholders, e.g. '/the-key/{idx}/{key2}'.
        Placeholders expand over list indices or dict keys.
    columns : dict[str, str]
        Column mapping. Values can be:
          - absolute path templates (must start with '/'), e.g. '/the-key/{idx}/{key2}/t'
          - a *literal* placeholder, e.g. '{key2}', to inject the placeholder's value directly.

    Returns
    -------
    polars.DataFrame
    """
    data = _load_json(json_obj_or_path)
    rows = []

    for matched_node, ctx in _match_pattern(data, pattern):
        row = {}
        for col_name, spec in columns.items():
            literal_var = _is_literal_placeholder(spec)
            if literal_var:
                row[col_name] = ctx.get(literal_var)
            else:
                if not spec.startswith("/"):
                    raise ValueError(
                        f"Column spec for {col_name!r} must be an absolute path or a single placeholder: {spec!r}"
                    )
                row[col_name] = _resolve_path(data, spec, ctx)
        rows.append(row)

    # Build DataFrame; if there are no rows, create an empty DF with the requested columns
    if rows:
        return pl.from_dicts(rows)
    else:
        # default to pl.String for empty DF to keep columns visible; adjust if you prefer pl.Null
        return pl.DataFrame({k: pl.Series([], dtype=pl.String) for k in columns.keys()})
