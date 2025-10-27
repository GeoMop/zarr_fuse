"""
Pattern matching JSON extractor.

- the JSON is searched for all matches of a path pattern like:
   '/the-key/{idx}/1/{key2}'

   The pattern is a list of segments separated by single slash.
   A segment is a string that may contain a placeholder in form '{<PLACEHOLDER_NAME>}'.
   To put characters '/', '{', '}' as literals double them.
   Segment is interpreted as a key in a dict or int index of a list
   depending on the JSON content.

-  For each pattern match, one row in the data frame will be created with columns given by a columns dict, e.g.

    { 'time': '/the-key/{idx}/{key2}/t',
      'group': '{key2}',
      'value': '/the-key/{idx}/{key2}/v'
      }

    Each key is a single column, value is given by the absolute path that could contain pattern placeholders
    or the placeholder itself.
    Any value transformation (e.g. an index shift by 1) should be done by a subsequent workflow task.
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union

import attrs
import polars as pl
import re

JSONLike = Union[dict, list]

_PLACEHOLDER_INNER_RE = re.compile(r"\{([a-zA-Z_]\w*)\}")

_SENT = "\x00"    # sentinel for double slash
_SENT_L = "\x01"  # sentinels for doubled braces
_SENT_R = "\x02"

def _load_json(json_obj_or_path: Union[str, Path, JSONLike]) -> JSONLike:
    if isinstance(json_obj_or_path, (str, Path)):
        with open(json_obj_or_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return json_obj_or_path


def _split_path(path: str) -> Iterable[str]:
    """
    Split an absolute path like '/a/b/{x}/c' into ['a', 'b', '{x}', 'c'].

    Rules:
      - '//' encodes a literal '/' inside a segment (handled via a sentinel).
      - '{{' and '}}' are left untouched here.
      - Leading/trailing/repeated slashes are tolerated; empty segments are dropped.
    """
    tmp = path.replace("//", _SENT)
    parts = tmp.split("/")  # may include leading '' from the root slash
    # recover escaped "/"
    parts =  [p.replace(_SENT, "/") for p in parts]
    if parts[0] != "":
        raise ValueError(f"JSON path pattern has to be absolute: {path}")
    return parts[1:]

def _compile_segment(seg_raw: str):
    """
    Turn a segment like 'run{run_idx}_{tag}' into a fullmatch regex and list of var names.
    Doubled braces '{{' '}}' are literal braces; they are protected via sentinels.
    Returns (regex_or_None, lits_or_str, var_names)

    - If there are NO placeholders, returns (None, literal_text, [])
    - If there ARE placeholders, returns (compiled_regex, None, [names...])
    """
    # Protect doubled braces so they are not seen as placeholders
    tmp = seg_raw.replace("{{", _SENT_L).replace("}}", _SENT_R)

    parts = []
    var_names = []
    pos = 0
    any_placeholder = False

    for m in _PLACEHOLDER_INNER_RE.finditer(tmp):
        any_placeholder = True
        # literal chunk before the placeholder
        lit = tmp[pos:m.start()]
        if lit:
            lit = lit.replace(_SENT_L, "{").replace(_SENT_R, "}")
            parts.append(re.escape(lit))
        # placeholder group (non-greedy to disambiguate next literals)
        name = m.group(1)
        var_names.append(name)
        parts.append(f"(?P<{name}>.*?)")
        pos = m.end()

    # trailing literal
    lit = tmp[pos:]
    if lit:
        lit = lit.replace(_SENT_L, "{").replace(_SENT_R, "}")
        parts.append(re.escape(lit))

    if not any_placeholder:
        # pure literal segment — return it (with doubled braces decoded)
        literal = seg_raw.replace("{{", "{").replace("}}", "}")
        return None, literal, []

    pattern = "^" + "".join(parts) + "$"
    return re.compile(pattern), None, var_names


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


@attrs.define
class PathMatch:
    node: JSONLike
    placeholders: dict
    pattern: str

    def get_placeholder(self, key):
        try:
            return self.placeholders[key]
        except KeyError:
            raise ValueError(f"Placeholder {key} not defined by the path pattern {self.pattern}.")


def _resolve_path(root: JSONLike, path_template: str, path_match: PathMatch) -> Any:
    """
    Resolve an absolute path template by substituting {placeholders} from ctx,
    honoring doubled braces as literals.
    """

    # 1) protect doubled braces
    tmp = path_template.replace("{{", _SENT_L).replace("}}", _SENT_R)

    # 2) substitute {vars} on the protected string
    def sub(match: re.Match) -> str:
        name = match.group(1)
        val = path_match.get_placeholder(name)
        return str(val)
    tmp = re.sub(r"\{([a-zA-Z_]\w*)\}", sub, tmp)

    # 3) restore doubled braces
    concrete = tmp.replace(_SENT_L, "{").replace(_SENT_R, "}")

    # 4) walk
    node = root
    for seg in _split_path(concrete):
        node = _descend(node, seg)
        if node is None:
            return None
    return node



# def _is_literal_placeholder(s: str) -> Union[str, None]:
#     """
#     If s is exactly a single placeholder like '{key2}', return the name 'key2'; else None.
#     """
#     m = _PLACEHOLDER_RE.match(s)
#     return m.group(1) if m else None

def _match_pattern(root: JSONLike, pattern: str) -> Iterable[PathMatch]:
    """
    Yield (matched_node, ctx) for every JSON node that matches the pattern.
    Now supports placeholders anywhere inside a segment, e.g. '/run{run_idx}_{tag}'.
    """
    segs = list(_split_path(pattern))
    # precompile each segment
    compiled = [ _compile_segment(seg) for seg in segs ]

    def dfs(node: Any, i: int, ctx: Dict[str, Any]):
        if i == len(compiled):
            yield PathMatch(node, ctx, pattern)
            return

        seg_regex, seg_literal, _varnames = compiled[i]

        if seg_regex is None:
            # No placeholders in this segment: plain literal descend
            next_node = _descend(node, seg_literal)
            if next_node is not None:
                yield from dfs(next_node, i + 1, ctx)
            return

        # Segment has placeholders: iterate children and regex-match their keys
        if isinstance(node, dict):
            items = node.items()
        elif isinstance(node, list):
            # use stringified indices as keys
            items = ((str(idx), child) for idx, child in enumerate(node))
        else:
            return  # dead end

        for key, child in items:
            m = seg_regex.fullmatch(str(key))
            if not m:
                continue
            new_ctx = dict(ctx)
            # Merge captured groups into context
            new_ctx.update(m.groupdict())
            yield from dfs(child, i + 1, new_ctx)

    yield from dfs(root, 0, {})

def json_extract(
    json_in: Union[str, Path, JSONLike],
    pattern: str,
    columns: Dict[str, str],
) -> pl.DataFrame:
    """
    Scan JSON for all matches of `pattern` and build a Polars DataFrame with columns defined by `columns`.

    Parameters
    ----------
    json_in : str | Path | dict | list
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
    data = _load_json(json_in)
    rows = []

    for match in _match_pattern(data, pattern):
        row = {}
        for col_name, spec in columns.items():
            if spec.startswith('/'):
                # abs path
                value = _resolve_path(data, spec, match)
            else:
                m =  _PLACEHOLDER_INNER_RE.fullmatch(spec)
                if not m:
                    raise ValueError(f"Column value specification could be either absolute path or a single placeholder,\n got: '{spec}'")
                try:
                    value = match.get_placeholder(m.group(1))
                except KeyError:
                    raise ValueError(f"")
            row[col_name] = value

        rows.append(row)

    # Build DataFrame; if there are no rows, create an empty DF with the requested columns
    if rows:
        return pl.from_dicts(rows)
    else:
        # default to pl.String for empty DF to keep columns visible; adjust if you prefer pl.Null
        return pl.DataFrame({k: pl.Series([], dtype=pl.String) for k in columns.keys()})
