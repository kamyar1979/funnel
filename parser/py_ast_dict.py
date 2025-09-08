from __future__ import annotations

import math
from datetime import datetime, time
from functools import lru_cache
from typing import Any, Callable, Iterable, Mapping, Sequence

from parser import FilterParser


# ----------------------------
# Safe dotted-path accessor
# ----------------------------

def _get_path(row: Any, path: str, *, allow_private: bool = False) -> Any:
    cur = row
    for part in path.split("."):
        if cur is None:
            return None

        if isinstance(cur, Mapping):
            cur = cur.get(part, None)
            continue

        if isinstance(cur, (list, tuple)):
            try:
                idx = int(part)
            except ValueError:
                return None
            if 0 <= idx < len(cur):
                cur = cur[idx]
                continue
            return None

        if not allow_private and (part.startswith("_") or part.startswith("__")):
            return None
        cur = getattr(cur, part, None)

    return cur


# ----------------------------
# Primitive helpers and lifted ops
# ----------------------------

def _to_str(x: Any) -> str:
    return "" if x is None else str(x)


def _endswith(a: Any, b: Any) -> bool:
    return _to_str(a).endswith(_to_str(b))


def _startswith(a: Any, b: Any) -> bool:
    return _to_str(a).startswith(_to_str(b))


def _like(a: Any, b: Any) -> bool:
    return _to_str(b) in _to_str(a)


def _contains(a: Any, b: Any) -> bool:
    if isinstance(a, str):
        return _to_str(b) in a
    if isinstance(a, Mapping):
        return b in a
    try:
        return b in a
    except TypeError:
        return False


def _lacks(a: Any, b: Any) -> bool:
    return not _contains(a, b)


def _in_list(a: Any, seq: Sequence[Any]) -> bool:
    try:
        return a in seq
    except TypeError:
        sseq = list(map(_to_str, seq))
        return _to_str(a) in sseq


# string funcs
def _length(x: Any) -> int:
    try:
        return len(x)
    except Exception:
        return 0


def _indexOf(s: Any, sub: Any) -> int:
    return _to_str(s).find(_to_str(sub))


def _replace(s: Any, old: Any, new: Any) -> str:
    return _to_str(s).replace(_to_str(old), _to_str(new))


def _substring(s: Any, start: int, length: int | None = None) -> str:
    s = _to_str(s)
    return s[start:] if length is None else s[start:start + length]


def _toLower(s: Any) -> str:
    return _to_str(s).lower()


def _toUpper(s: Any) -> str:
    return _to_str(s).upper()


def _trim(s: Any) -> str:
    return _to_str(s).strip()


# numeric funcs (tolerant)
def _round(x: Any) -> int:
    try:
        return round(float(x))
    except Exception:
        return 0


def _floor(x: Any) -> int:
    try:
        return math.floor(float(x))
    except Exception:
        return 0


def _ceiling(x: Any) -> int:
    try:
        return math.ceil(float(x))
    except Exception:
        return 0


# date/time funcs (assumes Python datetime/time objects already in data)
def _year(dt: Any) -> int:
    return getattr(dt, "year", 0) or 0


def _month(dt: Any) -> int:
    return getattr(dt, "month", 0) or 0


def _day(dt: Any) -> int:
    return getattr(dt, "day", 0) or 0


def _hour(dt: Any) -> int:
    return getattr(dt, "hour", 0) or 0


def _minute(dt: Any) -> int:
    return getattr(dt, "minute", 0) or 0


def _second(dt: Any) -> int:
    return getattr(dt, "second", 0) or 0


def _as_fn(x: Any) -> Callable[[Any], Any]:
    """Ensure a value is a callable(row)->value."""
    if callable(x):
        return x
    return lambda row, _x=x: _x


def _bin_value(op: Callable[[Any, Any], Any]) -> Callable[..., Callable[[Any], Any]]:
    """Lift a binary op on values to an op on (row)->value callables."""

    def gen(a, b):
        A, B = _as_fn(a), _as_fn(b)
        return lambda row: op(A(row), B(row))

    return gen


def _un_value(op: Callable[[Any], Any]) -> Callable[..., Callable[[Any], Any]]:
    def gen(a):
        A = _as_fn(a)
        return lambda row: op(A(row))

    return gen


def _tern_value(op: Callable[[Any, Any, Any], Any]) -> Callable[..., Callable[[Any], Any]]:
    def gen(a, b, c):
        A, B, C = _as_fn(a), _as_fn(b), _as_fn(c)
        return lambda row: op(A(row), B(row), C(row))

    return gen


def _nary_bool(combiner: Callable[[list[bool]], bool]) -> Callable[..., Callable[[Any], bool]]:
    """Combine N predicate/value callables into a boolean."""

    def gen(*args):
        Fs = [_as_fn(a) for a in args]
        return lambda row: combiner([bool(f(row)) for f in Fs])

    return gen


# ----------------------------
# Concrete FilterParser for Python in-memory rows
# ----------------------------

class _PythonFilterParser(FilterParser):
    # function names are looked up lowercased by FilterParser
    func_map = {
        'length': _un_value(_length),
        'indexof': _bin_value(_indexOf),
        'replace': _tern_value(_replace),
        'substring': lambda s, start, length=None: (
            _tern_value(lambda a, b, c: _substring(a, b, c))(s, start, length)
            if length is not None else
            _bin_value(lambda a, b: _substring(a, b, None))(s, start)
        ),
        'tolower': _un_value(_toLower),
        'toupper': _un_value(_toUpper),
        'trim': _un_value(_trim),
        'round': _un_value(_round),
        'floor': _un_value(_floor),
        'ceiling': _un_value(_ceiling),
        'year': _un_value(_year),
        'month': _un_value(_month),
        'day': _un_value(_day),
        'hour': _un_value(_hour),
        'minute': _un_value(_minute),
        'second': _un_value(_second),
    }

    # operators must match exactly what your grammar expects
    op_map = {
        # comparisons
        'eq': _bin_value(lambda a, b: a == b),
        'ne': _bin_value(lambda a, b: a != b),
        'gt': _bin_value(lambda a, b: (a is not None and b is not None and a > b)),
        'lt': _bin_value(lambda a, b: (a is not None and b is not None and a < b)),
        'ge': _bin_value(lambda a, b: (a is not None and b is not None and a >= b)),
        'le': _bin_value(lambda a, b: (a is not None and b is not None and a <= b)),
        # arithmetic
        'add': _bin_value(lambda a, b: (a or 0) + (b or 0)),
        'sub': _bin_value(lambda a, b: (a or 0) - (b or 0)),
        'mul': _bin_value(lambda a, b: (a or 0) * (b or 0)),
        'div': _bin_value(lambda a, b: (a or 0) / (b or 1)),
        'mod': _bin_value(lambda a, b: (a or 0) % (b or 1)),
        # logical (n-ary, required by your grammar)
        'AND': _nary_bool(lambda vals: all(vals)),
        'OR': _nary_bool(lambda vals: any(vals)),
        # string-like
        'like': _bin_value(_like),
        'endswith': _bin_value(_endswith),
        'startswith': _bin_value(_startswith),
        # containment / membership
        'contains': _bin_value(_contains),
        'lacks': _bin_value(_lacks),
        'has': _bin_value(_contains),
        'hasNot': _bin_value(lambda a, b: not _contains(a, b)),
        'in': _bin_value(lambda a, b: _in_list(a, b if isinstance(b, (list, tuple, set)) else [b])),
    }

    def __init__(self):
        super().__init__(op_map=self.op_map, func_map=self.func_map)

    def get_column(self, col: str):
        name = col.strip()
        return lambda row, _n=name: _get_path(row, _n)


# ----------------------------
# Public API wrapper
# ----------------------------

class DSLParseError(ValueError):
    pass


class DSLSemanticError(ValueError):
    pass


class DictASTFilterParser:
    """
    Fully uses FilterParser to compile filter strings into predicate(row)->bool.
    """

    def __init__(self) -> None:
        self._parser = _PythonFilterParser()

        # Slightly extend literal recognition *without* importing pyparsing:
        # Just prepend a few common converters so quoted scalars become Python types.
        extra_literal_parsers = [
            (r"^'?\d{2,4}-\d{1,2}-\d{1,2}'?$", lambda s: datetime.strptime(s.strip("'"), "%Y-%m-%d")),
            (r"^'?\d{1,2}:\d{1,2}(:\d{1,2})?'?$", lambda s: time.fromisoformat(s.strip("'"))),
            (r"^[+-]?\d+$", lambda s: int(s.strip("'"))),
            (r"^[+-]?\d+\.\d+$", lambda s: float(s.strip("'"))),
            (r"^(?i:true|false)$", lambda s: s.lower() == "true"),
            (r"^'.*'$", lambda s: s.strip("'")),
        ]
        # Put ours before any existing ones
        self._parser.parsers = extra_literal_parsers + self._parser.parsers

    @lru_cache(maxsize=256)
    def create_predicate(self, filter_string: str) -> Callable[[Any], bool]:
        try:
            fn_or_val = self._parser.create_filter(filter_string)
        except Exception as e:
            # Mirror previous behavior: turn parse failures into DSLParseError
            msg = f"DSL parse error: {e}"
            raise DSLParseError(msg) from e

        fn = fn_or_val if callable(fn_or_val) else (lambda _row, v=fn_or_val: v)

        def pred(row: Any) -> bool:
            try:
                return bool(fn(row))
            except Exception:
                return False

        return pred

    def apply_filter(self, filter_string: str, items: Iterable[Any]) -> list[Any]:
        pred = self.create_predicate(filter_string)
        return [row for row in items if pred(row)]
