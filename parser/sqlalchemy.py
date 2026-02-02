import functools
import operator

from sqlalchemy import Select, and_, func, not_, or_
from sqlalchemy.dialects.postgresql import JSONB

from . import FilterParser


class SQLAlchemyFilterParser[T](FilterParser):

    @staticmethod
    def date_part_func(part: str):
        def date_part_reverse(part_rev: str, val):
            return func.date_part(val, part_rev)

        return functools.partial(date_part_reverse, part)

    op_map = {
        "eq": operator.eq,
        "ne": operator.ne,
        "gt": operator.gt,
        "lt": operator.lt,
        "ge": operator.ge,
        "le": operator.le,
        "add": operator.add,
        "sub": operator.sub,
        "mul": operator.mul,
        "div": operator.truediv,
        "mod": operator.mod,
        "AND": and_,
        "OR": or_,
        "like": lambda a, b: a.ilike(f"%{b}%"),
        "endswith": lambda a, b: a.ilike(f"%{b}"),
        "startswith": lambda a, b: a.ilike(f"{b}%"),
        "contains": lambda a, b: a.in_(b.replace(" ", "").split(",")),
        "lacks": lambda a, b: not_(a.in_(b.replace(" ", "").split(","))),
        "has": lambda a, b: a.any(b),
        "hasNot": lambda a, b: not_(a.all(b)),
    }

    func_map = {
        "length": func.length,
        "indexOf": func.position,
        "replace": func.replace,
        "substring": func.substring,
        "toLower": func.lower,
        "toUpper": func.upper,
        "trim": func.trim,
        "round": func.round,
        "floor": func.floor,
        "ceiling": func.ceiling,
        "year": date_part_func("year"),
        "month": date_part_func("month"),
        "day": date_part_func("day"),
        "hour": date_part_func("hour"),
        "minute": date_part_func("minute"),
        "second": date_part_func("second"),
    }

    def __init__(self, entity_type: type[T]):
        super().__init__(op_map=self.op_map, func_map=self.func_map)
        self.model_type = entity_type

    def get_column(self, column: str):
        return getattr(self.model_type, column)

    def add_filter(self, filter_string: str, query: Select) -> Select:
        return query.where(self.create_filter(filter_string))


class SQLAlchemyJsonFilterParser[T](SQLAlchemyFilterParser[T]):
    def get_column(self, column: str):
        if "." not in column:
            return super().get_column(column)

        parts = column.split(".")
        root_col_name = parts[0]
        json_path = parts[1:]

        col = getattr(self.model_type, root_col_name)

        # Check if it's a JSON/JSONB column to apply JSON path indexing
        if hasattr(col, "type") and isinstance(col.type, JSONB):
            for part in json_path:
                col = col[part]
            # By default, SQLAlchemy JSON access returns a JSON element.
            # For comparisons with strings/numbers, we might need it as_string() or casted.
            # But usually, SQLAlchemy handles comparisons if it's JSONB.
            return col.astext if hasattr(col, "astext") else col

        # If not JSONB, maybe it's a relationship? 
        # For now, we follow the user's request for JSON search.
        return super().get_column(column)
