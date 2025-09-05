import math
from datetime import datetime
from typing import Sequence

from . import FilterParser
import operator

class DictFilterParser(FilterParser):
    op_map = {
        'eq': operator.eq,
        'ne': operator.ne,
        'gt': operator.gt,
        'lt': operator.lt,
        'ge': operator.ge,
        'le': operator.le,
        'add': operator.add,
        'sub': operator.sub,
        'mul': operator.mul,
        'div': operator.truediv,
        'mod': operator.mod,
        'AND': operator.and_,
        'OR': operator.or_,
        'like': lambda a, b: b in a,
        'endswith': lambda a, b: a.endswith(b),
        'startswith': lambda a, b: a.startswith(b),
        'contains': lambda a, b: b in a, # String
        'lacks': lambda a, b: b not in a, #String
        'has': lambda a, b: b in a, # Sequence
        'hasNot': lambda a, b: b not in a, #Sequence
    }

    func_map = {
        'length': len,
        'indexOf': str.index,
        'replace': str.replace,
        'substring': slice,
        'toLower': str.lower,
        'toUpper': str.upper,
        'trim': str.strip,
        'round': round,
        'floor': math.floor,
        'ceiling': math.ceil,
        'year': datetime.year,
        'month': datetime.month,
        'day': datetime.day,
        'hour': datetime.hour,
        'minute': datetime.minute,
        'second': datetime.second,
    }

    def __init__(self, data: Sequence[dict]):
        super().__init__(op_map=self.op_map, func_map=self.func_map,)
        self.data = data

    def get_column(self, col: str):
        return self.data[col]

    def apply_filter(self, filter_string: str, items: Sequence):
        criteria = self.create_filter(filter_string)
        return [data for data in items if criteria]

