from enum import Enum, IntEnum
import inspect
import typing
from typing import Literal, Optional


def _create_fn(name, args, body, globals_dict=None):
    """Executes a string of code to build a highly optimized function."""
    args_str = ", ".join(args)
    body_str = "\n        ".join(body)

    txt = f"""
def __builder__():
    def {name}({args_str}):
        {body_str}
    return {name}
"""
    namespace = {}
    exec(txt, globals_dict or {}, namespace)
    return namespace["__builder__"]()


def _slow_init(self, **kwargs):
    cls = type(self)
    annotations = typing.get_type_hints(cls)

    globals_dict = {}
    for hint in annotations.values():
        if inspect.isclass(hint) and issubclass(hint, Enum):
            globals_dict[hint.__name__] = hint

    init_args = ["self"] + [f"{key}=None" for key in annotations.keys()] + ["**kwargs"]
    init_body = []

    for key, expected_type in annotations.items():
        if inspect.isclass(expected_type) and issubclass(expected_type, Enum):
            init_body.append(f"self.{key} = {expected_type.__name__}({key}) if {key} is not None else None")
        else:
            init_body.append(f"self.{key} = {key}")

    init = cls.__init__ = _create_fn("__init__", init_args, init_body, globals_dict)
    init(self, **kwargs)


def _slow_repr(self):
    cls = type(self)
    annotations = typing.get_type_hints(cls)

    repr = cls.__repr__ = _create_fn(
        "__repr__", ["self"], [f"return f'{cls.__name__}({', '.join(f'{k}={{self.{k}!r}}' for k in annotations)})'"]
    )
    return repr(self)


def api_result(cls):
    cls.__init__ = _slow_init
    cls.__repr__ = _slow_repr
    return cls


@api_result
class IdResult:
    id: int


@api_result
class DataSeriesResult:
    dataSeriesId: int


@api_result
class DataSeriesInfoResult:
    dataSeriesId: int
    name: str
    description: str


@api_result
class StockFactorResult:
    factorId: int


@api_result
class StockFactorInfoResult:
    factorId: int
    name: str
    description: str


class RankingMethod(IntEnum):
    PERCENTILE_NA_NEGATIVE = 2
    PERCENTILE_NA_NEUTRAL = 4
    NORMAL_DISTRIBUTION = 1


@api_result
class RankInfoResult:
    name: str
    id: int
    xml: str
    currency: str
    rankingMethod: RankingMethod
    type: Literal["Stock", "ETF"]
    description: Optional[str]
    groupUid: int
    resolveGroupUid: int


@api_result
class StrategyInfoResult:
    strategyId: int
    name: str
    description: str
