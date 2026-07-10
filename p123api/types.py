from enum import Enum, IntEnum
import inspect
import typing
from typing import Literal


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


def _slow_init(self, d: dict):
    cls = type(self)
    annotations = typing.get_type_hints(cls)

    globals_dict = {}
    for hint in annotations.values():
        if inspect.isclass(hint) and issubclass(hint, Enum):
            globals_dict[hint.__name__] = hint

    init_args = ["self", "d"]
    init_body = []

    for key, expected_type in annotations.items():
        if inspect.isclass(expected_type) and issubclass(expected_type, Enum):
            init_body.append(f"self.{key} = {expected_type.__name__}(v) if (v := d.get({key!r})) is not None else None")
        else:
            init_body.append(f"self.{key} = d.get({key!r})")

    init = cls.__init__ = _create_fn("__init__", init_args, init_body, globals_dict)
    init(self, d)


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
    """
    Contains the identifier resulting from an API operation.

    Attributes:
        id: The unique integer identifier.
    """

    id: int


@api_result
class DataSeriesResult:
    """
    Contains the identifier for a data series operation.

    Attributes:
        dataSeriesId: The unique integer identifier of the data series.
    """

    dataSeriesId: int


@api_result
class DataSeriesInfoResult:
    """
    Contains the basic identification details of a data series.

    Attributes:
        dataSeriesId: The unique integer identifier of the data series.
        name: The name of the data series.
    """

    dataSeriesId: int
    name: str


@api_result
class StockFactorResult:
    """
    Contains the identifier for a stock factor operation.

    Attributes:
        factorId: The unique integer identifier of the stock factor.
    """

    factorId: int


@api_result
class StockFactorInfoResult:
    """
    Contains the basic identification details of a stock factor.

    Attributes:
        factorId: The unique integer identifier of the stock factor.
        name: The name of the stock factor.
    """

    factorId: int
    name: str


class RankingMethod(IntEnum):
    """
    Defines the methods used for calculating rankings.

    Attributes:
        PERCENTILE_NA_NEGATIVE: Percentile NAs Negative (2).
        PERCENTILE_NA_NEUTRAL: Percentile NAs Neutral (4).
        NORMAL_DISTRIBUTION: Normal Distribution (Experimental) (1).
    """

    PERCENTILE_NA_NEGATIVE = 2
    PERCENTILE_NA_NEUTRAL = 4
    NORMAL_DISTRIBUTION = 1


@api_result
class RankInfoResult:
    """
    Contains the complete details of a specific ranking system.

    Attributes:
        name: Name of the ranking system.
        id: The unique integer identifier of the ranking system.
        xml: Nodes in XML format.
        currency: The currency associated with the system (e.g., 'USD').
        rankingMethod: The specific method used to calculate rankings, represented by the RankingMethod enum.
        type: Type of ranking system, restricted to 'Stock' or 'ETF'.
        groupUid: Group ID.
        resolveGroupUid:  Group Context ID.
    """

    name: str
    id: int
    xml: str
    currency: str
    rankingMethod: RankingMethod
    type: Literal["Stock", "ETF"]
    groupUid: int
    resolveGroupUid: int


@api_result
class StrategyInfoResult:
    """
    Contains the basic identification details of a strategy.

    Attributes:
        strategyId: The unique integer identifier of the strategy.
        name: The name of the strategy.
    """

    strategyId: int
    name: str
