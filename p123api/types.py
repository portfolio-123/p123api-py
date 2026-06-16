from enum import IntEnum
from typing import Literal, Optional, TypedDict


class SharedResult(TypedDict):
    cost: int
    quotaRemaining: str


class IdResult(SharedResult):
    id: int


class DataSeriesResult(SharedResult):
    dataSeriesId: int


class DataSeriesInfoResult(DataSeriesResult):
    name: str
    description: str


class StockFactorResult(SharedResult):
    factorId: int


class StockFactorInfoResult(StockFactorResult):
    name: str
    description: str


class RankInfoResult(SharedResult):
    name: str
    id: int
    xml: str
    currency: str
    rankingMethod: int
    type: Literal["Stock", "ETF"]
    description: Optional[str]
    groupUid: int
    resolveGroupUid: int


class RankingMethod(IntEnum):
    PERCENTILE_NA_NEGATIVE = 2
    PERCENTILE_NA_NEUTRAL = 4
    NORMAL_DISTRIBUTION = 1


class StrategyInfoResult(TypedDict):
    strategyId: int
    name: str
    description: str
