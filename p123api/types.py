from typing import TypedDict


class SharedResult(TypedDict):
    cost: int
    quotaRemaining: str


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
