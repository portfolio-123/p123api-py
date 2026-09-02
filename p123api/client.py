from __future__ import annotations

from collections.abc import Callable
from io import BufferedIOBase, IOBase, RawIOBase
import requests
import time
from string import Template
from typing import Any, IO, Literal, TYPE_CHECKING, cast, overload
from typing_extensions import deprecated

if TYPE_CHECKING:
    import pandas  # type: ignore[import-not-found]


from p123api.types import (
    Currency,
    DataSeriesInfoResult,
    DataSeriesResult,
    IdResult,
    RankInfoResult,
    RankingMethod,
    StockFactorInfoResult,
    StockFactorResult,
    StrategyInfoResult,
)

ENDPOINT = "https://api.portfolio123.com"
AUTH_PATH = "/auth"
SCREEN_ROLLING_BACKTEST_PATH = "/screen/rolling-backtest"
SCREEN_BACKTEST_PATH = "/screen/backtest"
SCREEN_RUN_PATH = "/screen/run"
UNIVERSE_PATH = "/universe"
RANK_PATH = "/rank"
RANK_RANKS_PATH = "/rank/ranks"
RANK_PERF_PATH = "/rank/performance"
RANK_TOUCH_PATH = Template("/rank/$id/touch")
RANK_CREATE = "/rank/create"
DATA_PATH = "/data"
DATA_UNIVERSE_PATH = "/data/universe"
DATA_PRICES_PATH = Template("/data/prices/$identifier")
STRATEGY_DETAILS_PATH = Template("/strategy/$id")
STRATEGY_HOLDINGS_PATH = Template("/strategy/$id/holdings")
STRATEGY_TRADING_SYSTEM_PATH = Template("/strategy/$id/trading-system")
BOOK_TRADING_SYSTEM_PATH = Template("/strategy/$id/book-trading-system")
SIM_RERUN_PATH = Template("/strategy/$id/rerun")
BOOK_SIM_RERUN_PATH = Template("/strategy/$id/book-rerun")
STRATEGY_INFO_PATH = "/strategy"
STRATEGY_REBALANCE_PATH = Template("/strategy/$id/rebalance")
STRATEGY_REBALANCE_COMMIT_PATH = Template("/strategy/$id/rebalance/commit")
STRATEGY_TRANS_PATH = Template("/strategy/$id/transactions")
STRATEGY_COPY_PATH = Template("/strategy/$id/copy")
BOOK_COPY_PATH = Template("/strategy/$id/copy-book")
STOCK_FACTOR_UPLOAD_PATH = Template("/stockFactor/upload/$id")
STOCK_FACTOR_CREATE_UPDATE_PATH = "/stockFactor"
STOCK_FACTOR_DOWNLOAD_PATH = Template("/stockFactor/$id")
STOCK_FACTOR_DELETE_PATH = Template("/stockFactor/$id")
STOCK_FACTOR_INFO_PATH = "/stockFactor"
DATA_SERIES_UPLOAD_PATH = Template("/dataSeries/upload/$id")
DATA_SERIES_CREATE_UPDATE_PATH = "/dataSeries"
DATA_SERIES_INFO_PATH = "/dataSeries"
DATA_SERIES_DELETE_PATH = Template("/dataSeries/$id")
AIFACTOR_PREDICT_PATH = Template("/aiFactor/predict/$id")


class ClientException(Exception):
    def __init__(self, message, *, resp: requests.Response | None = None):
        super().__init__(message)
        self._resp = resp

    def get_resp(self):
        return self._resp

    def get_cause(self):
        return self.__cause__

    @staticmethod
    def build(message, resp: requests.Response):
        if resp.status_code == 404:
            return ClientItemNotFoundException(resp=resp)
        return ClientException(message=message, resp=resp)


class ClientItemNotFoundException(ClientException):
    def __init__(self, resp: requests.Response):
        super().__init__("Item not found", resp=resp)


class Client:
    """
    class for interfacing with P123 API
    """

    def __init__(self, *, api_id: str | int, api_key: str, auth_extra={}, endpoint=ENDPOINT, verify_requests=True):
        self._endpoint = endpoint
        self._verify_requests = verify_requests
        self._max_req_retries = 5
        self._timeout = 300
        self._token = None
        self.cost = None
        self.quotaRemaining = None
        if not isinstance(api_id, (str, int)):
            raise ClientException("api_id must be str or int")
        if not isinstance(api_key, str):
            raise ClientException("api_key must be str")

        self._auth_params = {"apiId": str(api_id), "apiKey": api_key, **auth_extra}
        self._session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._session.close()

    def set_max_request_retries(self, retries: int):
        if not isinstance(retries, int) or retries < 1 or retries > 10:
            raise ClientException("retries needs to be an int between 1 and 10")
        self._max_req_retries = retries

    def set_timeout(self, timeout: int):
        if not isinstance(timeout, int) or timeout < 1:
            raise ClientException("timeout needs to be an int greater than 0")
        self._timeout = timeout

    def get_token(self):
        return self._token

    def auth(self):
        """
        Authenticates and sets the Bearer authorization header on success. This method doesn't need to be called
        explicitly since all requests first check if the authorization header is set and attempt to re-authenticate
        if session expires.
        :return: bool
        """
        self._session.headers.clear()
        with req_with_retry(
            "POST",
            self._endpoint + AUTH_PATH,
            self._session,
            self._max_req_retries,
            json=self._auth_params,
            verify=self._verify_requests,
            timeout=30,
        ) as resp:
            if resp.status_code == 200:
                self._token = resp.text
                self._session.headers.update({"Authorization": f"Bearer {resp.text}"})
                return

            if resp.status_code == 406:
                message = "user account inactive"
            elif resp.status_code == 402:
                message = "paying subscription required"
            elif resp.status_code == 401:
                message = "invalid id/key combination or key inactive"
            elif resp.status_code == 400:
                message = "invalid key"
            else:
                message = resp.text
            if message:
                message = ": " + message
            raise ClientException(f"API authentication failed{message}", resp=resp)

    def _req_with_auth_fallback(
        self,
        *,
        method: Literal["GET", "POST", "DELETE"] = "POST",
        url: str,
        json: Any = None,
        params: list[tuple[str, Any]] | None = None,
        data: str | IO | None = None,
        headers: dict[str, str] | None = None,
        result_type: type | None = None,
    ) -> Any:
        """
        Request with authentication fallback, used by all requests (except authentication)
        :param method: request method
        :param url: request url
        :param json: request json
        :param params: request params
        :param data: request data
        :param headers: request headers
        :return: request response object
        """
        reauth = False
        while True:
            if self._session.headers.get("Authorization") is None:
                self.auth()
            with req_with_retry(
                method,
                url,
                self._session,
                self._max_req_retries,
                json=json,
                params=params,
                verify=self._verify_requests,
                timeout=self._timeout,
                data=data,
                headers=headers,
            ) as resp:

                if resp.status_code == 200:
                    json = resp.json()
                    if isinstance(json, dict):
                        self.cost = json.get("cost")
                        self.quotaRemaining = json.get("quotaRemaining")
                    else:
                        self.cost = None
                        self.quotaRemaining = None
                    return result_type(json) if result_type is not None else json

                if resp.status_code == 401 or resp.status_code == 403:
                    del self._session.headers["Authorization"]
                    if not reauth:
                        reauth = True
                        continue

                message = resp.text
                if not message and resp.status_code == 402:
                    message = "request quota exhausted"
                if message:
                    message = ": " + message
                raise ClientException.build(f"API request failed{message}", resp=resp)

    def screen_rolling_backtest(self, params: dict, to_pandas=False):
        """
        Executes a screen rolling backtest.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the rolling backtest.
                Key arguments include::

                    - screen (int | dict): Required. The screen ID or screen definition parameters.
                      For more information about and examples of using a dict, see the article "API functions: Screen" in the Knowledge Base.
                    - startDt (str): Required. Backtest start date (yyyy-mm-dd).
                    - endDt (str): Backtest end date (yyyy-mm-dd).
                    - frequency (str): Rebalance frequency. Allowed values are 'Every Week', 'Every 2 Weeks',
                      'Every 3 Weeks', 'Every 4 Weeks', 'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks',
                      'Every 26 Weeks', or 'Every 52 Weeks'. Defaults to 'Every 4 Weeks'.
                    - holdingPeriod (int): Holding period in days (1 to 730). Defaults to 182.
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete').
                    - transPrice (int): Transaction price type (1=Next Open, 4=Next Close, 3=Next Average). Defaults to 1.
                    - slippage (float): Slippage percentage. Defaults to 0.25.
                    - longWeight (float): Long weight percentage. Defaults to 100.
                    - shortWeight (float): Short weight percentage. Defaults to 100.
                    - maxPosPct (float): Maximum position percentage (0 to 100). Limits the allocation to each position in the returned screen,
                      working in conjunction with the maximum number of holdings. Defaults to 0.
            to_pandas (bool): If True, converts the 'rows' and 'columns' of the result into a pandas DataFrame.

        Returns:
            A dictionary containing the backtest results, including columns, rows,
            averages and up/down market metrics (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "pitMethod": "Prelim",
            ...     "precision": 2,
            ...     "screen": 254671,
            ...     "transPrice": 1,
            ...     "maxPosPct": 0,
            ...     "slippage": 0.25,
            ...     "longWeight": 100,
            ...     "shortWeight": 100,
            ...     "startDt": "2019-11-20",
            ...     "endDt": "2020-11-20",
            ...     "frequency": "Every Week",
            ...     "holdingPeriod": 182
            ... }
            >>> client.screen_rolling_backtest(params, to_pandas=False)
            {
                'columns': ['#', 'As Of Dt', 'Rank Dt', ...],
                'rows': [
                    [14, '2020-11-15', '2020-11-14', ...],
                    ...
                ],
                'average': [null, null, null, ...],
                'upMarkets': [null, 14.0, null, ...],
                'downMarkets': [null, 0.0, null, ...]
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_ROLLING_BACKTEST_PATH, json=params)

        if to_pandas:
            import pandas

            rows = ret["rows"]
            ret["average"][0] = "Average"
            rows.append(ret["average"])
            ret["upMarkets"][0] = "Up Markets"
            rows.append(ret["upMarkets"])
            ret["downMarkets"][0] = "Down Markets"
            rows.append(ret["downMarkets"])
            ret = pandas.DataFrame(data=rows, columns=ret["columns"])

        return ret

    def screen_backtest(self, params: dict, to_pandas=False):
        """
        Executes a screen backtest.

        Args:
            params (dict): A dictionary of parameters for the screen backtest.
                Key arguments include::

                    - screen (int | dict): Required. The screen ID or screen definition parameters.
                      For more information about and examples of using a dict, see the article "API functions: Screen" in the Knowledge Base.
                    - startDt (str): Required. Backtest start date (yyyy-mm-dd).
                    - endDt (str): Backtest end date (yyyy-mm-dd).
                    - rebalFreq (str): Rebalance frequency. Allowed values are 'Every Week', 'Every 2 Weeks',
                      'Every 3 Weeks', 'Every 4 Weeks', 'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks',
                      'Every 26 Weeks', or 'Every 52 Weeks'. Defaults to 'Every 4 Weeks'.
                    - riskStatsPeriod (str): Risk statistics period ('Monthly', 'Weekly', 'Daily'). Defaults to 'Monthly'.
                    - rankTolerance (float): Rank tolerance. Defaults to 0.
                    - carryCost (float): Carry cost percentage. Defaults to 1.5.
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete').
                    - transPrice (int): Transaction price type (1=Next Open, 4=Next Close, 3=Next Average). Defaults to 1.
                    - slippage (float): Slippage percentage. Defaults to 0.25.
                    - longWeight (float): Long weight percentage. Defaults to 100.
                    - shortWeight (float): Short weight percentage. Defaults to 100.
                    - maxPosPct (float): Maximum position percentage (0 to 100).
                      Limits the allocation to each position in the returned screen, working in conjunction with the maximum number of holdings. Defaults to 0.
            to_pandas (bool): If True, converts the tabular components of the results into a pandas DataFrame.

        Returns:
            A dictionary containing the backtest results, which includes performance stats (alpha, beta, Sharpe ratio, etc.),
            tabular results, and charting data.

        Examples:
            >>> params = {
            ...     "pitMethod": "Prelim",
            ...     "precision": 2,
            ...     "screen": 90108,
            ...     "transPrice": 1,
            ...     "maxPosPct": 0,
            ...     "slippage": 0.25,
            ...     "longWeight": 100,
            ...     "shortWeight": 100,
            ...     "startDt": "2026-06-24",
            ...     "endDt": "2026-06-24",
            ...     "rankTolerance": 0,
            ...     "carryCost": 1.5,
            ...     "rebalFreq": "Every 4 Weeks",
            ...     "riskStatsPeriod": "Monthly"
            ... }
            >>> client.screen_backtest(params, to_pandas=False)
            {
                'stats': {
                    'samples': 10,
                    'correlation': 0.95,
                    'r_squared': 0.9,
                    'beta': 1.36,
                    'alpha': -0.39,
                    'port': {
                        'standard_dev': 40.44,
                        'sharpe_ratio': 0.64,
                        'sortino_ratio': 0.84,
                        'total_return': 20.58,
                        'annualized_return': 20.6,
                        'max_drawdown': -41.62
                    },
                    'bench': {
                        'standard_dev': 28.31,
                        'sharpe_ratio': 0.68,
                        'sortino_ratio': 0.91,
                        'total_return': 17.24,
                        'annualized_return': 17.25,
                        'max_drawdown': -33.72
                    }
                },
                'results': {
                    'columns': ['#', 'As Of Dt', 'Rank Dt', ...],
                    'rows': [
                        [14, '2020-12-27', '2020-12-26', ...],
                        ...
                    ],
                    'average': [null, null, null, ...],
                    'upMarkets': [null, 9.0, null, ...],
                    'downMarkets': [null, 5.0, null, ...]
                },
                'chart': {
                    'dates': ['2020-01-01', '2020-01-02', '2020-01-03', ...],
                    'screenReturns': [100.0, 99.75, 99.29, ...],
                    'benchReturns': [100.0, 100.0, 99.24, ...],
                    'turnoverPct': [0.0, 0.0, 0.0, ...],
                    'positionCnt': [0.0, 3696.0, 3696.0, ...]
                }
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_BACKTEST_PATH, json=params)

        if to_pandas:
            import pandas

            columns = [
                "",
                "Total Return",
                "Annualized Return",
                "Max Drawdown",
                "Sharpe",
                "Sortino",
                "StdDev",
                "CorrelBench",
                "R-Squared",
                "Beta",
                "Alpha",
            ]
            stats = ret["stats"]
            port_stats = stats["port"]
            bench_stats = stats["bench"]
            rows = [
                [
                    "Screen",
                    port_stats["total_return"],
                    port_stats["annualized_return"],
                    port_stats["max_drawdown"],
                    port_stats.get("sharpe_ratio"),
                    port_stats.get("sortino_ratio"),
                    port_stats.get("standard_dev"),
                    stats.get("correlation"),
                    stats.get("r_squared"),
                    stats.get("beta"),
                    stats.get("alpha"),
                ],
                [
                    "Benchmark",
                    bench_stats["total_return"],
                    bench_stats["annualized_return"],
                    bench_stats["max_drawdown"],
                    bench_stats.get("sharpe_ratio"),
                    bench_stats.get("sortino_ratio"),
                    bench_stats.get("standard_dev"),
                    "",
                    "",
                    "",
                    "",
                ],
            ]
            panda_stats = pandas.DataFrame(data=rows, columns=columns)

            rows = ret["results"]["rows"]
            ret["results"]["average"][0] = "Average"
            rows.append(ret["results"]["average"])
            ret["results"]["upMarkets"][0] = "Up Markets"
            rows.append(ret["results"]["upMarkets"])
            ret["results"]["downMarkets"][0] = "Down Markets"
            rows.append(ret["results"]["downMarkets"])
            panda_results = pandas.DataFrame(data=rows, columns=ret["results"]["columns"])

            columns = ["Date", "Screen Return", "Bench Return", "Turnover %", "Position Count"]
            chart = ret["chart"]
            rows = []
            for idx, date in enumerate(chart["dates"]):
                rows.append(
                    [date, chart["screenReturns"][idx], chart["benchReturns"][idx], chart["turnoverPct"][idx], chart["positionCnt"][idx]]
                )
            panda_chart = pandas.DataFrame(data=rows, columns=columns)

            ret = {"stats": panda_stats, "results": panda_results, "chart": panda_chart}

        return ret

    def screen_run(self, params: dict, to_pandas=False):
        """
        Executes a screen run.

        Args:
            params (dict): A dictionary of parameters for the screen run.
                Key arguments include::

                    - screen (int | dict): Required. The screen ID or screen definition parameters.
                      For more information about and examples of using a dict, see the article "API functions: Screen" in the Knowledge Base.
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete'). Overrides for existing screens or sets for new screens (defaults to 'Complete').
                    - precision (int): Fixed precision digits (2 to 8). Defaults to 2.
                    - asOfDt (str): As of date (yyyy-mm-dd). Defaults to today.
                    - endDt (str): End date (yyyy-mm-dd).
            to_pandas (bool): If True, converts the tabular components of the results into a pandas DataFrame.

        Returns:
            A dictionary containing the screen run results, typically including columns and rows (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "pitMethod": "Prelim",
            ...     "precision": 2,
            ...     "screen": 10737,
            ...     "asOfDt": "2020-01-08",
            ...     "endDt": "2026-06-24"
            ... }
            >>> client.screen_run(params, to_pandas=False)
            {
                'columns': ['P123 UID', 'Ticker', 'Name', ...],
                'rows': [
                    [774, 'AAPL', 'Apple, Inc.', ...],
                    ...
                ]
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_RUN_PATH, json=params)

        if to_pandas:
            import pandas

            ret = pandas.DataFrame(data=ret["rows"], columns=ret["columns"])

        return ret

    def universe_update(self, params: dict) -> None:
        """
        Create or update a universe named 'ApiUniverse' that can be used in other endpoints.<br>
        WARNING: Since ApiUniverse is an actual custom universe in your account, calling this endpoint while
        there are ongoing requests using that universe will yield unexpected results.

        Args:
            params (dict[str, Any]): A dictionary containing the universe configuration.
                Expected keys include::

                    - type (str): Type of universe ('Stock' or 'ETF'). Defaults to 'Stock'.
                    - rules (list[str]): Required. A list of strings defining the universe rules.
                    - startingUniverse (str): Optional. The starting universe to build upon.
                    - currency (str): Currency (e.g., 'USD', 'CAD', 'EUR', 'GBP', 'CHF', 'NOK', 'PLN', 'SEK', 'TRY').
                      Defaults to 'USD'.

        Examples:
            >>> params = {
            ...     "type": "Stock",
            ...     "rules": [
            ...         "Price > 1"
            ...     ],
            ...     "currency": "USD",
            ...     "startingUniverse": "ALLFUND"
            ... }
            >>> client.universe_update(params)
            None
        """
        return self._req_with_auth_fallback(url=self._endpoint + UNIVERSE_PATH, json=params)

    def rank_update(self, params: dict) -> None:
        """
        Updates a ranking system.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the ranking system update.
                Key arguments include::

                    - type (str): Required. Type of ranking system ('Stock' or 'ETF').
                    - nodes (str): Required. Nodes in XML format.
                    - id (int): The ID of the ranking system to update. Omit this to update the API ranking system.
                    - rankingMethod (int): Ranking method (2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution (Experimental)). Defaults to 2.
                    - currency (str): Currency (e.g., 'USD', 'CAD', 'EUR', 'GBP', 'CHF', 'NOK', 'PLN', 'SEK', 'TRY'). Defaults to 'USD'.

        Examples:
            >>> params = {
            ...     "id": 2373,
            ...     "type": "Stock",
            ...     "rankingMethod": 2,
            ...     "nodes": "<RankingSystem RankType=\"Higher\">...</RankingSystem>",
            ...     "currency": "USD"
            ... }
            >>> client.rank_update(params)
            None
        """
        return self._req_with_auth_fallback(url=self._endpoint + RANK_PATH, json=params)

    def data(self, params: dict, to_pandas=False) -> dict[str, Any] | pandas.DataFrame:
        """
        Retrieves time-series data for specified formulas and identifiers.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the data request.
                Expected keys include::

                    - formulas (list[str]): Required. Array of formulas to evaluate.
                    - startDt (str): Required. Start date (yyyy-mm-dd).
                    - endDt (str): End date (yyyy-mm-dd).
                    - p123Uids (list[int]): Array of P123 UIDs (maximum 100).
                    - tickers (list[str]): Array of tickers (maximum 100).
                    - gvkeys (list[str]): Array of GVKeys (maximum 100).
                    - ciks (list[str]): Array of CIKs (maximum 100).
                    - figis (list[str]): Array of FIGIs (maximum 100).
                    - frequency (str): Retrieval frequency. Allowed values are 'Every Week', 'Every 2 Weeks',
                      'Every 3 Weeks', 'Every 4 Weeks', 'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks',
                      'Every 26 Weeks', or 'Every 52 Weeks'. Defaults to 'Every Week'.
                    - region (str): Region scope ('United States', 'Canada', 'North America', 'Europe', 'North Atlantic').
                      Defaults to 'United States'.
                    - ignoreErrors (bool): If True, ignores invalid/ambiguous P123 UIDs, tickers, GVKeys, CIKs, or FIGIs instead of failing. Defaults to True.
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete'). Defaults to Complete.
                    - precision (int): Fixed precision digits (2 to 8). Pass None for no additional rounding. Defaults to 2.
                    - currency (str): Currency (e.g., 'USD', 'CAD', 'EUR', 'GBP', 'CHF', 'NOK', 'PLN', 'SEK', 'TRY').
                      Defaults to 'USD'.
                    - benchmark (str): Benchmark ticker.
                    - includeNames (bool): Whether to include company names/tickers in the output.

        Returns:
            A dictionary containing the requested data items grouped by identifier, containing their ticker
            and respective data series (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "pitMethod": "Complete",
            ...     "precision": 2,
            ...     "currency": "USD",
            ...     "benchmark": "SPY",
            ...     "formulas": ["close(0)", "peg"],
            ...     "includeNames": True,
            ...     "tickers": ["IBM"],
            ...     "startDt": "2025-02-01",
            ...     "endDt": "2026-06-24",
            ...     "frequency": "Every Week",
            ...     "region": "United States",
            ...     "ignoreErrors": True
            ... }
            >>> client.data(params)
            {
                'dates': ['2025-02-01', '2025-02-08', '2025-02-15', ...],
                'items': {
                    '4737': {
                        'ticker': 'IBM',
                        'figi': 'BBG000BLNQ16',
                        'series': [
                            [244.283, 241.073, 251.277, ...],
                            [5.043, 4.967, 5.143, ...]
                        ]
                    }
                }
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + DATA_PATH, json=params)

        if to_pandas:
            import pandas

            raw_obj = dict(ret)
            with_cusips = params.get("cusips") is not None
            with_name = params.get("includeNames")
            data = []
            for date_idx, date in enumerate(ret["dates"]):
                for item_uid, item_data in ret["items"].items():
                    row = [date, item_uid, item_data["ticker"]]
                    if with_cusips:
                        row.append(item_data["cusip"])
                    if with_name:
                        row.append(item_data["name"])
                    for formula_idx, formula in enumerate(params["formulas"]):
                        row.append(item_data["series"][formula_idx][date_idx])
                    data.append(row)
            columns = ["date", "p123Uid", "ticker"]
            if with_cusips:
                columns.append("cusip")
            if with_name:
                columns.append("name")
            for formula_idx, formula in enumerate(params["formulas"]):
                columns.append(f"formula{formula_idx + 1}")
            ret = pandas.DataFrame(data=data, columns=columns)
            ret.attrs["raw_obj"] = raw_obj

        return ret

    def data_universe(self, params: dict, to_pandas=False) -> dict[str, Any] | pandas.DataFrame:
        """
        Retrieves universe data for specified formulas and parameters.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the data request.
                Expected keys include::

                    - universe (int | str): Required. The universe ID or name (use 'ApiUniverse' for temporary ones).
                    - formulas (list[str]): Required. Array of formulas to evaluate.
                    - type (str): Type of universe ('Stock' or 'ETF'). Defaults to 'Stock'.
                    - currency (str): Currency (e.g., 'USD', 'CAD', 'EUR', 'GBP', 'CHF', 'NOK', 'PLN', 'SEK', 'TRY'). Defaults to 'USD'.
                    - benchmark (str): Benchmark ticker.
                    - precision (int): Fixed precision digits (2 to 8). Pass None for no additional rounding. Defaults to 2.
                    - asOfDt (str): As of date (yyyy-mm-dd).
                    - asOfDts (list[str]): Array of as of dates (yyyy-mm-dd).
                    - figi (str): FIGI mapping ('Share Class' or 'Country Composite').
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete'). Defaults to Complete.
                    - includeNames (bool): Whether to include company names in the output.
                    - preproc (dict[str, Any]): Preprocessor configuration dictionary, containing::
                        - scaling (str): Required. Scaling method ('normal', 'rank', 'minmax').
                        - naFill (bool): Set NAs to the middle values. Defaults to False.
                        - scope (str): Preprocessor scope ('dataset', 'training', 'date'). Defaults to 'date'.
                        - trimPct (float): Trim percentage. Defaults to 0.
                        - outliers (bool): Clip outliers. Defaults to False.
                        - outlierLimit (float): Used for normal scaling. Defaults to 0.
                        - mlTrainingEnd (str): End date for scaling when scope='dataset'.
                        - excludedFormulas (list[str]): Formulas excluded (data license required for non-technical factors).

            to_pandas (bool): If True, converts the data, tickers, and names arrays into a pandas DataFrame.

        Returns:
            A dictionary containing the date and parallel arrays of P123 UIDs, tickers, names, and
            the requested data (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "pitMethod": "Complete",
            ...     "formulas": [
            ...         "close(0)"
            ...     ],
            ...     "includeNames": True,
            ...     "precision": 2,
            ...     "currency": "USD",
            ...     "benchmark": "SPY",
            ...     "type": "ETF",
            ...     "universe": "Prussell 2000",
            ...     "asOfDt": "2026-06-24",
            ...     "asOfDts": [
            ...         "2026-06-24"
            ...     ],
            ...     "figi": "Share Class",
            ...     "preproc": {
            ...         "scaling": "normal"
            ...     }
            ... }
            >>> client.data_universe(params, to_pandas=False)
            {
                'dt': '024-07-01',
                'p123Uids': [159, 115627, 26968, ...],
                'tickers': ['RAMP', 'BHVN', 'AROC', ...],
                'data': [
                    [30.94, 34.71, 20.22, ...],
                    ...
                ],
                'names': ['LiveRamp Holdings, Inc.', 'Biohaven Ltd.', 'Archrock, Inc.', ...],
                'figi': ['BBG001S5NCJ3', 'BBG017BXV3R8', 'BBG001SVDK72' ...]
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + DATA_UNIVERSE_PATH, json=params)

        if to_pandas:
            import pandas

            raw_obj = ret
            f_indices = range(len(params["formulas"]))

            names = params.get("names")
            if names is None:
                names = [f"formula{i + 1}" for i in f_indices]

            if params.get("asOfDt"):
                for formula_idx in f_indices:
                    name = names[formula_idx]
                    ret[name] = ret["data"][formula_idx]
                del ret["dt"], ret["cost"], ret["quotaRemaining"], ret["data"]
                ret = pandas.DataFrame(ret)
            else:
                includeNames = bool(params.get("includeNames"))
                includeFigi = bool(params.get("figi"))

                data = {"dates": [], "p123Uids": [], "tickers": []}
                if includeNames:
                    data["names"] = []
                if includeFigi:
                    data["figi"] = []

                date_data = [[] for _ in f_indices]

                for formula_idx in f_indices:
                    data[names[formula_idx]] = date_data[formula_idx]

                for dtObj in ret["dates"]:
                    data["dates"].extend([dtObj["dt"]] * len(dtObj["p123Uids"]))
                    data["p123Uids"].extend(dtObj["p123Uids"])
                    data["tickers"].extend(dtObj["tickers"])
                    if includeNames:
                        data["names"].extend(dtObj["names"])
                    if includeFigi:
                        data["figi"].extend(dtObj["figi"])
                    for formula_idx in f_indices:
                        date_data[formula_idx].extend(dtObj["data"][formula_idx])
                ret = pandas.DataFrame(data)
            ret.attrs["raw_obj"] = raw_obj

        return ret

    def rank_ranks(self, params: dict, to_pandas=False):
        """
        Retrieves ranking system ranks for a specific date.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the ranks request.
                Key arguments include::

                    - rankingSystem (int | str): Required. The ranking system ID or name.
                    - asOfDt (str): Required. As of date (yyyy-mm-dd).
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete'). Defaults to 'Complete'.
                    - precision (int): Fixed precision digits (2 to 8). Defaults to 2.
                    - universe (str): Universe name (use 'ApiUniverse' for temporary ones).
                    - rankingMethod (int): Ranking method (2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution (Experimental)). Defaults to 2.
                    - tickers (str): Comma-delimited list of tickers to include.
                    - includeNames (bool): Include company names in the output. Defaults to False.
                    - includeNaCnt (bool): Include NA count. Defaults to False.
                    - includeFinalStmt (bool): Include final statement flag. Defaults to False.
                    - includeNodeDetails (bool): Include node details. Defaults to False.
                    - nodeDetails (str): Include node details ('composite' or 'factor'). Omit for no details.
                    - additionalData (list[str]): Additional data formulas to evaluate (maximum 100).
                    - currency (str): Currency (e.g., 'USD', 'CAD', 'EUR', 'GBP', 'CHF', 'NOK', 'PLN', 'SEK', 'TRY').
                      Defaults to 'USD'.
                    - figi (str): FIGI mapping ('Share Class' or 'Country Composite').
            to_pandas (bool): If True, converts the resulting arrays into a pandas DataFrame.

        Returns:
            A dictionary containing the date, parallel arrays for UIDs, tickers, ranks, node details, and requested additional data (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "pitMethod": "Complete",
            ...     "precision": 3,
            ...     "rankingSystem": 267332,
            ...     "universe": "Prussell 2000",
            ...     "rankingMethod": 2,
            ...     "asOfDt": "2020-04-24",
            ...     "tickers": "AAPL",
            ...     "includeNames": True,
            ...     "includeNaCnt": False,
            ...     "includeFinalStmt": True,
            ...     "includeNodeDetails": False,
            ...     "nodeDetails": "composite",
            ...     "additionalData": [
            ...         "close(2)", "close(1)", "close(0)"
            ...     ],
            ...     "currency": "USD",
            ...     "figi": "Share Class"
            ... }
            >>> client.rank_ranks(params, to_pandas=False)
            {
                'dt': '2020-04-18',
                'p123Uids': [774],
                'tickers': ['AAPL' ],
                'names': ['Apple, Inc.' ],
                'naCnt': [16 ],
                'finalStmt': [true ],
                'ranks': [97.872 ],
                'nodes': {
                    'ids': [0, 1, 6, ...],
                    'names': [
                        'Industry performance comparison',
                        'TechRank',
                        'Valuation',
                        ...
                    ],
                    'parents': [0, 0, 0, ...],
                    'weights': [100.0, 40.0, 40.0, ...],
                    'ranks': [
                        [97.872, 97.642, 69.321, ...],
                        ...
                    ]
                },
                'additionalData': [
                    [284.43, 286.69, 282.],
                    ...
                ],
                'figi': ["BBG001S5N8V8"]
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + RANK_RANKS_PATH, json=params)

        if to_pandas:
            import pandas

            names = dict()
            raw_obj = dict(ret)
            del ret["cost"], ret["quotaRemaining"], ret["dt"]
            nodes = ret.get("nodes")
            if nodes is not None:
                for node_idx, node_name in enumerate(nodes["names"]):
                    if node_idx > 0:
                        node_name = node_name + f" ({nodes['weights'][node_idx]}%)"
                        if names.get(node_name) is not None:
                            idx = names[node_name] + 1
                            names[node_name] = idx
                            node_name = node_name + f" #{idx}"
                        else:
                            names[node_name] = 0
                        ret[node_name] = []
                        for idx, uid in enumerate(ret["p123Uids"]):
                            ret[node_name].append(nodes["ranks"][idx][node_idx])
                del ret["nodes"]
            additional_data = ret.get("additionalData")
            if additional_data is not None:
                for data_idx, data_name in enumerate(params["additionalData"]):
                    data_name = f"formula{data_idx + 1}"
                    ret[data_name] = []
                    for idx, uid in enumerate(ret["p123Uids"]):
                        ret[data_name].append(additional_data[idx][data_idx])
                del ret["additionalData"]
            ret = pandas.DataFrame(data=ret)
            ret.attrs["raw_obj"] = raw_obj

        return ret

    def rank_perf(self, params: dict):
        """
        Evaluates the performance of a ranking system.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the ranking system performance evaluation.
                Key arguments include::

                    - rankingSystem (int | str): Required. The ranking system ID or name.
                    - startDt (str): Required. Start date (yyyy-mm-dd).
                    - endDt (str): End date (yyyy-mm-dd).
                    - numBuckets (int): Number of rank buckets (2 to 200). Defaults to 20.
                    - maxNAs (float): Maximum number of NAs (9999 or unspecified to disable).
                    - minPrice (float): Minimum price. Defaults to 3.
                    - minLiquidity (float): Minimum liquidity (0 or unspecified to disable).
                    - maxReturn (float): Maximum return (0 or unspecified to disable).
                    - rebalFreq (str): Rebalance frequency. Allowed values are 'Every Week', 'Every 2 Weeks',
                      'Every 3 Weeks', 'Every 4 Weeks', 'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks',
                      'Every 26 Weeks', or 'Every 52 Weeks'. Defaults to 'Every 4 Weeks'.
                    - slippage (float): Slippage percentage applied when a stock changes bucket to make
                      performance more realistic. Defaults to 0.
                    - transType (str): Transaction type ('long' or 'short'). Defaults to 'long'.
                    - benchmark (str): Benchmark ticker. Defaults to 'SPY'.
                    - outputType (str): Output type ('ann' for annualized returns, 'perf' for performance). Defaults to 'ann'.
                    - pitMethod (str): Point-in-Time method ('Prelim' or 'Complete'). Defaults to 'Complete'.
                    - precision (int): Fixed precision digits (2 to 8). Defaults to 2.
                    - universe (str): Universe name.
                    - rankingMethod (int): Ranking method (2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution (Experimental)).

        Returns:
            A dictionary containing the ranking system's performance results.

        Examples:
            >>> params = {
            ...     "pitMethod": "Complete",
            ...     "precision": 2,
            ...     "rankingSystem": 1073,
            ...     "universe": "Prussell 2000",
            ...     "rankingMethod": 2,
            ...     "numBuckets": 20,
            ...     "maxNAs": 100,
            ...     "minPrice": 3,
            ...     "minLiquidity": 5000,
            ...     "maxReturn": 200,
            ...     "rebalFreq": "Every 4 Weeks",
            ...     "slippage": 0,
            ...     "transType": "long",
            ...     "benchmark": "SPY",
            ...     "startDt": "2015-04-21",
            ...     "endDt": "2026-06-25",
            ...     "outputType": "ann"
            ... }
            >>> client.rank_perf(params)
            {
                'benchmarkAnnRet': 13.6885,
                'bucketAnnRet': [2.1824, 5.9422, 6.8893, ...]
            }
        """
        return self._req_with_auth_fallback(url=self._endpoint + RANK_PERF_PATH, json=params)

    def rank_touch(self, rank_id: int) -> None:
        """
        Touches a ranking system. This invalidates cached ranks for this ranking system.
        The next request referencing this ranking system will be forced to recompute ranks.

        Args:
            rank_id (int): The ID of the ranking system to touch.

        Examples:
            >>> client.rank_touch(107374)
            None
        """
        self._req_with_auth_fallback(method="POST", url=self._endpoint + RANK_TOUCH_PATH.substitute(id=rank_id))

    def rank_create(
        self,
        name: str,
        nodes: str,
        *,
        rankingMethod=RankingMethod.PERCENTILE_NA_NEGATIVE,
        type: Literal["Stock", "ETF"] = "Stock",
        currency: Currency = "USD",
    ) -> IdResult:
        """
        Creates a new ranking system.

        Creates a ranking system based on the provided nodes and configuration parameters.

        Args:
            name: Ranking system name.
            nodes: Ranking system nodes XML.
            rankingMethod: Ranking method to be used.
            type: Ranking method type. Use "Stock" or "ETF".
            currency: Ranking method currency.

        Returns:
            An object containing the new ranking system's id.

        Examples:
            >>> client.rank_create(
            ...     "New Ranking System",
            ...     "<RankingSystem RankType="Higher">...</RankingSystem>",
            ...     rankingMethod=RankingMethod.PERCENTILE_NA_NEGATIVE,
            ...     type="Stock",
            ...     currency="USD"
            ... )
            IdResult(id=98765)
        """

        return self._req_with_auth_fallback(
            method="POST",
            url=self._endpoint + RANK_CREATE,
            json={"name": name, "nodes": nodes, "currency": currency, "type": type, "rankingMethod": rankingMethod},
            result_type=IdResult,
        )

    @overload
    def rank_get(self, *, id: int) -> RankInfoResult:
        """
        Gets information for a specific ranking system.

        Retrieves the full configuration details for a given ranking system by its ID.

        Args:
            id: The unique identifier of the ranking system.

        Returns:
            An object containing the ranking system's details.

        Examples:
            >>> client.rank_get(id=12345)
            RankInfoResult(
                name='My Ranking System',
                id=12345,
                xml='<RankingSystem,...</RankingSystem>',
                currency='USD',
                rankingMethod=<RankingMethod.PERCENTILE_NA_NEGATIVE: 2>,
                type='Stock',
                groupUid=100,
                resolveGroupUid=200
            )
        """
        ...

    @overload
    def rank_get(self, *, name: str) -> RankInfoResult:
        """
        Gets information for a specific ranking system.

        Retrieves the details for a given ranking system by name.

        Args:
            name: The name of the ranking system.

        Returns:
            An object containing the ranking system's details.

        Examples:
            >>> client.rank_get(name="My Ranking System")
            RankInfoResult(
                name='My Ranking System',
                id=12345,
                xml='<RankingSystem,...</RankingSystem>',
                currency='USD',
                rankingMethod=<RankingMethod.PERCENTILE_NA_NEGATIVE: 2>,
                type='Stock',
                groupUid=100,
                resolveGroupUid=200
            )
        """
        ...

    def rank_get(self, *, id: int | None = None, name: str | None = None) -> RankInfoResult:
        return self._req_with_auth_fallback(
            method="GET", url=self._endpoint + RANK_PATH, params=[("id", id), ("name", name)], result_type=RankInfoResult
        )

    def strategy(self, strategy_id: int):
        """
        Retrieves details for a specific strategy or book.

        Args:
            strategy_id (int): The ID of the strategy or book.

        Returns:
            A dictionary containing the strategy's details, including summary information,
            extensive performance and trading statistics, risk measurements, and daily
            performance data.

        >>> client.strategy(21293)
        {
            'summary': {
                'generalInfo': {
                    'name': 'Built for Stability',
                    'mktVal': 141178.94,
                    'cash': 4764.04,
                    'noPos': 31,
                    'lastTrades': 2,
                    'lastTraded': '2026-04-27',
                    'start': '2023-03-21',
                    'end': '2026-08-26',
                    'sizingMethod': 'Static Weight',
                    'rebalPeriod': 'Every Week',
                    'rebalMode': 'Automatic',
                    'nextRebal': '2026-05-04',
                    'rebalText': 'Past Due',
                    'benchmarkId': 43688,
                    'benchmark': 'S&P MidCap 400 Pure Growth ($MIDPG:USA)',
                    'universe': 'Easy to Trade Europe',
                    'rankingSystemId': 300819,
                    'rankingSystem': 'Stability'
                },
                'quickStats': {
                    'totalReturn': 41.18,
                    'benchReturn': 73.18,
                    'activeReturn': -32.0,
                    'annualizedReturn': 10.56,
                    'annualTurnover': 34.12,
                    'maxDrawdown': -10.09,
                    'benchMaxDrawdown': -26.65,
                    'overallWinners': 40,
                    'overallWinnersPct': 64.52,
                    'sharpeRatio': 0.4,
                    'benchCorrel': 0.42
                }
            },
            'stats': {
                'perf': {
                    'returnPct': {
                        'total': {'model': 41.18, 'bench': 73.18},
                        'annualized': {'model': 10.56, 'bench': 17.33},
                        'yearToDate': {'model': 8.24, 'bench': 17.81},
                        'monthToDate': {'model': 3.22, 'bench': 3.68},
                        'period4Week': {'model': 1.19, 'bench': 6.22},
                        'period13Week': {'model': 7.56, 'bench': -1.67},
                        'period1Year': {'model': 6.65, 'bench': 22.26},
                        'period3Year': {'model': 36.34, 'bench': 55.22}
                    },
                    'monthly': {
                        'period': ['2023-03-21', '2023-04-01', '2023-05-01', ...],
                        'model': [2.6, 2.45, -4.34, ...],
                        'bench': [2.43, -0.38, -2.5, ...]
                    },
                    'yearly': {
                        'period': ['2023-03-21', '2024-01-01', '2025-01-01', ...],
                        'model': [12.46, 11.23, 4.27, ...],
                        'bench': [15.34, 17.42, 8.54, ...]
                    },
                    'weekly': {
                        'period': ['2023-03-21', '2023-03-25', '2023-04-01', ...],
                        'model': [0.45, 2.14, 0.97, ...],
                        'bench': [-1.95, 4.46, -1.51, ...]
                    }
                },
                'trading': {
                    'parameters': {
                        'startingCapital': 100000.0,
                        'totalCashAdded': 0.0,
                        'endingMarketValue': 141178.94,
                        'startDate': '2023-03-21',
                        'endDate': '2026-08-27',
                        'daysSinceInception': 1254
                    },
                    'summary': {
                        'totalBuyShortTrades': 62,
                        'totalSellCoverTrades': 31,
                        'averageAnnualTurnover': 34.12,
                        'totalTradingCost': 1371.14,
                        'realizedWinners': 70.97,
                        'unrealizedWinners': 58.06,
                        'overallWinners': 64.52
                    },
                    'realized': {
                        'trades': {'all': 31, 'winners': 22, 'losers': 9},
                        'avgReturn': {'all': 14.05, 'winners': 24.33, 'losers': -11.08},
                        'avgDaysHeld': {'all': 332.58, 'winners': 334.41, 'losers': 328.11},
                        'totalAmount': {'all': 19536.34, 'winners': 23108.34, 'losers': -3572.01},
                        'biggestAmount': {'winners': 3744.6, 'losers': -840.28},
                        'biggestPct': {'winners': 100.74, 'losers': -23.96}
                    },
                    'unrealized': {
                        'trades': {'all': 31, 'winners': 18, 'losers': 13},
                        'avgReturn': {'all': 11.44, 'winners': 28.35, 'losers': -11.98},
                        'avgDaysHeld': {'all': 895.68, 'winners': 963.94, 'losers': 801.15},
                        'totalAmount': {'all': 11034.44, 'winners': 16848.05, 'losers': -5813.61},
                        'biggestAmount': {'winners': 3435.76, 'losers': -1181.39},
                        'biggestPct': {'winners': 108.6, 'losers': -34.52}
                    }
                },
                'riskMeasurements': {
                    'daily': {
                        'trailing3Year': {
                            'totalReturn': {'model': 36.02, 'bench': 54.18},
                            'annualizedReturn': {'model': 10.82, 'bench': 15.55},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 781.0, 'bench': 781.0},
                            'standardDeviation': {'model': 10.93, 'bench': 20.28},
                            'sharpeRatio': {'model': 0.68, 'bench': 0.63},
                            'sortinoRatio': {'model': 0.94, 'bench': 0.9},
                            'benchCorrel': {'model': 0.29},
                            'rSquared': {'model': 0.08},
                            'beta': {'model': 0.15},
                            'alpha': {'model': 5.61}
                        },
                        'sinceInception': {
                            'totalReturn': {'model': 41.18, 'bench': 73.18},
                            'annualizedReturn': {'model': 10.57, 'bench': 17.35},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 895.0, 'bench': 895.0},
                            'standardDeviation': {'model': 10.62, 'bench': 19.65},
                            'sharpeRatio': {'model': 0.67, 'bench': 0.72},
                            'sortinoRatio': {'model': 0.93, 'bench': 1.02},
                            'benchCorrel': {'model': 0.31},
                            'rSquared': {'model': 0.1},
                            'beta': {'model': 0.17},
                            'alpha': {'model': 4.86}
                        }
                    },
                    'weekly': {
                        'trailing3Year': {
                            'totalReturn': {'model': 36.02, 'bench': 54.18},
                            'annualizedReturn': {'model': 10.82, 'bench': 15.55},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 155.0, 'bench': 155.0},
                            'standardDeviation': {'model': 10.25, 'bench': 19.05},
                            'sharpeRatio': {'model': 0.62, 'bench': 0.58},
                            'sortinoRatio': {'model': 0.85, 'bench': 0.82},
                            'benchCorrel': {'model': 0.4},
                            'rSquared': {'model': 0.16},
                            'beta': {'model': 0.22},
                            'alpha': {'model': 4.04}
                        },
                        'sinceInception': {
                            'totalReturn': {'model': 41.18, 'bench': 73.18},
                            'annualizedReturn': {'model': 10.57, 'bench': 17.35},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 178.0, 'bench': 178.0},
                            'standardDeviation': {'model': 10.14, 'bench': 18.39},
                            'sharpeRatio': {'model': 0.56, 'bench': 0.75},
                            'sortinoRatio': {'model': 0.77, 'bench': 1.06},
                            'benchCorrel': {'model': 0.42},
                            'rSquared': {'model': 0.17},
                            'beta': {'model': 0.23},
                            'alpha': {'model': 2.53}
                        }
                    },
                    'monthly': {
                        'trailing3Year': {
                            'totalReturn': {'model': 36.02, 'bench': 54.18},
                            'annualizedReturn': {'model': 10.82, 'bench': 15.55},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 35.0, 'bench': 35.0},
                            'standardDeviation': {'model': 11.96, 'bench': 18.81},
                            'sharpeRatio': {'model': 0.47, 'bench': 0.55},
                            'sortinoRatio': {'model': 0.64, 'bench': 0.77},
                            'benchCorrel': {'model': 0.4},
                            'rSquared': {'model': 0.16},
                            'beta': {'model': 0.25},
                            'alpha': {'model': 3.06}
                        },
                        'sinceInception': {
                            'totalReturn': {'model': 41.18, 'bench': 73.18},
                            'annualizedReturn': {'model': 10.57, 'bench': 17.35},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 40.0, 'bench': 40.0},
                            'standardDeviation': {'model': 11.76, 'bench': 18.42},
                            'sharpeRatio': {'model': 0.4, 'bench': 0.64},
                            'sortinoRatio': {'model': 0.54, 'bench': 0.9},
                            'benchCorrel': {'model': 0.42},
                            'rSquared': {'model': 0.18},
                            'beta': {'model': 0.27},
                            'alpha': {'model': 1.51}
                        }
                    },
                    'yearly': {
                        'sinceInception': {
                            'totalReturn': {'model': 41.18, 'bench': 73.18},
                            'annualizedReturn': {'model': 10.57, 'bench': 17.35},
                            'maxDrawdown': {'model': -10.09, 'bench': -26.65},
                            'periodSamples': {'model': 2.0, 'bench': 2.0},
                            'standardDeviation': {'model': 4.92, 'bench': 6.28},
                            'sharpeRatio': {'model': 0.61, 'bench': 1.31},
                            'sortinoRatio': {'model': 0.86, 'bench': 1.85},
                            'benchCorrel': {'model': 1.0},
                            'rSquared': {'model': 1.0},
                            'beta': {'model': 0.76},
                            'alpha': {'model': -3.23}
                        }
                    }
                }
            },
            'dailyPerf': {
                'date': ['2023-03-21', '2023-03-22', '2023-03-23', ...],
                'cash': [100000.0, 41605.29, 41605.29, ...],
                'mktValLong': [0.0, 58091.01, 57904.3, ...],
                'mktValShort': [0.0, 0.0, 0.0, ...],
                'mktValHedge': [0.0, 0.0, 0.0, ...],
                'cashAdded': [100000.0, 0.0, 0.0, ...],
                'totalEquity': [100000.0, 99696.3, 99509.59, ...],
                'accruedDiv': [0.0, 0.0, 67.68, ...],
                'leverageRatio': [0.0, 0.58, 0.58, ...],
                'posCnt': [0, 18, 18, ...],
                'bench': [18097.43, 17684.48, 17626.62, ...],
                'ret': [100.0, 99.7, 99.58, ...],
                'retBench': [100.0, 97.72, 97.4, ...]
            }
        }
        """

        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STRATEGY_DETAILS_PATH.substitute(id=strategy_id))

    def strategy_copy(self, id: int, name: str, type: Literal["PTF", "SIM"] | None = None) -> IdResult:
        """
        Copy an existing strategy to a new strategy.

        Copies a live or simulated strategy to a new live or simulated strategy. Copied live strategies are set to manual rebalance.

        Args:
            id (int): Existing strategy ID.
            name (str): Name for the new strategy.
            type (Literal["PTF", "SIM"] | None): Type of strategy to create. Use "PTF" for a live strategy or "SIM" for simulated strategy.

        Returns:
            An object containing the new strategy's id.

        Examples:
            >>> client.strategy_copy(12345, "Sim copy", "SIM")
            IdResult(id=12345)
        """
        return self._req_with_auth_fallback(
            method="POST",
            url=self._endpoint + STRATEGY_COPY_PATH.substitute(id=id),
            json={"name": name, "type": type},
            result_type=IdResult,
        )

    def book_copy(self, id: int, name: str, type: Literal["BOOK", "BOOKSIM"] | None = None) -> IdResult:
        """
        Copy an existing book to a new book.

        Copies a live or simulated book to a new live or simulated book. Copied live books are set to manual rebalance.

        Args:
            id (int): Existing book ID.
            name (str): Name for the new book.
            type (Literal["BOOK", "BOOKSIM"] | None): Type of book to create. Use "BOOK" for a live book or "BOOKSIM" for simulated book.

        Returns:
            An object containing the new book's id.

        Examples:
            >>> client.book_copy(12344, "Sim book copy", "BOOKSIM")
            IdResult(id=12344)
        """
        return self._req_with_auth_fallback(
            method="POST", url=self._endpoint + BOOK_COPY_PATH.substitute(id=id), json={"name": name, "type": type}, result_type=IdResult
        )

    def strategy_transactions(self, strategy_id: int, start: str, end: str, to_pandas=False):
        """
        Retrieves transactions for a specific strategy or book within a date range.

        Args:
            strategy_id (int): The ID of the strategy or book.
            start (str): Start date (yyyy-mm-dd).
            end (str): End date (yyyy-mm-dd).
            to_pandas (bool): If True, converts the transaction results into a pandas DataFrame. Defaults to False.

        Returns:
            A dictionary containing the operation's cost, remaining quota, and a list of transaction details (or a DataFrame if to_pandas is True).

        Examples:
            >>> client.strategy_transactions(22124, "2026-06-25", "2026-06-25", to_pandas=False)
            {
                'trans': [
                    {
                        'tranDt': '2026-06-15',
                        'transId': 129,
                        'notes': '0.01x3220 shs pay 06/30/26',
                        'type': 'DIV',
                        'shares': 0.0,
                        'p123Uid': 54409,
                        'ticker': 'SACH:USA',
                        'amount': 32.2,
                        'settleDt': '2026-06-30'
                    },
                    ...
                ]
            }
        """

        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id), params=[("start", start), ("end", end)]
        )

        if to_pandas:
            import pandas

            return pandas.DataFrame(ret["trans"])

        return ret

    def strategy_transaction_import(
        self,
        strategy_id: int,
        data: str | IO[bytes],
        content_type: Literal["text/csv", "text/tsv"] = "text/csv",
        update_existing=False,
        make_rebal_dt_curr=False,
    ):
        """
        Imports transactions into a strategy.

        Supported formats are CSV and TSV. Expected columns (in order) are: date, ticker, type,
        shares, price, commission, and notes. Valid transaction types are BUY, SELL, COVER, SHORT,
        DIV, SPLIT, and CASH. The Preferred Country setting will be used to resolve tickers that
        do not have a country suffix. Dividends and splits are handled automatically unless
        overridden. Prices and commissions are assumed to be in the strategy's currency.

        Args:
            strategy_id (int): The ID of the strategy or book.
            data (str | IO[bytes]): The transaction data as a string or file-like object.
            content_type (str): The format of the data ('text/csv' or 'text/tsv'). Defaults to 'text/csv'.
            update_existing (bool): If True, updates existing transactions. Defaults to False.
            make_rebal_dt_curr (bool): If True, sets the rebalancing date to the current date. Defaults to False.

        Returns:
            A dictionary containing the operation's cost, remaining quota, and the number of processed transactions.

        Examples:
            >>> csv_data = "04/28/2025,IBM,BUY,100,123.45,10.0\\n04/25/2025,,CASH,,,123.45"
            >>> client.strategy_transaction_import(
            ...     strategy_id=31824,
            ...     data=csv_data,
            ...     content_type="text/csv",
            ...     update_existing=False,
            ...     make_rebal_dt_curr=False
            ... )
            {'processedTransactions': 34}
        """

        get_params = []
        if update_existing:
            get_params.append(("updateExisting", "1"))

        if make_rebal_dt_curr:
            get_params.append(("makeRebalDtCurr", "1"))

        return self._req_with_auth_fallback(
            url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id),
            params=get_params,
            data=data,
            headers={"Content-Type": content_type},
        )

    def strategy_transaction_delete(self, strategy_id: int, params: list[int]):
        """
        Deletes specific transactions from a strategy by their transaction IDs.

        Args:
            strategy_id (int): The ID of the strategy or book.
            params (list[int]): A list of transaction IDs to delete.

        Returns:
            A dictionary containing the operation's cost and remaining quota.

        Examples:
            >>> client.strategy_transaction_delete(10737, [10737, 23412])
            None
        """

        return self._req_with_auth_fallback(
            method="DELETE", url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id), json=params
        )

    def strategy_holdings(self, strategy_id: int, date: str | None = None, to_pandas=False):
        """
        Retrieves the historical holdings for a specific strategy as of a given date.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            date (str): The date to retrieve holdings for (yyyy-mm-dd). If None, defaults to the current date.
            to_pandas (bool): If True, converts the holdings list into a pandas DataFrame. Defaults to False.

        Returns:
            A dictionary containing a list of holdings details (or a DataFrame if to_pandas is True).

        Examples:
            >>> client.strategy_holdings(1073741824, "2026-06-25", to_pandas=False)
            {
                'holdings': [
                    {
                        'ticker': 'ABT:USA',
                        'name': 'Abbott Laboratories',
                        'sector': 'Healthcare',
                        'mktUid': 136,
                        'retPct': -4.17,
                        'shares': 42.0,
                        'avgShareCost': 119.0595,
                        'cost': 5000.5,
                        'currPrice': 114.1,
                        'value': 4792.2,
                        'daysHeld': 912,
                        'weight': 3.39,
                        'rank': 90.13
                    },
                    ...
                ]
            }
        """
        get_params = [("date", date)] if date is not None else []

        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + STRATEGY_HOLDINGS_PATH.substitute(id=strategy_id), params=get_params
        )

        if to_pandas:
            import pandas

            return pandas.DataFrame(ret["holdings"])

        return ret

    def strategy_trading_system(self, strategy_id: int):
        """
        Retrieves the trading system configuration for a specific strategy or book.

        Args:
            strategy_id (int): The ID of the strategy or book.

        Returns:
            A dictionary containing the trading system configuration details (such as capital, universe, ranking,
            rules, and rebalance settings).

        Examples:
            >>> client.strategy_trading_system(1414986)
            {
                'tradingSystem': {
                    'startingCapital': 100000.0,
                    'useMargin': False,
                    'universeUid': 0,
                    'universe': 'United States (Incl. Foreign Primary)',
                    'rankingSystemUid': 72883,
                    'rankingSystem': 'P123 Multi-market Rank',
                    'rankingMethod': 0,
                    'rebalance': {
                        'sizingMethod': 'STATIC_OLD',
                        'posWeight': 10.0,
                        'rebalFreq': "Every Week"
                    },
                    'buyRules': [
                        {
                            'name': 'Defensive Buss.',
                            'formula': 'Eval(sma(5,0,#SPEPSCY)>sma(21,0,#SPEPSCY) and close(0,#SPRP)>1, Price>=0, GICS(5510,3520,3510, 20201050))',
                            'disabled': False
                        },
                        ...
                    ],
                    'sellRules': [
                        {
                            'name': 'Sell Rule',
                            'formula': 'Eval((sma(5,0,#SPEPSCY)<sma(21,0,#SPEPSCY) or close(0,#SPRP)<1),Rank<=90 , Rank<=80)',
                            'disabled': False
                        },
                        ...
                    ]
                }
            }
        """

        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STRATEGY_TRADING_SYSTEM_PATH.substitute(id=strategy_id))

    def strategy_trading_system_update(self, strategy_id: int, params: dict) -> None:
        """
        Updates the trading system configuration for a live strategy.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the trading system update.
                Key arguments include::

                    - useMargin (bool): Whether to use margin.
                    - universe (int | str): Universe name or ID.
                    - rankingSystem (int | str): Ranking system name or ID.
                    - rankingMethod (int): Ranking method (0=Default, 2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution).
                    - buyRules (list[dict]): List of buy rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - sellRules (list[dict]): List of sell rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - rebalance (dict[str, Any]): Rebalance configuration. Must include 'sizingMethod' ('DYNAMIC', 'STATIC', or 'STATIC_OLD').
                        - For 'DYNAMIC': Includes 'numPos' (int), 'rebalFreq' (str), and 'reconFreq' (str).
                        - For 'STATIC' / 'STATIC_OLD': Includes 'posWeight' (float) and 'rebalFreq' (str).
                        - Allowed values for 'rebalFreq' and 'reconFreq': 'Every Week', 'Every 2 Weeks', 'Every 3 Weeks', 'Every 4 Weeks',
                          'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks', 'Every 26 Weeks', 'Every 52 Weeks'.

        Examples:
            >>> params = {
            ...     "useMargin": True,
            ...     "universe": "Prussell 2000",
            ...     "rankingSystem": 23132,
            ...     "rankingMethod": 2,
            ...     "buyRules": [
            ...         {
            ...             "name": "Diverse",
            ...             "formula": "SecWeight < 30",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "sellRules": [
            ...         {
            ...             "name": "main rank",
            ...             "formula": "rank < 70 and NoBars > 60",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "rebalance": {
            ...         "sizingMethod": "DYNAMIC",
            ...         "numPos": 30,
            ...         "rebalFreq": "Every Week",
            ...         "reconFreq": "Every Week"
            ...     }
            ... }
            >>> client.strategy_trading_system_update(42123, params)
            None
        """

        return self._req_with_auth_fallback(url=self._endpoint + STRATEGY_TRADING_SYSTEM_PATH.substitute(id=strategy_id), json=params)

    def book_trading_system_update(self, strategy_id: int, params: dict):
        """
        Updates the trading system configuration for a live strategy.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the trading system update.
                Key arguments include::

                    - useMargin (bool): Whether to use margin.
                    - universe (int | str): Universe name or ID.
                    - rankingSystem (int | str): Ranking system name or ID.
                    - rankingMethod (int): Ranking method (0=Default, 2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution).
                    - buyRules (list[dict]): List of buy rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - sellRules (list[dict]): List of sell rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - rebalance (dict[str, Any]): Rebalance configuration. Must include 'sizingMethod' ('DYNAMIC', 'STATIC', or 'STATIC_OLD').
                        - For 'DYNAMIC': Includes 'numPos' (int), 'rebalFreq' (str), and 'reconFreq' (str).
                        - For 'STATIC' / 'STATIC_OLD': Includes 'posWeight' (float) and 'rebalFreq' (str).
                        - Allowed values for 'rebalFreq' and 'reconFreq': 'Every Week', 'Every 2 Weeks', 'Every 3 Weeks', 'Every 4 Weeks',
                          'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks', 'Every 26 Weeks', 'Every 52 Weeks'.

        Examples:
            >>> params = {
            ...     "useMargin": True,
            ...     "universe": "Prussell 2000",
            ...     "rankingSystem": 2382,
            ...     "rankingMethod": 2,
            ...     "buyRules": [
            ...         {
            ...             "name": "Diverse",
            ...             "formula": "SecWeight < 30",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "sellRules": [
            ...         {
            ...             "name": "main rank",
            ...             "formula": "rank < 70 and NoBars > 60",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "rebalance": {
            ...         "sizingMethod": "DYNAMIC",
            ...         "numPos": 30,
            ...         "rebalFreq": "Every Week",
            ...         "reconFreq": "Every Week"
            ...     }
            ... }
            >>> client.strategy_trading_system_update(2293, params)
            None
        """

        return self._req_with_auth_fallback(url=self._endpoint + BOOK_TRADING_SYSTEM_PATH.substitute(id=strategy_id), json=params)

    def strategy_rerun(self, strategy_id: int, params: dict) -> None:
        """
        Reruns a simulated strategy.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the simulation rerun.
                Key arguments include::

                    - startDt (str): Required. Simulation start date (yyyy-mm-dd).
                    - endDt (str): Required. Simulation end date (yyyy-mm-dd).
                    - saveTrans (bool): Whether to save transactions.
                    - useMargin (bool): Whether to use margin.
                    - universe (int | str): Universe name or ID.
                    - rankingSystem (int | str): Ranking system name or ID.
                    - rankingMethod (int): Ranking method (0=Default, 2=Percentile NAs Negative, 4=Percentile NAs Neutral, 1=Normal Distribution).
                    - buyRules (list[dict]): List of buy rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - sellRules (list[dict]): List of sell rules, where each rule contains 'formula' (str, required), 'name' (str), and 'disabled' (bool).
                    - rebalance (dict[str, Any]): Rebalance configuration. Must include 'sizingMethod' ('DYNAMIC', 'STATIC', or 'STATIC_OLD').
                        - For 'DYNAMIC': Includes 'numPos' (int), 'rebalFreq' (str), and 'reconFreq' (str).
                        - For 'STATIC' / 'STATIC_OLD': Includes 'posWeight' (float) and 'rebalFreq' (str).
                        - Allowed values for 'rebalFreq' and 'reconFreq': 'Every Week', 'Every 2 Weeks', 'Every 3 Weeks', 'Every 4 Weeks',
                          'Every 6 Weeks', 'Every 8 Weeks', 'Every 13 Weeks', 'Every 26 Weeks', 'Every 52 Weeks'.

        Examples:
            >>> params = {
            ...     "useMargin": True,
            ...     "universe": "Prussell 2000",
            ...     "rankingSystem": 3421,
            ...     "rankingMethod": 2,
            ...     "buyRules": [
            ...         {
            ...             "name": "Diverse",
            ...             "formula": "SecWeight < 30",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "sellRules": [
            ...         {
            ...             "name": "main rank",
            ...             "formula": "rank < 70 and NoBars > 60",
            ...             "disabled": True
            ...         },
            ...         ...
            ...     ],
            ...     "rebalance": {
            ...         "sizingMethod": "DYNAMIC",
            ...         "numPos": 30,
            ...         "rebalFreq": "Every Week",
            ...         "reconFreq": "Every Week"
            ...     },
            ...     "startDt": "2020-01-01",
            ...     "endDt": "2026-06-25",
            ...     "saveTrans": True
            ... }
            >>> client.strategy_rerun(107374, params)
            None
        """

        return self._req_with_auth_fallback(url=self._endpoint + SIM_RERUN_PATH.substitute(id=strategy_id), json=params)

    def book_rerun(self, strategy_id: int, params: dict) -> None:
        """
        Reruns a simulated book.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the book simulation rerun.
                Key arguments include::

                    - startDt (str): Required. Simulation start date (yyyy-mm-dd).
                    - endDt (str): Required. Simulation end date (yyyy-mm-dd).
                    - assets (list[dict]): A list of assets included in the book. Each asset requires::
                        - itemUid (int): The unique identifier for the item.
                        - type (str): The asset type ('PTF' for live strategy, 'DM' for designer model,
                          'PRC' for stock or ETF, 'SIM' for simulated strategy).
                        - relativeWeight (float): The relative weight of the asset in the book.

        Examples:
            >>> params = {
            ...     "assets": [
            ...         {
            ...             "itemUid": 3221,
            ...             "type": "PTF",
            ...             "relativeWeight": 0.4
            ...         },
            ...         ...
            ...     ],
            ...     "startDt": "2020-01-01",
            ...     "endDt": "2026-06-25"
            ... }
            >>> client.book_rerun(32212, params)
            None
        """

        return self._req_with_auth_fallback(url=self._endpoint + BOOK_SIM_RERUN_PATH.substitute(id=strategy_id), json=params)

    def strategy_rebalance(self, strategy_id: int, params: dict):
        """
        Retrieves rebalance recommendations for a strategy.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the rebalance request.
                Key arguments include::

                    - pitMethod (str): Point-in-Time method override ('Prelim' or 'Complete').
                    - op (str): Rebalance operation for Dynamic Weight Live Strategies ('Rebal', 'Recon', or 'ReconRebal'). Assigned automatically by default based on the strategy's nextRebal and nextRecon dates.
                    - reject (list[int]): A list of P123 UIDs for which to suppress rebalance recommendations.
                    - figi (str): FIGI mapping ('Share Class' or 'Country Composite').
                    - minRebalTran (float): Override for the Minimum Rebalance Transaction (applicable for Live Book rebalances only).

        Returns:
            A dictionary containing the specific operation executed, asset ranks, and a list of rebalance recommendations
            detailing actions, shares, prices, and related metrics.

        Examples:
            >>> params = {
            ...     "pitMethod": "Prelim",
            ...     "op": "Rebal",
            ...     "reject": [231212],
            ...     "figi": "Share Class",
            ...     "minRebalTran": 0.1
            ... }
            >>> client.strategy_rebalance(1073741824, params)
            {
                'ranks': [
                    [1893, 74.16950988769531],
                    ...
                ],
                'recs': [
                    {
                        'ticker': 'MTVW:GBR',
                        'p123Uid': 87770,
                        'action': 'BUY',
                        'price': 116.9082,
                        'shares': 38.0,
                        'comm': 0.0,
                        'slip': 222.51
                    }
                ]
            }
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + STRATEGY_REBALANCE_PATH.substitute(id=strategy_id), json=params)

        return ret

    def strategy_rebalance_commit(self, strategy_id: int, params: dict) -> None:
        """
        Commits rebalance transactions for a strategy.

        Args:
            strategy_id (int): Required. The ID of the strategy or book.
            params (dict[str, Any]): A dictionary of parameters for the rebalance commit.
                Key arguments include::

                    - trans (list[dict]): Required. A list of rebalance transactions to commit. Each
                      transaction dictionary must contain 'p123Uid' (int), 'action' ('BUY', 'COVER', 'SELL', or 'SHORT'),
                      'price' (float), and 'shares' (float). Optional keys include 'comm' (float), 'slip' (float), and 'note' (str).
                    - op (str): Rebalance operation for Dynamic Weight Live Strategies ('Rebal', 'Recon', or 'ReconRebal').
                    - ranks (list[list]): Ranks included with the rebalance recommendations request (e.g., [[4737, 99.5], [774, 99.3]]).
                      Required for Live Strategy rebalances.

        Examples:
            >>> params = {
            ...     "op": "Rebal",
            ...     "ranks": [
            ...         [4737, 99.5],
            ...         [774, 99.3]
            ...     ],
            ...     "trans": [
            ...         {
            ...             "p123Uid": 31231,
            ...             "action": "BUY",
            ...             "price": 331,
            ...             "shares": 3,
            ...             "comm": 0.1,
            ...             "slip": 0.1,
            ...         }
            ...     ]
            ... }
            >>> client.strategy_rebalance_commit(12312, params)
            None
        """

        ret = self._req_with_auth_fallback(url=self._endpoint + STRATEGY_REBALANCE_COMMIT_PATH.substitute(id=strategy_id), json=params)

        return ret

    def stock_factor_upload(
        self,
        factor_id: int,
        data: str | IO[bytes],
        column_separator: Literal[",", ";", "\t"] = ",",
        existing_data: Literal["overwrite", "skip", "delete"] = "overwrite",
        date_format="yyyy-mm-dd",
        decimal_separator: Literal[".", ","] = ".",
        ignore_errors=False,
        ignore_duplicates=False,
    ):
        """
        Upload stock factor data.

        Uploads delimited content to the specified stock factor.

        The uploaded content must contain a header row.
        Only the first three columns are processed and must contain ``date``, ``<identifier>``, and ``value``.

        ``<identifier>`` may be one of ``id`` (Portfolio123 stock ID), ``ticker`` (Portfolio123 ticker), ``gvkey``, ``cik``, or ``figi``.

        The ``value`` column may specify ``na``, ``nan``, or ``null`` (case-insensitive) to clear a prior value on an observation date.

        Example input::

            date,ticker,value
            2026-01-31,AAPL:USA,1.25
            2026-01-31,MSFT:USA,0.83
            2026-02-28,MSFT:USA,na

        Note that some types of identifiers may resolve to multiple stocks.
        For example, the FIGI ``BBG001SG1LP6`` resolves to both ``UMC:USA`` and ``UMCB:DEU``.

        Identifier resolution is performed at the time of the upload. If identifier relationships ever change or coverage expands,
        the stock factor data will still reflect the original resolution.

        Args:
            factor_id (int): Unique identifier of the stock factor.
            data (str | IO[bytes]): Delimited content string or file-like containing delimited content. Must not exceed 100 MB or 5 million lines.
            column_separator (Literal[",", ";", "\t"]): Separator character between columns. Defaults to comma.
            existing_data (Literal["overwrite", "skip", "delete"]): Policy for dealing with collisions against stored dates. Defaults to ``overwrite``.
                Allowed values include 'overwrite' (overwrite stored values), 'skip' (retain stored values), or 'delete' (clear before storing uploaded data).
            date_format (str): Date format. Defaults to ``yyyy-mm-dd``.
            decimal_separator (Literal[".", ","]): Decimal separator. Defaults to period. If comma is used, the thousands separator, if used, is assumed to be period.
            ignore_errors (bool): If ``True``, lines in the data with errors will be silently discarded.
            ignore_duplicates (bool): If ``True``, additional occurrences of a (date, identifier) pair in the data are skipped.

        Example:
        >>> client.stock_factor_upload(
        ...     data=csv_data,
        ...     factor_id=4412,
        ...     column_separator=",",
        ...     existing_data="overwrite",
        ...     ignore_errors=True,
        ...     ignore_duplicates=True
        ... )
        None
        """

        # COMPAT: column_separator originally accepted 'comma', 'semicolon', 'tab' which matches the API.
        actual_column_separator: str = column_separator
        if column_separator == ",":
            actual_column_separator = "comma"
        elif column_separator == ";":
            actual_column_separator = "semicolon"
        elif column_separator == "\t":
            actual_column_separator = "tab"

        get_params = [
            ("columnSeparator", actual_column_separator),
            ("existingData", existing_data),
            ("dateFormat", date_format),
            ("decimalSeparator", decimal_separator),
            ("onError", "continue" if ignore_errors else "stop"),
            ("onDuplicates", "continue" if ignore_duplicates else "stop"),
        ]
        return self._req_with_auth_fallback(
            url=self._endpoint + STOCK_FACTOR_UPLOAD_PATH.substitute(id=factor_id), params=get_params, data=data
        )

    def stock_factor_create_update(self, params: dict) -> StockFactorResult:
        """
        Creates or updates a stock factor.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the create or update request.
                Expected keys include::

                    - id (int): The ID of the stock factor to update. Omit to create a new stock factor.
                    - name (str): Name of the stock factor (Required for creating a new stock factor).
                    - maxDays (int): Controls how long a factor is valid. If a factor value is older than Max Days from an observation date, it will be set to N/A.
                    - fhistRange (int): Controls how much extra data is loaded for the analysis both past and future. This is an advanced setting to support the use of the FHist() function. If you do not use FHist() with your factor, set it to 0 so that no extra data is loaded.

        Returns:
            An object containing the identifier for the stock factor operation.

        Examples:
            >>> params = {"name": "My Custom Factor", "maxDays": 5, "fhistRange": 0}
            >>> client.stock_factor_create_update(params=params)
            StockFactorResult(factorId=98765)
        """
        return self._req_with_auth_fallback(
            url=self._endpoint + STOCK_FACTOR_CREATE_UPDATE_PATH, json=params, result_type=StockFactorResult
        )

    def stock_factor_delete(self, factor_id: int) -> None:
        """
        Deletes a specific stock factor by its ID.

        Args:
            factor_id (int): The ID of the stock factor to delete.

        Examples:
            >>> client.stock_factor_delete(10737)
            None
        """
        return self._req_with_auth_fallback(url=self._endpoint + STOCK_FACTOR_DELETE_PATH.substitute(id=factor_id), method="DELETE")

    def data_series_upload(
        self,
        series_id: int,
        data: str | IO[bytes],
        existing_data: Literal["overwrite", "skip", "delete"] = "overwrite",
        date_format="yyyy-mm-dd",
        decimal_separator: Literal[".", ","] = ".",
        ignore_errors=False,
        ignore_duplicates=False,
        contains_header_row=True,
    ) -> None:
        """
        Upload data series data.

        Uploads delimited content to the specified data series.

        The data must contain dates in the first column and values in the second column.
        If the data includes a header row, the names are not processed.

        The ``value`` column may specify ``na``, ``nan``, or ``null`` (case-insensitive) to clear a prior value on an observation date.

        Example input::

            date,value
            2026-01-31,1.25
            2026-02-28,na

        Args:
            series_id (int): Unique identifier of the data series.
            data (str | IO[bytes]): Delimited content string or file-like containing delimited content. Must not exceed 100 MB.
            existing_data (Literal["overwrite", "skip", "delete"]): Policy for dealing with collisions against stored dates. Defaults to ``overwrite``.
                Allowed values include 'overwrite' (overwrite stored values), 'skip' (retain stored values), or 'delete' (clear before storing uploaded data).
            date_format (str): Date format. Defaults to ``yyyy-mm-dd``.
            decimal_separator (Literal[".", ","]): Decimal separator. Defaults to period. If comma is used, the thousands separator, if used, is assumed to be period.
            ignore_errors (bool): If ``True``, lines in the data with errors will be silently discarded.
            ignore_duplicates (bool): If ``True``, additional occurrences of a date in the data are skipped.
            contains_header_row (bool): If ``True``, the first line of the uploaded data will be skipped.

        Examples:
            >>> client.data_series_upload(
            ...     series_id=10737,
            ...     data=csv_data,
            ...     existing_data="overwrite",
            ...     contains_header_row=True
            ... )
            None
        """
        get_params = [
            ("existingData", existing_data),
            ("dateFormat", date_format),
            ("decimalSeparator", decimal_separator),
            ("onError", "continue" if ignore_errors else "stop"),
            ("onDuplicates", "continue" if ignore_duplicates else "stop"),
            ("headerRow", contains_header_row),
        ]
        return self._req_with_auth_fallback(
            url=self._endpoint + DATA_SERIES_UPLOAD_PATH.substitute(id=series_id), params=get_params, data=data
        )

    def data_series_create_update(self, params: dict) -> DataSeriesResult:
        """
        Creates a new data series or updates an existing one.

        Args:
            params (dict[str, Any]): A dictionary of parameters for the data series.
                Key arguments include::

                    - name (str): Required. Name of the data series.
                    - id (int): The ID of the data series to update. Omit this to create a new data series.
                    - description (str): Description of the data series.

        Returns:
            An object containing the data series ID.

        Examples:
            >>> params = {"id": 10737, "name": "Data Series", "description": "Data series description"}
            >>> client.data_series_create_update(params)
            DataSeriesResult(dataSeriesId=10737)
        """
        return self._req_with_auth_fallback(url=self._endpoint + DATA_SERIES_CREATE_UPDATE_PATH, json=params, result_type=DataSeriesResult)

    def data_series_delete(self, series_id: int) -> None:
        """
        Deletes a specific data series by its ID.

        Args:
            series_id (int): The ID of the data series to delete.

        Examples:
            >>> client.data_series_delete(10734)
            None
        """
        return self._req_with_auth_fallback(method="DELETE", url=self._endpoint + DATA_SERIES_DELETE_PATH.substitute(id=series_id))

    def get_api_id(self):
        return self._auth_params["apiId"]

    def aifactor_predict(self, predictor_id: int, params={}, to_pandas=False) -> dict[str, Any] | pandas.DataFrame:
        """
        Retrieves predictions for a trained AI Factor predictor.

        Args:
            predictor_id (int): The ID of the trained predictor.
            params (dict[str, Any]): A dictionary of parameters for the prediction request.
                Expected keys include::

                    - precision (int): Fixed precision digits (2 to 6). Defaults to 2.
                    - universe (int | str): Universe name or ID.
                    - asOfDt (str): As of date (yyyy-mm-dd).
                    - includeNames (bool): Whether to include company names.
                    - includeFeatures (bool): Whether to include features.
                    - figi (str): FIGI mapping ('Share Class' or 'Country Composite').
            to_pandas (bool): If True, converts the resulting arrays into a pandas DataFrame. Defaults to False.

        Returns:
            A dictionary containing the date and parallel arrays for P123 UIDs, tickers,
            and predictions (or a DataFrame if to_pandas is True).

        Examples:
            >>> params = {
            ...     "precision": 2,
            ...     "universe": "Prussell 2000",
            ...     "asOfDt": "2026-06-25",
            ...     "includeNames": True,
            ...     "includeFeatures": True,
            ...     "figi": "Share Class"
            ... }
            >>> client.aifactor_predict(217945, params, to_pandas=False)
            {
                'dt': '2024-10-04',
                'p123Uids': [774, 4737],
                'tickers': ['AAPL:USA', 'IBM:USA'],
                'predictions': [0.12, 0.34]
            }
        """

        ret = self._req_with_auth_fallback(url=self._endpoint + AIFACTOR_PREDICT_PATH.substitute(id=predictor_id), json=params)

        if to_pandas:
            import pandas

            data = {"p123Uid": ret["p123Uids"], "ticker": ret["tickers"]}
            if "names" in ret:
                data["name"] = ret["names"]
            if "figi" in ret:
                data["figi"] = ret["figi"]
            data["prediction"] = ret["predictions"]
            df = pandas.DataFrame(data)
            if "features" in ret:
                df = pandas.concat(
                    [
                        df,
                        pandas.DataFrame(ret["data"], columns=ret["features"]),
                        *((pandas.DataFrame(ret["rawData"], columns=["raw " + x for x in ret["features"]]),) if "rawData" in ret else ()),
                    ],
                    axis="columns",
                )
            ret = df

        return ret

    def stock_factor_download(self, factor_id: int):
        """
        Downloads data for a specific stock factor.

        Args:
            factor_id (int): The ID of the stock factor.

        Returns:
            A dictionary containing the parallel arrays of dates, tickers, values, and P123 UIDs representing the factor data.

        Examples:
            >>> client.stock_factor_download(23124)
            {
                'dates': ['2026-06-25', ...],
                'tickers': ['IBM', ...],
                'values': [0.1, ...],
                'p123Uids': [107374, ...]
            }
        """
        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STOCK_FACTOR_DOWNLOAD_PATH.substitute(id=factor_id))

    def data_prices(self, identifier: int | str, start: str, end: str | None, to_pandas=False):
        """
        Retrieves historical price data for a specific security by UID or ticker.

        Tickers without a country code default to ':USA' (e.g., 'MSFT' becomes 'MSFT:USA').
        Numeric identifiers are treated as P123 UIDs (e.g., '955' or '955:HKG').

        Args:
            identifier (int | str): Security identifier (UID or ticker with optional country).
            start (str): Start date (inclusive) in 'yyyy-mm-dd' format.
            end (str): End date (inclusive) in 'yyyy-mm-dd' format. If omitted or None, defaults to the current date.
            to_pandas (bool): If True, converts the prices list into a pandas DataFrame. Defaults to False.

        Returns:
            A dictionary containing the security information and a list of historical price records (or a DataFrame if to_pandas is True).

        Examples:
            >>> client.data_prices(identifier="MSFT", start="2025-01-03", end="2025-05-31")
            {
                'security': {
                    'p123Uid': 5881,
                    'ticker': 'MSFT:USA'
                },
                'prices': [
                    {
                        'date': '2025-01-03',
                        'open': 150.25,
                        'high': 152.3,
                        'low': 149.8,
                        'close': 152.35,
                        'vol': 25678900
                    },
                    {
                        'date': '2025-01-06',
                        'open': 152.5,
                        'high': 153.8,
                        'low': 151.2,
                        'close': 153.25,
                        'vol': 24135600
                    }
                ]
            }
        """
        get_params = [("start", start)]
        if end is not None:
            get_params.append(("end", end))
        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + DATA_PRICES_PATH.substitute(identifier=identifier), params=get_params
        )

        if to_pandas:
            import pandas

            return pandas.DataFrame(ret["prices"])

        return ret

    @overload
    def stock_factor_info(self, *, id: int) -> StockFactorInfoResult:
        """
        Retrieve basic stock factor info by ID.

        Args:
            id: Stock factor ID.

        Returns:
            An object containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(id=123)
            StockFactorInfoResult(factorId=123, name='Stock factor name')
        """
        ...

    @overload
    def stock_factor_info(self, *, name: str) -> StockFactorInfoResult:
        """
        Retrieve basic stock factor info by name.

        Args:
            name: Stock factor name.

        Returns:
            An object containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(name="Stock factor name")
            StockFactorInfoResult(factorId=123, name='Stock factor name')
        """
        ...

    @overload
    @deprecated("use overload accepting `id` parameter instead")
    def stock_factor_info(self, *, factor_id: int) -> StockFactorInfoResult:
        """
        Retrieve basic stock factor info by ID.

        Deprecated:
            Use overload accepting `id` parameter instead.

        Args:
            factor_id: Stock factor ID.

        Returns:
            An object containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(factor_id=123)
            StockFactorInfoResult(factorId=123, name='Stock factor name')
        """
        ...

    def stock_factor_info(self, *, id: int | None = None, factor_id: int | None = None, name: str | None = None) -> StockFactorInfoResult:
        if id is not None:
            params = [("id", id)]
        elif factor_id is not None:
            params = [("id", factor_id)]
        else:
            params = [("name", name)]
        return self._req_with_auth_fallback(
            method="GET", url=self._endpoint + STOCK_FACTOR_INFO_PATH, params=params, result_type=StockFactorInfoResult
        )

    @overload
    def data_series_info(self, *, id: int) -> DataSeriesInfoResult:
        """
        Retrieve basic data series info by ID.

        Args:
            id: Data series ID.

        Returns:
            An object containing the basic data series info.

        Examples:
            >>> client.data_series_info(id=123)
            DataSeriesInfoResult(dataSeriesId=123, name='Data series name')
        """
        ...

    @overload
    def data_series_info(self, *, name: str) -> DataSeriesInfoResult:
        """
        Retrieve basic data series info by name.

        Args:
            name: Data series name.

        Returns:
            An object containing the basic data series info.

        Examples:
            >>> client.data_series_info(name="Data series name")
            DataSeriesInfoResult(dataSeriesId=123, name='Data series name')
        """
        ...

    def data_series_info(self, *, id: int | None = None, name: str | None = None) -> DataSeriesInfoResult:
        return self._req_with_auth_fallback(
            method="GET",
            url=self._endpoint + DATA_SERIES_INFO_PATH,
            params=[("name", name)] if id is None else [("id", id)],
            result_type=DataSeriesInfoResult,
        )

    @overload
    def strategy_info(self, *, id: int) -> StrategyInfoResult:
        """
        Retrieve basic strategy info by ID.

        Args:
            id (int): Strategy ID.

        Returns:
            An object containing the basic strategy info.

        Examples:
            >>> client.strategy_info(id=123)
            StrategyInfoResult(strategyId=123, name='Strategy name')
        """
        ...

    @overload
    def strategy_info(self, *, name: str) -> StrategyInfoResult:
        """
        Retrieve basic strategy info by name.

        Args:
            name (str): Strategy name.

        Returns:
            An object containing the basic strategy info.

        Examples:
            >>> client.strategy_info(name="Strategy name")
            StrategyInfoResult(strategyId=123, name='Strategy name')
        """
        ...

    def strategy_info(self, *, id: int | None = None, name: str | None = None) -> StrategyInfoResult:
        return self._req_with_auth_fallback(
            method="GET",
            url=self._endpoint + STRATEGY_INFO_PATH,
            params=[("name", name)] if id is None else [("id", id)],
            result_type=StrategyInfoResult,
        )


def req_with_retry(
    method: str,
    url,
    session: requests.Session,
    max_tries=5,
    data: Any = None,
    json: Any = None,
    params: Any = None,
    verify: Any = None,
    timeout: Any = None,
    headers: Any = None,
):
    tries = 0
    pos = None
    match data:
        case str() | bytes() | None:
            pass
        case RawIOBase():
            if data.seekable():
                pos = data.tell()
            else:
                data = data.read()
        case BufferedIOBase():
            if data.seekable():
                pos = data.tell()
            else:
                chunks = []
                while chunk := data.read():
                    chunks.append(chunk)
                data = b"".join(chunks)
        case _:
            data = data.read()

    while True:
        if tries > 0:
            time.sleep(2 * tries)

        try:

            if data and not isinstance(data, (str, bytes)):
                if isinstance(data, (BufferedIOBase, RawIOBase)):
                    try:
                        data.seek(0)
                    except Exception:
                        if isinstance(data, RawIOBase):
                            data = data.readall()
                        else:
                            data = data.read()
                else:
                    data = data.read()

            resp = session.request(
                method=method, url=url, data=data, json=json, params=params, verify=verify, timeout=timeout, headers=headers
            )
            exception = None
        except requests.ConnectionError as e:
            resp = None
            exception = e

        if resp is not None:
            if resp.status_code < 500:
                return resp
            resp.close()
        tries += 1
        if tries >= max_tries:
            break
        if pos is not None:
            cast(IOBase, data).seek(pos)
    raise ClientException("Cannot connect to API") from exception
