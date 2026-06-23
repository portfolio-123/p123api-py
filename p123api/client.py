from collections import defaultdict
from collections.abc import Callable
import requests
import time
import pandas
from string import Template
from typing import IO, Literal, overload
from typing_extensions import deprecated

from p123api.types import (
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
    def __init__(self, message, *, resp: requests.Response | None = None, exception: Exception | None = None):
        super().__init__(message)
        self._resp = resp
        self._exception = exception

    def get_resp(self):
        return self._resp

    def get_cause(self):
        return self._exception

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

    def __init__(self, *, api_id, api_key, auth_extra={}, endpoint=ENDPOINT, verify_requests=True):
        self._endpoint = endpoint
        self._verify_requests = verify_requests
        self._max_req_retries = 5
        self._timeout = 300
        self._token = None

        if not isinstance(api_id, str) or not api_id:
            raise ClientException("api_id needs to be a non-empty str")
        if not isinstance(api_key, str) or not api_key:
            raise ClientException("api_key needs to be a non-empty str")

        self._auth_params = {"apiId": api_id, "apiKey": api_key, **auth_extra}
        self._session = requests.Session()
        self._method_map = {"GET": self._session.get, "POST": self._session.post, "DELETE": self._session.delete}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._session.close()

    def set_max_request_retries(self, retries):
        if not isinstance(retries, int) or retries < 1 or retries > 10:
            raise ClientException("retries needs to be an int between 1 and 10")
        self._max_req_retries = retries

    def set_timeout(self, timeout):
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
            self._session.post,
            self._max_req_retries,
            url=self._endpoint + AUTH_PATH,
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
        json=None,
        params=None,
        data=None,
        headers=None,
        result_type=None,
    ):
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
                self._method_map[method],
                self._max_req_retries,
                url=url,
                json=json,
                params=params,
                verify=self._verify_requests,
                timeout=self._timeout,
                data=data,
                headers=headers,
            ) as resp:

                if resp.status_code == 200:
                    json = resp.json()
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
        Screen rolling backtest
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_ROLLING_BACKTEST_PATH, json=params)

        if to_pandas:
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
        Screen backtest
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_BACKTEST_PATH, json=params)

        if to_pandas:
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
        Screen run
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + SCREEN_RUN_PATH, json=params)

        if to_pandas:
            ret = pandas.DataFrame(data=ret["rows"], columns=ret["columns"])

        return ret

    def universe_update(self, params: dict):
        """
        Universe update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + UNIVERSE_PATH, json=params)

    def rank_update(self, params: dict):
        """
        Ranking system update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + RANK_PATH, json=params)

    def data(self, params: dict, to_pandas=False):
        """
        Data
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + DATA_PATH, json=params)

        if to_pandas:
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

    def data_universe(self, params: dict, to_pandas=False):
        """
        Universe data
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + DATA_UNIVERSE_PATH, json=params)

        if to_pandas:
            raw_obj = ret
            names = params.get("names")
            f_indices = range(len(params["formulas"]))
            if params.get("asOfDt"):
                for formula_idx in f_indices:
                    name = names[formula_idx] if names is not None else f"formula{formula_idx + 1}"
                    ret[name] = ret["data"][formula_idx]
                del ret["dt"], ret["cost"], ret["quotaRemaining"], ret["data"]
                ret = pandas.DataFrame(ret)
            else:
                data = {"dates": [], "p123Uids": [], "tickers": []}
                includeNames = False
                if params.get("includeNames"):
                    data["names"] = []
                    includeNames = True
                includeFigi = False
                if params.get("figi"):
                    data["figi"] = []
                    includeFigi = True
                formulas = defaultdict(list)
                for dtObj in ret["dates"]:
                    data["dates"].extend(dtObj["dt"] for _ in range(len(dtObj["p123Uids"])))
                    data["p123Uids"].extend(dtObj["p123Uids"])
                    data["tickers"].extend(dtObj["tickers"])
                    if includeNames:
                        data["names"].extend(dtObj["names"])
                    if includeFigi:
                        data["figi"].extend(dtObj["figi"])
                    for formula_idx in f_indices:
                        formulas[formula_idx].extend(dtObj["data"][formula_idx])
                for formula_idx in f_indices:
                    name = names[formula_idx] if names is not None else f"formula{formula_idx + 1}"
                    data[name] = formulas[formula_idx]
                ret = pandas.DataFrame(data)
            ret.attrs["raw_obj"] = raw_obj

        return ret

    def rank_ranks(self, params: dict, to_pandas=False):
        """
        Ranking system ranks
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + RANK_RANKS_PATH, json=params)

        if to_pandas:
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
        Ranking system performance
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + RANK_PERF_PATH, json=params)

    def rank_touch(self, rank_id: int):
        """
        Rank touch
        :param rank_id:
        """
        self._req_with_auth_fallback(method="POST", url=self._endpoint + RANK_TOUCH_PATH.substitute(id=rank_id))

    def rank_create(
        self,
        name: str,
        nodes: str,
        *,
        rankingMethod=RankingMethod.PERCENTILE_NA_NEGATIVE,
        type: Literal["Stock", "ETF"] = "Stock",
        currency="USD",
    ) -> IdResult:
        """
        Creates a new ranking system.

        Creates a ranking system based on the provided nodes and configuration parameters.

        Args:
            name: Ranking system name.
            nodes: Ranking system nodes XML.
            rankingMethod: Ranking method to be used.
            type: Ranking method type. Use "Stock" or "ETF".
            currency: Ranking method currency (e.g., "USD").

        Returns:
            A dictionary containing the new ranking system's details.

        Examples:
            >>> client.rank_create(
            ...     'New Ranking System',
            ...     '<RankingSystem RankType="Higher">...</RankingSystem>',
            ...     rankingMethod=RankingMethod.PERCENTILE_NA_NEGATIVE,
            ...     type='Stock',
            ...     currency='USD'
            ... )
            {
                'id': 98765,
                'cost': 1,
                'quotaRemaining': 45678
            }
        """

        return self._req_with_auth_fallback(
            method="POST",
            url=self._endpoint + RANK_CREATE,
            json={"name": name, "nodes": nodes, "currency": currency, "type": type, "rankingMethod": rankingMethod},
        )

    @overload
    def rank_get(self, *, id: int) -> RankInfoResult:
        """
        Gets information for a specific ranking system.

        Retrieves the full configuration details for a given ranking system by its ID.

        Args:
            id: The unique identifier of the ranking system.

        Returns:
            A dictionary containing the ranking system's details.

        Examples:
            >>> client.rank_get(id=12345)
            {
                'name': 'My Ranking System',
                'id': 12345,
                'xml': '<RankingSystem>...</RankingSystem>',
                'currency': 'USD',
                'description': 'Ranking system description',
                'rankingMethod': 1,
                'type': 'Stock',
                'groupUid': 100,
                'resolveGroupUid': 200
            }
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
            A dictionary containing the ranking system's details.

        Examples:
            >>> client.rank_get(name='My Ranking System')
            {
                'name': 'My Ranking System',
                'id': 12345,
                'xml': '<RankingSystem>...</RankingSystem>',
                'currency': 'USD',
                'description': 'Ranking system description',
                'rankingMethod': 1,
                'type': 'Stock',
                'groupUid': 100,
                'resolveGroupUid': 200
            }
        """
        ...

    def rank_get(self, *, id: int | None = None, name: str | None = None) -> RankInfoResult:
        return self._req_with_auth_fallback(
            method="GET", url=self._endpoint + RANK_PATH, params={"id": id, "name": name}, result_type=RankInfoResult
        )

    def strategy(self, strategy_id: int):
        """
        Strategy details
        :param strategy_id:
        :return:
        """

        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STRATEGY_DETAILS_PATH.substitute(id=strategy_id))

    def strategy_copy(self, id: int, name: str, type: Literal["PTF", "SIM"]) -> IdResult:
        """
        Copy an existing strategy to a new strategy.

        Copies a live or simulated strategy to a new live or simulated strategy. Copied live strategies are set to manual rebalance.

        Args:
            id: Existing strategy ID.
            name: Name for the new strategy.
            type: Type of strategy to create. Use "PTF" for a live strategy or "SIM" for simulated strategy.

        Returns:
            A dictionary containing the new strategy's details.

        Examples:
            >>> client.strategy_copy(123, 'Sim copy', 'SIM')
            {
                'id': 12345,
                'cost': 1,
                'quotaRemaining': 45678
            }
        """
        return self._req_with_auth_fallback(
            method="POST",
            url=self._endpoint + STRATEGY_COPY_PATH.substitute(id=id),
            json={"name": name, "type": type},
            result_type=IdResult,
        )

    def book_copy(self, id: int, name: str, type: Literal["BOOK", "BOOKSIM"]) -> IdResult:
        """
        Copy an existing book to a new book.

        Copies a live or simulated book to a new live or simulated book. Copied live books are set to manual rebalance.

        Args:
            id: Existing book ID.
            name: Name for the new book.
            type: Type of book to create. Use "BOOK" for a live book or "BOOKSIM" for simulated book.

        Returns:
            A dictionary containing the new book's details.

        Examples:
            >>> client.book_copy(123, 'Sim book copy', 'BOOKSIM')
            {
                'id': 12345,
                'cost': 1,
                'quotaRemaining': 45678
            }
        """
        return self._req_with_auth_fallback(
            method="POST", url=self._endpoint + BOOK_COPY_PATH.substitute(id=id), json={"name": name, "type": type}, result_type=IdResult
        )

    def strategy_transactions(self, strategy_id: int, start: str, end: str, to_pandas=False):
        """
        Strategy transactions
        :param strategy_id:
        :param start: start date in YYYY-MM-DD format
        :param end: end date in YYYY-MM-DD format
        :return:
        """

        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id), params=[("start", start), ("end", end)]
        )
        return pandas.DataFrame(ret["trans"]) if to_pandas else ret

    def strategy_transaction_import(
        self,
        strategy_id: int,
        data: str | IO[str],
        content_type: Literal["text/csv", "text/tsv"] = "text/csv",
        update_existing=False,
        make_rebal_dt_curr=False,
    ):
        """
        Strategy transaction import
        :param strategy_id:
        :param file:
        :param update_existing: update existing transactions
        :param make_rebal_dt_curr: if True, the rebalancing date will be set to the current date
        :return:
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
        Strategy transaction delete
        :param strategy_id:
        :param trans_ids:
        :return:
        """
        return self._req_with_auth_fallback(
            method="DELETE", url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id), json=params
        )

    def strategy_holdings(self, strategy_id: int, date: str | None = None, to_pandas=False):
        """
        Strategy holdings
        :param strategy_id:
        :param date: date in YYYY-MM-DD format, if None, current date is used
        :return:
        """

        get_params = [("date", date)] if date is not None else []

        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + STRATEGY_HOLDINGS_PATH.substitute(id=strategy_id), params=get_params
        )

        return pandas.DataFrame(ret["holdings"]) if to_pandas else ret

    def strategy_trading_system(self, strategy_id: int):
        """
        Strategy trading system
        :param strategy_id:
        :return:
        """

        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STRATEGY_TRADING_SYSTEM_PATH.substitute(id=strategy_id))

    def strategy_trading_system_update(self, strategy_id: int, params: dict):
        """
        Live strategy trading system update
        :param strategy_id:
        :param params:
        :return:
        """

        return self._req_with_auth_fallback(url=self._endpoint + STRATEGY_TRADING_SYSTEM_PATH.substitute(id=strategy_id), json=params)

    def book_trading_system_update(self, strategy_id: int, params: dict):
        """
        Live book trading system update
        :param strategy_id:
        :param params:
        :return:
        """

        return self._req_with_auth_fallback(url=self._endpoint + BOOK_TRADING_SYSTEM_PATH.substitute(id=strategy_id), json=params)

    def strategy_rerun(self, strategy_id: int, params: dict):
        """
        Simulated strategy rerun
        :param strategy_id:
        :param params:
        :return:
        """

        return self._req_with_auth_fallback(url=self._endpoint + SIM_RERUN_PATH.substitute(id=strategy_id), json=params)

    def book_rerun(self, strategy_id: int, params: dict):
        """
        Simulated book rerun
        :param strategy_id:
        :param params:
        :return:
        """

        return self._req_with_auth_fallback(url=self._endpoint + BOOK_SIM_RERUN_PATH.substitute(id=strategy_id), json=params)

    def strategy_rebalance(self, strategy_id: int, params: dict):
        """
        Strategy rebalance
        :param strategy_id:
        :param params:
        :return:
        """

        ret = self._req_with_auth_fallback(url=self._endpoint + STRATEGY_REBALANCE_PATH.substitute(id=strategy_id), json=params)

        return ret

    def strategy_rebalance_commit(self, strategy_id: int, params: dict):
        """
        Strategy rebalance commit
        :param strategy_id:
        :param params:
        :return:
        """

        ret = self._req_with_auth_fallback(url=self._endpoint + STRATEGY_REBALANCE_COMMIT_PATH.substitute(id=strategy_id), json=params)

        return ret

    def stock_factor_upload(
        self,
        factor_id: int,
        data: str | IO[str],
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
            factor_id: Unique identifier of the stock factor.
            data: Delimited content string or file-like containing delimited content. Must not exceed 100 MB or 5 million lines.
            column_separator: Separator character between columns. Defaults to comma.
            existing_data: Policy for dealing with collisions against stored (date, stock ID) pairs. Defaults to ``overwrite``.
                - ``overwrite``: Overwrite stored values.
                - ``skip``: Retaine stored values.
                - ``delete``: Clear before storing uploaded data.
            date_format: Date format. Defaults to ``yyyy-mm-dd``.
            decimal_separator: Decimal separator. Defaults to period. If comma is used, the thousands separator, if used, is assumed to be period.
            ignore_errors: If ``True``, lines in the data with errors will be silently discarded.
            ignore_duplicates: If ``True``, additional occurrences of a (date, identifier) pair in the data are skipped.
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
        Stock factor create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            url=self._endpoint + STOCK_FACTOR_CREATE_UPDATE_PATH, json=params, result_type=StockFactorResult
        )

    def stock_factor_delete(self, factor_id: int):
        """
        Stock factor delete
        :param factor_id: id of the data stock factor to delete
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + STOCK_FACTOR_DELETE_PATH.substitute(id=factor_id), method="DELETE")

    def data_series_upload(
        self,
        series_id: int,
        data: str | IO[str],
        existing_data: Literal["overwrite", "skip", "delete"] = "overwrite",
        date_format="yyyy-mm-dd",
        decimal_separator: Literal[".", ","] = ".",
        ignore_errors=False,
        ignore_duplicates=False,
        contains_header_row=True,
    ):
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
            series_id: Unique identifier of the data series.
            data: Delimited content string or file-like containing delimited content. Must not exceed 100 MB.
            existing_data: Policy for dealing with collisions against stored dates. Defaults to ``overwrite``.
                - ``overwrite``: Overwrite stored values.
                - ``skip``: Retaine stored values.
                - ``delete``: Clear before storing uploaded data.
            date_format: Date format. Defaults to ``yyyy-mm-dd``.
            decimal_separator: Decimal separator. Defaults to period. If comma is used, the thousands separator, if used, is assumed to be period.
            ignore_errors: If ``True``, lines in the data with errors will be silently discarded.
            ignore_duplicates: If ``True``, additional occurrences of a date in the data are skipped.
            contains_header_row: If ``True``, the first line of the uploaded data will be skipped.
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
        Data series create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + DATA_SERIES_CREATE_UPDATE_PATH, json=params, result_type=DataSeriesResult)

    def data_series_delete(self, series_id: int):
        """
        Data series delete
        :param series_id: id of the data series to delete
        :return:
        """
        return self._req_with_auth_fallback(method="DELETE", url=self._endpoint + DATA_SERIES_DELETE_PATH.substitute(id=series_id))

    def get_api_id(self):
        return self._auth_params["apiId"]

    def aifactor_predict(self, predictor_id: int, params={}, to_pandas=False):
        """
        AI Factor predict
        :param predictor_id:
        :param params:
        :return:
        """
        ret = self._req_with_auth_fallback(url=self._endpoint + AIFACTOR_PREDICT_PATH.substitute(id=predictor_id), json=params)

        if to_pandas:
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
        Stock factor download
        :param factor_id:
        :return:
        """
        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STOCK_FACTOR_DOWNLOAD_PATH.substitute(id=factor_id))

    def data_prices(self, identifier: int | str, start: str, end: str | None, to_pandas=False):
        """ """
        get_params = [("start", start)]
        if end is not None:
            get_params.append(("end", end))
        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + DATA_PRICES_PATH.substitute(identifier=identifier), params=get_params
        )
        return pandas.DataFrame(ret["prices"]) if to_pandas else ret

    @overload
    def stock_factor_info(self, *, id: int) -> StockFactorInfoResult:
        """
        Retrieve basic stock factor info by ID.

        Args:
            id: Stock factor ID.

        Returns:
            A dictionary containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(id=123)
            {
                'factorId': 123,
                'name': 'Stock factor name',
                'description': 'Stock factor description'
            }
        """
        ...

    @overload
    def stock_factor_info(self, *, name: str) -> StockFactorInfoResult:
        """
        Retrieve basic stock factor info by name.

        Args:
            name: Stock factor name.

        Returns:
            A dictionary containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(name='Stock factor name')
            {
                'factorId': 123,
                'name': 'Stock factor name',
                'description': 'Stock factor description'
            }
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
            A dictionary containing the basic stock factor info.

        Examples:
            >>> client.stock_factor_info(factor_id=123)
            {
                'factorId': 123,
                'name': 'Stock factor name',
                'description': 'Stock factor description'
            }
        """
        ...

    def stock_factor_info(self, *, id: int | None = None, factor_id: int | None = None, name: str | None = None) -> StockFactorInfoResult:
        if id is not None:
            params = {"id": id}
        elif factor_id is not None:
            params = {"id": factor_id}
        else:
            params = {"name": name}
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
            A dictionary containing the basic data series info.

        Examples:
            >>> client.data_series_info(id=123)
            {
                'dataSeriesId': 123,
                'name': 'Data series name',
                'description': 'Data series description'
            }
        """
        ...

    @overload
    def data_series_info(self, *, name: str) -> DataSeriesInfoResult:
        """
        Retrieve basic data series info by name.

        Args:
            name: Data series name.

        Returns:
            A dictionary containing the basic data series info.

        Examples:
            >>> client.data_series_info(name='Data series name')
            {
                'dataSeriesId': 123,
                'name': 'Data series name',
                'description': 'Data series description'
            }
        """
        ...

    def data_series_info(self, *, id: int | None = None, name: str | None = None) -> DataSeriesInfoResult:
        return self._req_with_auth_fallback(
            method="GET",
            url=self._endpoint + DATA_SERIES_INFO_PATH,
            params={"name": name} if id is None else {"id": id},
            result_type=DataSeriesInfoResult,
        )

    @overload
    def strategy_info(self, *, id: int) -> StrategyInfoResult:
        """
        Retrieve basic strategy info by ID.

        Args:
            id: Strategy ID.

        Returns:
            A dictionary containing the basic strategy info.

        Examples:
            >>> client.strategy_info(id=123)
            {
                'strategyId': 123,
                'name': 'Strategy name',
                'description': 'Strategy description'
            }
        """
        ...

    @overload
    def strategy_info(self, *, name: str) -> StrategyInfoResult:
        """
        Retrieve basic strategy info by name.

        Args:
            name: Strategy name.

        Returns:
            A dictionary containing the basic strategy info.

        Examples:
            >>> client.strategy_info(name='Strategy name')
            {
                'strategyId': 123,
                'name': 'Strategy name',
                'description': 'Strategy description'
            }
        """
        ...

    def strategy_info(self, *, id: int | None = None, name: str | None = None) -> StrategyInfoResult:
        return self._req_with_auth_fallback(
            method="GET",
            url=self._endpoint + STRATEGY_INFO_PATH,
            params={"name": name} if id is None else {"id": id},
            result_type=StrategyInfoResult,
        )


def req_with_retry(req: Callable[..., requests.Response], max_tries=5, **kwargs):
    tries = 0
    while True:
        if tries > 0:
            time.sleep(2 * tries)
        try:
            resp = req(**kwargs)
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
    raise ClientException("Cannot connect to API", exception=exception)
