from collections import defaultdict
import requests
import time
import pandas
from string import Template
from typing import IO, Callable, List, Literal, Optional, Union, overload
from typing_extensions import deprecated

from .types import  DataSeriesInfoResult, DataSeriesResult, IdResult, RankInfoResult, StockFactorInfoResult, StockFactorResult

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
    def __init__(self, message, *, resp: Union[requests.Response, None] = None, exception: Union[Exception, None] = None):
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
        self, *, method: Literal["GET", "POST", "DELETE"] = "POST", url: str, json=None, params=None, data=None, headers=None
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
                    return resp.json()

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
    
    def rank_create(self, name: str, nodes:str,*, rankingMethod: Optional[Literal[0, 2, 4, 1]] = None, type: Optional[Literal["Stock", "ETF"]] = None, currency: str) -> IdResult:
        """
        Creates Ranking System

        :param name: Rank name
        :param nodes: Rank nodes
        :param rankingMethod: Ranking method [0, 2, 4, 1]
        :param type: Ranking method type ["Stock", "ETF"]
        :return: rank_id:
        """
        data: dict ={"name":name, "nodes":nodes, "currency":currency} 

        if(type):
            data["type"] = type

        if(rankingMethod):
            data["rankingMethod"] = rankingMethod
        
        return self._req_with_auth_fallback(method="POST", url=self._endpoint + RANK_CREATE, json=data)

    def rank_get(self, id: Optional[int]=None, name:Optional[str] = None) -> RankInfoResult:
        """
        Gets Rank info 

        :param id: Rank Id
        :param name: Rank name
        :return: RankInfoResult object containing:
        - name (str)
        - id (int)
        - xml (str)
        - currency (str)
        - description (str)
        - rankingMethod (int)
        - type (Literal["Stock", "ETF"])
        - groupUid (int)
        - resolveGroupUid (int)
        """
        return self._req_with_auth_fallback(method="GET", url=self._endpoint + RANK_PATH, params={"id":id, "name":name})

    def strategy(self, strategy_id: int):
        """
        Strategy details
        :param strategy_id:
        :return:
        """

        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STRATEGY_DETAILS_PATH.substitute(id=strategy_id))

    def strategy_copy(self, id: int, name:str, type: Optional[Literal["PTF", "SIM"]]) -> IdResult:
        """
        Strategy copy

        :param id: Strategy Id
        :param name: name of the strategy copy
        :param type: type of the strategy copy ("PTF"|"SIM")
        :return: id
        """
        return self._req_with_auth_fallback(method="POST", url=self._endpoint + STRATEGY_COPY_PATH.substitute(id=id), json={"name":name, "type":type})

    def book_copy(self, id: int, name:str, type: Optional[Literal["BOOK", "BOOKSIM"]]) -> IdResult:
        """
        Book copy

        :param book_id:
        :param name: name of the book copy
        :param type: type of the book copy ("BOOK"|"BOOKSIM")
        :return: id
        """
        return self._req_with_auth_fallback(method="POST", url=self._endpoint + BOOK_COPY_PATH.substitute(id=id), json={"name":name, "type":type})

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
        data: Union[str, IO[str]],
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

    def strategy_transaction_delete(self, strategy_id: int, params: List[int]):
        """
        Strategy transaction delete
        :param strategy_id:
        :param trans_ids:
        :return:
        """
        return self._req_with_auth_fallback(
            method="DELETE", url=self._endpoint + STRATEGY_TRANS_PATH.substitute(id=strategy_id), json=params
        )

    def strategy_holdings(self, strategy_id: int, date: Optional[str] = None, to_pandas=False):
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
        data: Union[str, IO[str]],
        column_separator: Union[str, None] = None,
        existing_data: Union[str, None] = None,
        date_format: Union[str, None] = None,
        decimal_separator: Union[str, None] = None,
        ignore_errors: Union[bool, None] = None,
        ignore_duplicates: Union[bool, None] = None,
    ):
        """
        Stock factor data upload
        :param factor_id:
        :param data:
        :param column_separator: comma, semicolon or tab
        :param existing_data: overwrite, skip or delete
        :param date_format: dd for day, mm for month and yyyy for year, any separator allowed (defaults to yyyy-mm-dd)
        :param decimal_separator: . or ,
        :param ignore_errors:
        :param ignore_duplicates:
        :return:
        """
        get_params = []
        if column_separator is not None:
            get_params.append(("columnSeparator", column_separator))
        if existing_data is not None:
            get_params.append(("existingData", existing_data))
        if date_format is not None:
            get_params.append(("dateFormat", date_format))
        if decimal_separator is not None:
            get_params.append(("decimalSeparator", decimal_separator))
        if ignore_errors is not None:
            get_params.append(("onError", "continue" if ignore_errors else "stop"))
        if ignore_duplicates is not None:
            get_params.append(("onDuplicates", "continue" if ignore_duplicates else "stop"))
        return self._req_with_auth_fallback(
            url=self._endpoint + STOCK_FACTOR_UPLOAD_PATH.substitute(id=factor_id), params=get_params, data=data
        )

    def stock_factor_create_update(self, params: dict) -> StockFactorResult:
        """
        Stock factor create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + STOCK_FACTOR_CREATE_UPDATE_PATH, json=params)

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
        data: Union[str, IO[str]],
        existing_data: Union[str, None] = None,
        date_format: Union[str, None] = None,
        decimal_separator: Union[str, None] = None,
        ignore_errors: Union[bool, None] = None,
        ignore_duplicates: Union[bool, None] = None,
        contains_header_row: Union[bool, None] = None,
    ):
        """
        Data series upload
        :param series_id:
        :param data:
        :param existing_data: overwrite, skip or delete
        :param date_format: dd for day, mm for month and yyyy for year, any separator allowed (defaults to yyyy-mm-dd)
        :param decimal_separator: . or ,
        :param ignore_errors:
        :param ignore_duplicates:
        :param contains_header_row:
        :return:
        """
        get_params = []
        if existing_data is not None:
            get_params.append(("existingData", existing_data))
        if date_format is not None:
            get_params.append(("dateFormat", date_format))
        if decimal_separator is not None:
            get_params.append(("decimalSeparator", decimal_separator))
        if ignore_errors is not None:
            get_params.append(("onError", "continue" if ignore_errors else "stop"))
        if ignore_duplicates is not None:
            get_params.append(("onDuplicates", "continue" if ignore_duplicates else "stop"))
        if contains_header_row is not None:
            get_params.append(("headerRow", contains_header_row))
        return self._req_with_auth_fallback(
            url=self._endpoint + DATA_SERIES_UPLOAD_PATH.substitute(id=series_id), params=get_params, data=data
        )

    def data_series_create_update(self, params: dict) -> DataSeriesResult:
        """
        Data series create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(url=self._endpoint + DATA_SERIES_CREATE_UPDATE_PATH, json=params)

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

    def data_prices(self, identifier: Union[int, str], start: str, end: Optional[str], to_pandas=False):
        """ """
        get_params = [("start", start)]
        if end is not None:
            get_params.append(("end", end))
        ret = self._req_with_auth_fallback(
            method="GET", url=self._endpoint + DATA_PRICES_PATH.substitute(identifier=identifier), params=get_params
        )
        return pandas.DataFrame(ret["prices"]) if to_pandas else ret

    @overload
    def stock_factor_info(self, *, id: int) -> StockFactorInfoResult: ...
    @overload
    @deprecated("use overload accepting `id` parameter instead")
    def stock_factor_info(self, *, factor_id: int) -> StockFactorInfoResult: ...
    @overload
    def stock_factor_info(self, *, name: str) -> StockFactorInfoResult: ...
    def stock_factor_info(
        self, *, id: Optional[int] = None, factor_id: Optional[int] = None, name: Optional[str] = None
    ) -> StockFactorInfoResult:
        """
        Stock factor info, only specify factor_id or name
        """
        if id is not None:
            params = {"id": id}
        elif factor_id is not None:
            params = {"id": factor_id}
        else:
            params = {"name": name}
        return self._req_with_auth_fallback(method="GET", url=self._endpoint + STOCK_FACTOR_INFO_PATH, params=params)

    @overload
    def data_series_info(self, *, id: int) -> DataSeriesInfoResult: ...
    @overload
    def data_series_info(self, *, name: str) -> DataSeriesInfoResult: ...
    def data_series_info(self, *, id: Optional[int] = None, name: Optional[str] = None) -> DataSeriesInfoResult:
        """
        Data series info, only specify factor_id or name
        """
        return self._req_with_auth_fallback(
            method="GET", url=self._endpoint + DATA_SERIES_INFO_PATH, params={"name": name} if id is None else {"id": id}
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
