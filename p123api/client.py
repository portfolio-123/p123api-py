from collections import defaultdict
import requests
import time
import pandas
from string import Template


ENDPOINT = "https://api.portfolio123.com"
AUTH_PATH = "/auth"
SCREEN_ROLLING_BACKTEST_PATH = "/screen/rolling-backtest"
SCREEN_BACKTEST_PATH = "/screen/backtest"
SCREEN_RUN_PATH = "/screen/run"
UNIVERSE_PATH = "/universe"
RANK_PATH = "/rank"
DATA_PATH = "/data"
RANK_RANKS_PATH = "/rank/ranks"
RANK_PERF_PATH = "/rank/performance"
RANK_TOUCH_PATH = Template("/rank/$id/touch")
DATA_UNIVERSE_PATH = "/data/universe"
STRATEGY_UNIVERSE_PATH = Template("/strategy/$id")
STOCK_FACTOR_UPLOAD_PATH = Template("/stockFactor/upload/$id")
STOCK_FACTOR_CREATE_UPDATE_PATH = "/stockFactor"
STOCK_FACTOR_DELETE_PATH = Template("/stockFactor/$id")
DATA_SERIES_UPLOAD_PATH = Template("/dataSeries/upload/$id")
DATA_SERIES_CREATE_UPDATE_PATH = "/dataSeries"
DATA_SERIES_DELETE_PATH = Template("/dataSeries/$id")
AIFACTOR_PREDICT_PATH = Template("/aiFactor/predict/$id")


class ClientException(Exception):
    def __init__(self, message, *, resp=None, exception=None):
        super().__init__(message)
        self._resp = resp
        self._exception = exception

    def get_resp(self) -> requests.Response:
        return self._resp

    def get_cause(self) -> Exception:
        return self._exception


class Client(object):
    """
    class for interfacing with P123 API
    """

    def __init__(
        self, *, api_id, api_key, auth_extra={}, endpoint=ENDPOINT, verify_requests=True
    ):
        self._endpoint = endpoint
        self._verify_requests = verify_requests
        self._max_req_retries = 5
        self._timeout = 300
        self._token = None

        if not isinstance(api_id, str) or not api_id:
            raise ClientException("api_id needs to be a non empty str")
        if not isinstance(api_key, str) or not api_key:
            raise ClientException("api_key needs to be a non empty str")

        self._auth_params = {"apiId": api_id, "apiKey": api_key, **auth_extra}
        self._session = requests.Session()

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
        resp = req_with_retry(
            self._session.post,
            self._max_req_retries,
            url=self._endpoint + AUTH_PATH,
            json=self._auth_params,
            verify=self._verify_requests,
            timeout=30,
        )
        if resp.status_code == 200:
            self._token = resp.text
            self._session.headers.update({"Authorization": f"Bearer {resp.text}"})
        else:
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
        name: str,
        method: str = "POST",
        url: str,
        params=None,
        data=None,
        headers=None,
        stop: bool = False,
    ):
        """
        Request with authentication fallback, used by all requests (except authentication)
        :param name: request action
        :param method: request method
        :param url: request url
        :param params: request params
        :param data: request data
        :param headers: request headers
        :param stop: flag to stop infinite authentication recursion
        :return: request response object
        """
        resp = None
        if self._session.headers.get("Authorization") is not None:
            if method == "POST":
                resp = req_with_retry(
                    self._session.post,
                    self._max_req_retries,
                    url=url,
                    json=params,
                    verify=self._verify_requests,
                    timeout=self._timeout,
                    data=data,
                    headers=headers,
                )
            else:
                req_type = (
                    self._session.delete if method == "DELETE" else self._session.get
                )
                resp = req_with_retry(
                    req_type,
                    self._max_req_retries,
                    url=url,
                    verify=self._verify_requests,
                    timeout=self._timeout,
                    headers=headers,
                )
        if resp is None or resp.status_code == 403:
            if not stop:
                self.auth()
                return self._req_with_auth_fallback(
                    name=name,
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    stop=True,
                )
        elif resp.status_code == 200:
            return resp
        else:
            message = resp.text
            if not message and resp.status_code == 402:
                message = "request quota exhausted"
            if message:
                message = ": " + message
            raise ClientException(f"API request failed{message}", resp=resp)

    def screen_rolling_backtest(self, params: dict, to_pandas=False):
        """
        Screen rolling backtest
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name="screen rolling backtest",
            url=self._endpoint + SCREEN_ROLLING_BACKTEST_PATH,
            params=params,
        ).json()

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
        ret = self._req_with_auth_fallback(
            name="screen backtest",
            url=self._endpoint + SCREEN_BACKTEST_PATH,
            params=params,
        ).json()

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
            panda_results = pandas.DataFrame(
                data=rows, columns=ret["results"]["columns"]
            )

            columns = [
                "Date",
                "Screen Return",
                "Bench Return",
                "Turnover %",
                "Position Count",
            ]
            chart = ret["chart"]
            rows = []
            for idx, date in enumerate(chart["dates"]):
                rows.append(
                    [
                        date,
                        chart["screenReturns"][idx],
                        chart["benchReturns"][idx],
                        chart["turnoverPct"][idx],
                        chart["positionCnt"][idx],
                    ]
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
        ret = self._req_with_auth_fallback(
            name="screen backtest", url=self._endpoint + SCREEN_RUN_PATH, params=params
        ).json()

        if to_pandas:
            ret = pandas.DataFrame(data=ret["rows"], columns=ret["columns"])

        return ret

    def universe_update(self, params: dict):
        """
        Universe update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name="universe update", url=self._endpoint + UNIVERSE_PATH, params=params
        ).json()

    def rank_update(self, params: dict):
        """
        Ranking system update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name="ranking system update", url=self._endpoint + RANK_PATH, params=params
        ).json()

    def data(self, params: dict, to_pandas=False):
        """
        Data
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name="data", url=self._endpoint + DATA_PATH, params=params
        ).json()

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
        ret = self._req_with_auth_fallback(
            name="data universe", url=self._endpoint + DATA_UNIVERSE_PATH, params=params
        ).json()

        if to_pandas:
            raw_obj = ret
            names = params.get("names")
            f_indices = range(len(params["formulas"]))
            if params.get("asOfDt"):
                for formula_idx in f_indices:
                    name = (
                        names[formula_idx]
                        if names is not None
                        else f"formula{formula_idx + 1}"
                    )
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
                    data["dates"].extend(
                        dtObj["dt"] for _ in range(len(dtObj["p123Uids"]))
                    )
                    data["p123Uids"].extend(dtObj["p123Uids"])
                    data["tickers"].extend(dtObj["tickers"])
                    if includeNames:
                        data["names"].extend(dtObj["names"])
                    if includeFigi:
                        data["figi"].extend(dtObj["figi"])
                    for formula_idx in f_indices:
                        formulas[formula_idx].extend(dtObj["data"][formula_idx])
                for formula_idx in f_indices:
                    name = (
                        names[formula_idx]
                        if names is not None
                        else f"formula{formula_idx + 1}"
                    )
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
        ret = self._req_with_auth_fallback(
            name="ranking system ranks",
            url=self._endpoint + RANK_RANKS_PATH,
            params=params,
        ).json()

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
        return self._req_with_auth_fallback(
            name="ranking system performance",
            url=self._endpoint + RANK_PERF_PATH,
            params=params,
        ).json()

    def rank_touch(self, rank_id: int):
        """
        Rank touch
        :param rank_id:
        """
        self._req_with_auth_fallback(
            name="rank touch",
            method="POST",
            url=self._endpoint + RANK_TOUCH_PATH.substitute(id=rank_id),
        )

    def strategy(self, strategy_id: int):
        """
        Strategy details
        :param strategy_id:
        :return:
        """
        return self._req_with_auth_fallback(
            name="strategy details",
            method="GET",
            url=self._endpoint + STRATEGY_UNIVERSE_PATH.substitute(id=strategy_id),
        ).json()

    def stock_factor_upload(
        self,
        factor_id: int,
        file: str,
        column_separator: str = None,
        existing_data: str = None,
        date_format: str = None,
        decimal_separator: chr(1) = None,
        ignore_errors: bool = None,
        ignore_duplicates: bool = None,
    ):
        """
        Stock factor data upload
        :param factor_id:
        :param file:
        :param column_separator: comma, semicolon or tab
        :param existing_data: overwrite, skip or delete
        :param date_format: dd for day, mm for month and yyyy for year, any separator allowed (defaults to yyyy-mm-dd)
        :param decimal_separator: . or ,
        :param ignore_errors:
        :param ignore_duplicates:
        :return:
        """
        with open(file, "rb") as data:
            get_params = []
            if column_separator is not None:
                get_params.append(f"columnSeparator={column_separator}")
            if existing_data is not None:
                get_params.append(f"existingData={existing_data}")
            if date_format is not None:
                get_params.append(f"dateFormat={date_format}")
            if decimal_separator is not None:
                get_params.append(f"decimalSeparator={decimal_separator}")
            if ignore_errors is not None:
                get_params.append(
                    "onError={}".format("continue" if ignore_errors else "stop")
                )
            if ignore_duplicates is not None:
                get_params.append(
                    "onDuplicates={}".format(
                        "continue" if ignore_duplicates else "stop"
                    )
                )
            get_params = "?" + "&".join(get_params) if len(get_params) else ""
            return self._req_with_auth_fallback(
                name="stock factor data upload",
                url=self._endpoint
                + STOCK_FACTOR_UPLOAD_PATH.substitute(id=factor_id)
                + get_params,
                data=data,
            ).json()

    def stock_factor_create_update(self, params: dict):
        """
        Stock factor create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name="stock factor create/update",
            url=self._endpoint + STOCK_FACTOR_CREATE_UPDATE_PATH,
            params=params,
        ).json()

    def stock_factor_delete(self, factor_id: int):
        """
        Stock factor delete
        :param factor_id: id of the data stock factor to delete
        :return:
        """
        return self._req_with_auth_fallback(
            name="stock factor delete",
            method="DELETE",
            url=self._endpoint + STOCK_FACTOR_DELETE_PATH.substitute(id=factor_id),
        ).json()

    def data_series_upload(
        self,
        series_id: int,
        file: str,
        existing_data: str = None,
        date_format: str = None,
        decimal_separator: chr(1) = None,
        ignore_errors: bool = None,
        ignore_duplicates: bool = None,
        contains_header_row: bool = None,
    ):
        """
        Data series upload
        :param series_id:
        :param file:
        :param existing_data: overwrite, skip or delete
        :param date_format: dd for day, mm for month and yyyy for year, any separator allowed (defaults to yyyy-mm-dd)
        :param decimal_separator: . or ,
        :param ignore_errors:
        :param ignore_duplicates:
        :param contains_header_row:
        :return:
        """
        with open(file, "rb") as data:
            get_params = []
            if existing_data is not None:
                get_params.append(f"existingData={existing_data}")
            if date_format is not None:
                get_params.append(f"dateFormat={date_format}")
            if decimal_separator is not None:
                get_params.append(f"decimalSeparator={decimal_separator}")
            if ignore_errors is not None:
                get_params.append(
                    "onError={}".format("continue" if ignore_errors else "stop")
                )
            if ignore_duplicates is not None:
                get_params.append(
                    "onDuplicates={}".format(
                        "continue" if ignore_duplicates else "stop"
                    )
                )
            if contains_header_row is not None:
                get_params.append(f"headerRow={contains_header_row}")
            get_params = "?" + "&".join(get_params) if len(get_params) else ""
            return self._req_with_auth_fallback(
                name="data series upload",
                url=self._endpoint
                + DATA_SERIES_UPLOAD_PATH.substitute(id=series_id)
                + get_params,
                data=data,
            ).json()

    def data_series_create_update(self, params: dict):
        """
        Data series create/update
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name="data series create/update",
            url=self._endpoint + DATA_SERIES_CREATE_UPDATE_PATH,
            params=params,
        ).json()

    def data_series_delete(self, series_id: int):
        """
        Data series delete
        :param series_id: id of the data series to delete
        :return:
        """
        return self._req_with_auth_fallback(
            name="data series delete",
            method="DELETE",
            url=self._endpoint + DATA_SERIES_DELETE_PATH.substitute(id=series_id),
        ).json()

    def get_api_id(self):
        return self._auth_params["apiId"]

    def aifactor_predict(self, predictor_id: int, params={}, to_pandas=False):
        """
        AI Factor predict
        :param predictor_id:
        :param params:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name="AI Factor predict",
            url=self._endpoint + AIFACTOR_PREDICT_PATH.substitute(id=predictor_id),
            params=params,
        ).json()

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
                    (df, pandas.DataFrame(ret["data"], columns=ret["features"])),
                    axis="columns",
                )
            ret = df

        return ret


def req_with_retry(req, max_tries=None, **kwargs):
    tries = 0
    if max_tries is None:
        max_tries = 5
    resp = None
    while tries < max_tries:
        if tries > 0:
            time.sleep(2 * tries)
        try:
            resp = req(**kwargs)
            if resp.status_code < 500:
                break
        except requests.ConnectionError as e:
            if tries + 1 == max_tries:
                raise ClientException("Cannot connect to API", exception=e)
        tries += 1
    return resp
