import requests
import time
import pandas


ENDPOINT = 'https://api.portfolio123.com'
AUTH_PATH = '/auth'
SCREEN_ROLLING_BACKTEST_PATH = '/screen/rolling-backtest'
SCREEN_BACKTEST_PATH = '/screen/backtest'
SCREEN_RUN_PATH = '/screen/run'
UNIVERSE_PATH = '/universe'
RANK_PATH = '/rank'
DATA_PATH = '/data'
RANK_RANKS_PATH = '/rank/ranks'
RANK_PERF_PATH = '/rank/performance'
DATA_UNIVERSE_PATH = '/data/universe'


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

    def __init__(self, *, api_id, api_key):
        self._endpoint = ENDPOINT
        self._verify_requests = True
        self._max_req_retries = 5
        self._timeout = 300

        if not isinstance(api_id, str) or not api_id:
            raise ClientException('api_id needs to be a non empty str')
        if not isinstance(api_key, str) or not api_key:
            raise ClientException('api_key needs to be a non empty str')

        self._api_id = api_id
        self._api_key = api_key
        self._session = requests.Session()

    def set_endpoint(self, endpoint):
        self._endpoint = endpoint

    def enable_verify_requests(self):
        self._verify_requests = True

    def disable_verify_requests(self):
        self._verify_requests = False

    def set_max_request_retries(self, retries):
        if not isinstance(retries, int) or retries < 1 or retries > 10:
            raise ClientException('retries needs to be an int between 1 and 10')
        self._max_req_retries = retries

    def set_timeout(self, timeout):
        if not isinstance(timeout, int) or timeout < 1:
            raise ClientException('timeout needs to be an int greater than 0')
        self._timeout = timeout

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
            auth=(self._api_id, self._api_key),
            verify=self._verify_requests,
            timeout=30
        )
        if resp.status_code == 200:
            self._session.headers.update({'Authorization': f'Bearer {resp.text}'})
        else:
            if resp.status_code == 406:
                message = 'user account inactive'
            elif resp.status_code == 402:
                message = 'paying subscription required'
            elif resp.status_code == 401:
                message = 'invalid id/key combination or key inactive'
            elif resp.status_code == 400:
                message = 'invalid key'
            else:
                message = resp.text
            if message:
                message = ': ' + message
            raise ClientException(f'API authentication failed{message}', resp=resp)

    def _req_with_auth_fallback(self, *, name: str, url: str, params, stop: bool = False):
        """
        Request with authentication fallback, used by all requests (except authentication)
        :param name: request action
        :param url: request url
        :param params: request params
        :param stop: flag to stop infinite authentication recursion
        :return: request response object
        """
        resp = None
        if self._session.headers.get('Authorization') is not None:
            resp = req_with_retry(
                self._session.post,
                self._max_req_retries,
                url=url,
                json=params,
                verify=self._verify_requests,
                timeout=self._timeout
            )
        if resp is None or resp.status_code == 403:
            if not stop:
                self.auth()
                return self._req_with_auth_fallback(name=name, url=url, params=params, stop=True)
        elif resp.status_code == 200:
            return resp
        else:
            message = resp.text
            if not message and resp.status_code == 402:
                message = 'request quota exhausted'
            if message:
                message = ': ' + message
            raise ClientException(f'API request failed{message}', resp=resp)

    def screen_rolling_backtest(self, params: dict):
        """
        Screen rolling backtest
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='screen rolling backtest',
            url=self._endpoint + SCREEN_ROLLING_BACKTEST_PATH,
            params=params
        ).json()

    def screen_backtest(self, params: dict):
        """
        Screen backtest
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='screen backtest',
            url=self._endpoint + SCREEN_BACKTEST_PATH,
            params=params
        ).json()

    def screen_run(self, params: dict):
        """
        Screen run
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='screen backtest',
            url=self._endpoint + SCREEN_RUN_PATH,
            params=params
        ).json()

    def universe_update(self, params: dict):
        """
        API universe update
        :param params:
        :return:
        """
        self._req_with_auth_fallback(
            name='universe update',
            url=self._endpoint + UNIVERSE_PATH,
            params=params
        )

    def rank_update(self, params: dict):
        """
        API ranking system update
        :param params:
        :return:
        """
        self._req_with_auth_fallback(
            name='ranking system update',
            url=self._endpoint + RANK_PATH,
            params=params
        )

    def data(self, params: dict, to_pandas: bool = False):
        """
        Data
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + DATA_PATH,
            params=params
        ).json()

        if to_pandas:
            raw_obj = dict(ret)
            with_cusips = params.get('cusips') is not None
            with_name = params.get('includeNames')
            data = []
            for date_idx, date in enumerate(ret['dates']):
                for item_uid, item_data in ret['items'].items():
                    row = [date, item_uid, item_data['ticker']]
                    if with_cusips:
                        row.append(item_data['cusip'])
                    if with_name:
                        row.append(item_data['name'])
                    for formula_idx, formula in enumerate(params['formulas']):
                        row.append(item_data['series'][formula_idx][date_idx])
                    data.append(row)
            columns = ['date', 'p123Uid', 'ticker']
            if with_cusips:
                columns.append('cusip')
            if with_name:
                columns.append('name')
            for formula_idx, formula in enumerate(params['formulas']):
                columns.append(f'formula{formula_idx + 1}')
            ret = pandas.DataFrame(data=data, columns=columns)
            ret.attrs['raw_obj'] = raw_obj

        return ret

    def data_universe(self, params: dict, to_pandas: bool = False):
        """
        Universe data
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + DATA_UNIVERSE_PATH,
            params=params
        ).json()

        if to_pandas:
            raw_obj = dict(ret)
            for formula_idx, formula in enumerate(params['formulas']):
                ret[f'formula{formula_idx + 1}'] = ret['data'][formula_idx]
            del ret['quota'], ret['quotaRemaining'], ret['data']
            if ret.get('dt'):
                del ret['dt']
            ret = pandas.DataFrame(data=ret)
            ret.attrs['raw_obj'] = raw_obj

        return ret

    def rank_ranks(self, params: dict, to_pandas: bool = False):
        """
        Ranking system ranks
        :param params:
        :param to_pandas:
        :return:
        """
        ret = self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + RANK_RANKS_PATH,
            params=params
        ).json()

        if to_pandas:
            raw_obj = dict(ret)
            del ret['quota'], ret['quotaRemaining'], ret['dt']
            nodes = ret.get('nodes')
            if nodes is not None:
                for node_idx, node_name in enumerate(nodes['names']):
                    if node_idx > 0:
                        node_name = node_name + f" ({nodes['weights'][node_idx]}%)"
                        ret[node_name] = []
                        for idx, uid in enumerate(ret['p123Uids']):
                            ret[node_name].append(nodes['ranks'][idx][node_idx])
                del ret['nodes']
            additional_data = ret.get('additionalData')
            if additional_data is not None:
                for data_idx, data_name in enumerate(params['additionalData']):
                    data_name = f'formula{data_idx + 1}'
                    ret[data_name] = []
                    for idx, uid in enumerate(ret['p123Uids']):
                        ret[data_name].append(additional_data[idx][data_idx])
                del ret['additionalData']
            ret = pandas.DataFrame(data=ret)
            ret.attrs['raw_obj'] = raw_obj

        return ret

    def rank_perf(self, params: dict):
        """
        Ranking system performance
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + RANK_PERF_PATH,
            params=params
        ).json()

    def get_api_id(self):
        return self._api_id


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
        except Exception as e:
            if tries + 1 == max_tries:
                raise ClientException('Cannot connect to API', exception=e)
        tries += 1
    return resp
