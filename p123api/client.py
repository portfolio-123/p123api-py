import requests
import time


ENDPOINT = 'https://api.portfolio123.com:8443'
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

    def data(self, params: dict):
        """
        Data
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + DATA_PATH,
            params=params
        ).json()

    def data_universe(self, params: dict):
        """
        Universe data
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + DATA_UNIVERSE_PATH,
            params=params
        ).json()

    def rank_ranks(self, params: dict):
        """
        Ranking system ranks
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=self._endpoint + RANK_RANKS_PATH,
            params=params
        ).json()

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
