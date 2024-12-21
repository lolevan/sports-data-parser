import config
import json
import sys
import traceback

from aiohttp import ClientSession, BasicAuth


class PinnacleAPI:
    def __init__(self):
        self.url = 'https://api.pinnacle.com/'
        self.username = config.PINNCALE_USERNAME
        self.password = config.PINNCALE_PASSWORD
        self.proxy = config.PINNCALE_PROXY
        self.timeout = 5

    def _url(self, chunk: str, parameters={}):
        url = self.url + chunk + '?'
        if parameters != {}:
            for id, parametr in enumerate(parameters):
                if parameters[parametr] is None:
                    continue
                url = url + str(parametr) + '=' + str(
                    parameters[parametr]) + '&'
        print(url)
        return url

    async def _query(
            self, url: str, data: dict = {}, *, relogin: bool = False
    ):
        if not data:
            data = None

        try:
            async with ClientSession() as session:
                async with session.get(
                        url,
                        timeout=self.timeout,
                        data=data,
                        proxy=self._get_proxy(),
                        auth=self._get_auth(),
                        headers=self._get_headers()
                ) as response:
                    result = await response.text()
                    # print(result, file=open("pinnacle_response.txt", "a"))
                    # print(response.status, file=open("pinnacle_response2.txt", "a"))
                    try:
                        result = json.loads(result)
                    except Exception:
                        return []
                    return result
        except Exception as e:
            print(e)
            traceback.print_exc(file=sys.stdout)
            return []

    def _get_headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _get_auth(self):
        return BasicAuth(self.username, self.password)

    def _get_proxy(self):
        return None if self.proxy == None else "http://" + str(self.proxy)
