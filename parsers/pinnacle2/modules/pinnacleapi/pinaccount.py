from modules.pinnacleapi.pinnacle_api import PinnacleAPI

class PinAccount(PinnacleAPI):
    async def check_token(self) -> str:
        balance = await self.get_balance()
        return 'availableBalance' in balance

    async def get_balance(self) -> str:
        url = self._url('v1/client/balance')
        return await self._query(url)
