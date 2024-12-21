import json
from modules.pinnacleapi.pinnacle_api import PinnacleAPI


class PinMarket(PinnacleAPI):
    async def get_sports(self):
        url = self._url('v2/sports')
        return await self._query(url)

    async def get_fixtures(self, sportid=12, live=1, since=0):
        url = self._url('v1/fixtures',
                        {'sportId': str(sportid), 'isLive': str(live),
                         'since': since})
        return await self._query(url)

    async def get_events_odds(self, sportid=12, live=1, eventIds=None,
                              oddsFormat='Decimal', since=0):
        dataArray = ','.join(eventIds) if eventIds else None
        url = self._url('v1/odds',
                        {'sportId': str(sportid), 'isLive': str(live),
                         'eventIds': dataArray,
                         'oddsFormat': str(oddsFormat), 'since': since})

        result = await self._query(url)
        return result

    async def get_leagues(self, sportid):
        url = self._url('v2/leagues', {'sportId': str(sportid)})
        return await self._query(url)
