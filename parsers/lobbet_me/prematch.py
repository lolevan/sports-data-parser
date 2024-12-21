# prematch.py

import asyncio
import logging
import time
import random
import re
from typing import List, Dict
from datetime import datetime, timedelta
from aiohttp import ClientSession

BOOKIE = 'lobbet_me'
MAIN_URL = 'https://www.lobbet.me'

# Список прокси
PROXIES = [
    "http://ymuwnyuv:iryi7xvd3347@173.211.0.148:6641",
]

class PreMatchOddsParser:
    def __init__(self):
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.lock = asyncio.Lock()
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'text/plain;charset=UTF-8',
        }

    async def fetch(self, url: str, session: ClientSession,
                    proxy: str = None) -> Dict | List:
        try:
            async with session.get(url, proxy=proxy, timeout=10) as response:
                return await response.json()
        except Exception as e:
            logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
            return None

    async def fetch_with_retry(self, url: str, session: ClientSession,
                               max_retries: int = 3) -> Dict | List:
        for attempt in range(max_retries):
            proxy = random.choice(PROXIES)
            result = await self.fetch(url, session, proxy)
            if result is not None:
                return result
            logging.warning(
                f"Attempt {attempt + 1}/{max_retries} failed for URL {url}")
        logging.error(f"All {max_retries} attempts failed for URL {url}")
        return None

    async def get_all_leagues(self, session: ClientSession) -> List[str]:
        url = f'{MAIN_URL}/ibet/offer/sportsAndLeagues/-1.json?v=4.58.27&locale=en'
        all_sports = await self.fetch_with_retry(url, session)
        leagues_list = []
        if all_sports:
            for sport in all_sports:
                if sport['name'] in ['Soccer', 'Tennis']:
                    for league in sport['leagues']:
                        if league.get('active') and not league.get('blocked') and league.get('numOfMatches') > 0:
                            league_url = f"{MAIN_URL}/ibet/offer/league/{league.get('betLeagueId')}/-1/0/false.json?v=4.58.27&locale=en&ttgIds="
                            leagues_list.append(league_url)
        return leagues_list

    def convert_timestamp_to_lobbet_format(self, timestamp: int) -> str:
        timestamp_seconds = timestamp / 1000
        dt = datetime.fromtimestamp(timestamp_seconds)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    async def get_matches_from_league_data(self, league: Dict) -> List[Dict]:
        matches = []
        for match in league.get('matchList', []):
            kickoff_time = match.get('kickOffTime', 0)
            sport, country = self.get_sport_and_country(match)
            match_ = {
                'match_id': match.get('id'),
                'name': f'{match.get("home")} vs {match.get("away")}',
                'url': f'{MAIN_URL}/ibet-web-client/#/home/special/{match.get("id")}/S',
                'home_team': match.get("home"),
                'away_team': match.get("away"),
                'start_time': self.convert_timestamp_to_lobbet_format(kickoff_time),
                'league': match.get('leagueName'),
                'country': country.lower() if country else None,
                'sport': sport,
                'outcomes': []
            }
            # Only include matches starting within the next 24 hours
            now = datetime.now()
            match_time = datetime.fromisoformat(match_['start_time'])
            if now <= match_time <= now + timedelta(days=1):
                matches.append(match_)
        return matches

    async def get_matches(self, url: str, session: ClientSession) -> List[Dict]:
        league: Dict = await self.fetch_with_retry(url, session)
        if league:
            return await self.get_matches_from_league_data(league)
        return []

    async def get_events(self, session: ClientSession) -> List[Dict]:
        leagues_urls: List[str] = await self.get_all_leagues(session)
        tasks = [self.get_matches(league_url, session) for league_url in leagues_urls]
        responses = await asyncio.gather(*tasks)
        matches = [match for response in responses for match in response]
        return matches

    async def get_match_details(self, local_match_id: int,
                                session: ClientSession) -> List[Dict]:
        url = f"{MAIN_URL}/ibet/offer/special/undefined/{local_match_id}.json?v=4.58.27&locale=en"
        data = await self.fetch_with_retry(url, session)
        if data is None:
            return []
        markets = await self.get_markets(data)
        logging.info(f"Match {local_match_id} has {len(markets)} markets")
        return markets

    async def get_match_details2(self, local_match_id: int,
                                session: ClientSession) -> Dict:
        url = f"{MAIN_URL}/ibet/offer/special/undefined/{local_match_id}.json?v=4.58.27&locale=en"
        data = await self.fetch_with_retry(url, session)
        if data is None:
            return {}

        # Получаем основную информацию о матче
        match_info = {
            'match_id': local_match_id,
            'name': f"{data.get('home')} vs {data.get('away')}",
            'home_team': data.get('home'),
            'away_team': data.get('away'),
            'start_time': self.convert_timestamp_to_lobbet_format(
                data.get('kickOffTime')),
            'league': data.get('leagueName'),
            'country': self.get_sport_and_country(data)[1],
            'sport': self.get_sport_and_country(data)[0],
            'time': time.time()
        }

        # Получаем исходы (markets)
        markets = await self.get_markets(data)

        # Объединяем информацию о матче с исходами
        match_info['outcomes'] = markets

        return match_info

    async def get_markets(self, match_info: Dict) -> List[Dict]:
        bet_picks = match_info.get('odBetPickGroups')
        if bet_picks is None:
            return []
        markets = []
        for pick in bet_picks:
            handicap_param_value = pick.get('handicapParamValue')
            for bet in pick.get('tipTypes'):
                pick_name = pick.get('name').strip().lower()
                sport = match_info.get('sport')
                market = await self.convert_to_scanner_format(sport,
                                                              bet, pick_name,
                                                              handicap_param_value)
                if market and market not in markets:
                    markets.append(market)
        return markets

    async def convert_to_scanner_format(self, sport: str, bet: Dict, market_name: str,
                                        handicap_param_value: str = None) -> Dict | bool:
        if bet['value'] == 0:
            return False

        if sport == 'S':
            # Football
            return self.convert_to_scanner_format_football(bet, market_name,
                                                           handicap_param_value)

        if sport == 'T':
            # Tennis
            return self.convert_to_scanner_format_tennis(bet, market_name,
                                                         handicap_param_value)

        return False



    async def process_match(self, match: Dict, session: ClientSession,
                            semaphore: asyncio.Semaphore):
        async with semaphore:
            match_id = match['match_id']
            match['outcomes'] = await self.get_match_details(match_id, session)
            match['time'] = time.time()
            async with self.lock:
                self.ALL_MATCHES[match_id] = match
                self.parsed_matches[match_id] = match

    def get_sport_and_country(self, match: Dict):
        sport_data = match.get('sport')
        league_name = match.get('leagueName') or match.get('league')
        sport, country = "", ""
        if sport_data is not None:
            if sport_data == 'T':
                sport = 'Tennis'
                if league_name is not None:
                    country_match = re.search(r'\(([^)]+)\)', league_name)
                    if country_match:
                        country = country_match.group(1).strip()
                    else:
                        country = league_name.split(',')[0].strip()
            elif sport_data == 'S':
                sport = 'Football'
                country = league_name.split(',')[0].strip() if league_name else ''
            # Add more sports as needed
            else:
                sport = 'Unknown'
        return sport, country

    async def get_prematch_odds(self):
        semaphore = asyncio.Semaphore(30)  # Максимум 20 одновременных запросов

        async with ClientSession(headers=self.HEADERS) as session:
            matches = await self.get_events(session)
            tasks = [self.process_match(match, session, semaphore) for match in matches]
            await asyncio.gather(*tasks)
            logging.info(f"Total pre-match events: {len(matches)}")
        return self.parsed_matches


    def convert_to_scanner_format_football(self, bet: Dict, market_name: str,
                                           handicap_param_value: str = None) -> Dict | bool:

        if market_name == 'full time' and bet.get('tipType') in ['KI_1', 'KI_X',
                                                                 'KI_2']:
            return {
                "type_name": '1X2',
                "type": bet.get('caption'),
                "line": 0,
                "odds": float(bet.get('value'))
            }
        elif market_name == 'first half':
            return {
                "type_name": 'First Half 1X2',
                "type": '1H' + bet['name'].replace('Ih ', ''),
                "line": 0,
                "odds": float(bet.get('value'))
            }

        elif market_name == 'second half':
            return {
                "type_name": 'Second Half 1X2',
                "type": '2H' + bet['name'].replace('IIh ', ''),
                "line": 0,
                "odds": float(bet.get('value'))
            }
        elif market_name == 'total goals':

            if not bet['name'].startswith('tg'):
                return False
            type_ = 'O' if '+' in bet.get('name') else 'U'
            if '0-' in bet.get('name'):
                line = int(bet.get('name').split('0-')[1]) + 0.5
            elif '+' in bet.get('name'):
                line = int(bet.get('name').split('tg ')[1].split('+')[0]) - 0.5
            elif 'tg 0' == bet.get('name'):
                line = 0.5
            else:
                return False

            return {
                "type_name": 'Total',
                "type": type_,
                "line": line,
                "odds": float(bet.get('value'))
            }

        elif "total goals first half" in market_name:
            if not bet['name'].startswith('Ih'):
                return False
            type_ = '1HO' if '+' in bet.get('name') else '1HU'
            if '0-' in bet.get('name'):
                line = int(bet.get('name').split('0-')[1]) + 0.5
            elif '+' in bet.get('name'):
                line = int(bet.get('name').split('Ih ')[1].split('+')[0]) - 0.5
            elif 'Ih 0' == bet.get('name'):
                line = 0.5
            else:
                return False
            return {
                "type_name": 'First Half Total',
                "type": type_,
                "line": line,
                "odds": float(bet.get('value'))
            }

        elif market_name in ['home team total goals', 'away team total goals']:
            if 'tg team' not in bet['name']:
                return False
            splitter = 'tg team1 ' if market_name == 'home team total goals' else 'tg team2 '
            if '0-' in bet['name']:
                line = int(bet.get('name').split('0-')[1]) + 0.5
                type_ = 'THU' if market_name == 'home team total goals' else 'TAU'
            elif '+' in bet.get('name'):
                line = int(bet.get('name').split(splitter)[1].split('+')[0]) - 0.5
                type_ = 'THO' if market_name == 'home team total goals' else 'TAO'
            elif f'{splitter} 0' == bet.get('name'):
                line = 0.5
                type_ = 'THU' if market_name == 'home team total goals' else 'TAU'
            else:
                return False
            return {
                "type_name": 'Individual Total',
                "type": type_,
                "line": line,
                "odds": float(bet.get('value'))
            }
        elif market_name == 'handicap' or market_name == 'handicap b' or market_name == 'handicap c':
            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0

            if bet['tipType'] in ['H_1', 'H21', 'H31']:
                return {
                    "type_name": 'Asian Handicap',
                    "type": "AH1",
                    "line": handicap_value - 0.5,
                    "odds": float(bet['value'])
                }
            elif bet['tipType'] in ['H_2', 'H22', 'H32']:
                return {
                    "type_name": 'Asian Handicap',
                    "type": "AH2",
                    "line": (-handicap_value) - 0.5,
                    "odds": float(bet['value'])
                }
            else:
                return False

        elif market_name == 'handicap first half':
            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0
            if bet['tipType'] in ['PH_1']:
                return {
                    "type_name": 'First Half Asian Handicap',
                    "type": "1HAH1",
                    "line": handicap_value - 0.5,
                    "odds": float(bet['value'])
                }
            elif bet['tipType'] in ['PH_2']:
                return {
                    "type_name": 'First Half Asian Handicap',
                    "type": "1HAH2",
                    "line": (-handicap_value) - 0.5,
                    "odds": float(bet['value'])
                }
            else:
                return False

        elif "total goals second half" in market_name:
            if not bet['name'].startswith('IIh'):
                return False
            type_ = '2HO' if '+' in bet.get('name') else '2HU'
            if '0-' in bet.get('name'):
                line = int(bet.get('name').split('0-')[1]) + 0.5
            elif '+' in bet.get('name'):
                line = int(bet.get('name').split('IIh ')[1].split('+')[0]) - 0.5
            elif 'IIh 0' == bet.get('name'):
                line = 0.5
            else:
                return False
            return {
                "type_name": 'Second Half Total',
                "type": type_,
                "line": line,
                "odds": float(bet.get('value'))
            }

        else:
            return False

    def convert_to_scanner_format_tennis(self, bet: Dict, market_name: str,
                                         handicap_param_value: str = None) -> Dict | bool:

        if market_name == 'final outcome' and bet.get('tipType') in ['KI_1', 'KI_X',
                                                                     'KI_2']:
            return {
                "type_name": '1X2',
                "type": bet.get('caption'),
                "line": 0,
                "odds": float(bet.get('value'))
            }

        elif market_name == 'hendicap in sets':
            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0

            if bet['tipType'] in ['HS_1']:
                return {
                    "type_name": 'Asian Handicap in sets',
                    "type": "AH1",
                    "line": handicap_value,
                    "odds": float(bet.get('value'))
                }
            elif bet['tipType'] in ['HS_2']:
                return {
                    "type_name": 'Asian Handicap in sets',
                    "type": "AH2",
                    "line": (-handicap_value),
                    "odds": float(bet.get('value'))
                }
            else:
                return False

        elif market_name == 'hendicap in games':
            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0

            if bet['tipType'] in ['GH_1']:
                return {
                    "type_name": 'Asian Handicap in games',
                    "type": "GAH1",
                    "line": handicap_value,
                    "odds": float(bet.get('value'))
                }
            elif bet['tipType'] in ['GH_2']:
                return {
                    "type_name": 'Asian Handicap in games',
                    "type": "GAH2",
                    "line": (-handicap_value),
                    "odds": float(bet.get('value'))
                }
            else:
                return False

        elif (market_name in ['first set', 'second set', 'third set', 'iv set', 'v set']
              and bet.get('tipType') in ['S1_1', 'S1_2',
                                         'S2_1', 'S2_2',
                                         'S3_1', 'S3_2',
                                         'S4_1', 'S4_2',
                                         'S5_1', 'S5_2']):
            type_ = bet.get('tipType')[1] + 'H' + bet.get('tipType')[3]
            return {
                "type_name": f'{bet.get("tipType")[1]}' + 'H 1X2',
                "type": type_,
                "line": 0,
                "odds": float(bet.get('value'))
            }

        elif market_name == 'total games match':
            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0
            if not bet['name'].startswith('tg'):
                return False
            type_ = 'GO' if '>' in bet.get('name') else 'GU'

            return {
                "type_name": 'Total (games)',
                "type": type_,
                "line": handicap_value,
                "odds": float(bet.get('value'))
            }

        elif (market_name in ['first set games alternative',
                              'first set total games',
                              'second set games',
                              'third set games',
                              'iv set games',
                              'v set games']
              and (bet.get('tipType') in ['GGP_MINUS', 'GGP_PLUS',
                                          'G_S2_UNDER', 'G_S2_OVER',
                                          'G_S3_UNDER', 'G_S3_OVER',
                                          'G_S4_UNDER', 'G_S4_OVER',
                                          'G_S5_UNDER', 'G_S5_OVER']
                   or 'GGP_UNDER' in bet.get('tipType') or 'GGP_OVER' in bet.get('tipType'))):

            bet_name = bet.get('name')
            if 'sg' not in bet_name:
                return False

            if bet_name.startswith('Isg'):
                period = '1H'
            elif bet_name.startswith('IIsg'):
                period = '2H'
            elif bet_name.startswith('IIIsg'):
                period = '3H'
            elif bet_name.startswith('IVsg'):
                period = '4H'
            elif bet_name.startswith('Vsg'):
                period = '5H'
            else:
                return False

            handicap_value = float(
                handicap_param_value) if handicap_param_value else 0

            return {
                "type_name": f'{period} Total (games in set)',
                "type": f'{period}GO' if '>' in bet.get('name') else f'{period}GU',
                "line": handicap_value,
                "odds": float(bet.get('value'))
            }

        else:
            return False
