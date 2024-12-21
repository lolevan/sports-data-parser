# live.py

import asyncio
import logging
import time
import random
import re
from typing import List, Dict
from datetime import datetime
from aiohttp import ClientSession

BOOKIE = 'lobbet_me'
MAIN_URL = 'https://www.lobbet.me'

# Список прокси
PROXIES = [
    "http://ymuwnyuv:iryi7xvd3347@198.23.239.134:6540",
    "http://ymuwnyuv:iryi7xvd3347@207.244.217.165:6712",
    "http://ymuwnyuv:iryi7xvd3347@107.172.163.27:6543",
    "http://ymuwnyuv:iryi7xvd3347@64.137.42.112:5157",
    "http://ymuwnyuv:iryi7xvd3347@173.211.0.148:6641",
    "http://ymuwnyuv:iryi7xvd3347@161.123.152.115:6360",
    "http://ymuwnyuv:iryi7xvd3347@167.160.180.203:6754",
    "http://ymuwnyuv:iryi7xvd3347@154.36.110.199:6853",
    "http://ymuwnyuv:iryi7xvd3347@173.0.9.70:5653",
    "http://ymuwnyuv:iryi7xvd3347@173.0.9.209:5792",
]


class LiveOddsParser:
    def __init__(self):
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.lock = asyncio.Lock()
        self.HEADERS = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=utf-8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://www.lobbet.me/ibet-web-client/',
            'Origin': 'https://www.lobbet.me',
            'Connection': 'keep-alive',
        }

    async def fetch(self, url: str, session: ClientSession,
                    proxy: str = None, method='GET',
                    data=None) -> Dict | List:
        try:
            if method == 'GET':
                async with session.get(url, proxy=proxy, timeout=10,
                                       headers=self.HEADERS) as response:
                    return await response.json(content_type=None)
            elif method == 'POST':
                async with session.post(url, proxy=proxy, data=data,
                                        timeout=10,
                                        headers=self.HEADERS) as response:
                    return await response.json(content_type=None)
            else:
                logging.error(f"Unsupported method {method} for URL {url}")
                return None
        except Exception as e:
            logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
            return None

    async def fetch_with_retry(self, url: str, session: ClientSession,
                               max_retries: int = 3, method='GET',
                               data=None) -> Dict | List:
        for attempt in range(max_retries):
            proxy = random.choice(PROXIES)
            result = await self.fetch(url, session, proxy, method, data)
            if result is not None:
                return result
            logging.warning(
                f"Attempt {attempt + 1}/{max_retries} failed for URL {url}")
        logging.error(f"All {max_retries} attempts failed for URL {url}")
        return None

    async def fetch_live_odds(self, session: ClientSession) -> List[Dict]:
        url = f'{MAIN_URL}/ibet/async/live/multy/-1.json?locale=en&v=4.61.8.9'
        data = '{}'  # Отправляем '{}' как тело запроса
        data_bytes = data.encode('utf-8')
        data_length = len(data_bytes)
        self.HEADERS['Content-Length'] = str(data_length)
        response_data = await self.fetch_with_retry(url, session,
                                                    method='POST', data=data)
        # print(response_data, file=open('response_data.txt', 'w'))
        if response_data is None:
            return []
        all_matches = response_data.get('IMatchLiveContainer', {}).get(
            'matches', [])
        # Фильтруем матчи с 'phase' != 'NOT_STARTED' и вид спорта 'S'
        active_matches = [
            match for match in all_matches
            if match.get('phase') != 'NOT_STARTED' and match.get("sport") in [
                'S', 'T']  # and match.get('liveStatus') == 1
        ]

        print(active_matches, file=open('active_matches.txt', 'w'))
        return active_matches

    async def process_live_match(self, match: Dict, session: ClientSession,
                                 semaphore: asyncio.Semaphore):
        async with semaphore:
            match_id = match.get('id')
            home_team = match.get('home')
            away_team = match.get('away')
            kickoff_time = match.get('kickOffTime')
            start_time = self.convert_timestamp_to_lobbet_format(kickoff_time)
            league = match.get('leagueName')
            sport_code = match.get('sport')
            sport, country = self.get_sport_and_country(match)

            # Получаем текущий счет
            current_score = match.get('matchResult', {}).get('currentScore',
                                                             {})
            score_h = current_score.get('h') or 0
            score_a = current_score.get('a') or 0

            # Преобразуем в целые числа
            try:
                score_h = int(score_h)
                score_a = int(score_a)
            except (ValueError, TypeError):
                score_h = 0
                score_a = 0

            # Вычисляем разницу счета
            score_difference = score_h - score_a

            # Подготавливаем данные матча
            match_data = {
                'match_id': match_id,
                'name': f'{home_team} vs {away_team}',
                'url': f'{MAIN_URL}/ibet-web-client/#/home/special/{match_id}/S',
                'home_team': home_team,
                'away_team': away_team,
                'start_time': start_time,
                'league': league,
                'country': country.lower() if country else None,
                'sport': sport,
                'current_score': f'{score_h}-{score_a}' if score_h is not None and score_a is not None else None,
                'phase': match.get('phase'),
                'outcomes': [],
                'time': time.time()
            }

            # Обрабатываем ставки и пиков
            bets = match.get('bets', [])
            markets = []
            for bet in bets:
                picks = bet.get('picks', [])
                market_name = bet.get('liveBetCaption', '').strip().lower()
                handicap_param_value = bet.get('specialValue')
                for pick in picks:
                    market = await self.convert_live_pick_to_scanner_format(
                        sport_code, pick, market_name, handicap_param_value,
                        score_difference
                    )
                    if market:
                        markets.append(market)

            match_data['outcomes'] = markets
            match_data['time'] = time.time()

            async with self.lock:
                self.ALL_MATCHES[match_id] = match_data
                self.parsed_matches[match_id] = match_data

    async def convert_live_pick_to_scanner_format(self, sport_code: str,
                                                  pick: Dict,
                                                  market_name: str,
                                                  handicap_param_value: str = None,
                                                  score_difference: int = 0) -> Dict | bool:
        if pick['oddValue'] == 0:
            return False

        if sport_code == 'S':
            # Футбол
            return self.convert_live_pick_football(pick, market_name,
                                                   handicap_param_value,
                                                   score_difference)
        elif sport_code == 'T':
            # Теннис
            return self.convert_live_pick_tennis(pick, market_name,
                                                 handicap_param_value)
        # Добавьте другие виды спорта по необходимости
        else:
            return False

    def convert_live_pick_football(self, pick: Dict, market_name: str,
                                   handicap_param_value: str = None,
                                   score_difference: int = 0) -> Dict | bool:
        market_name = market_name.lower()
        pick_label = pick.get('liveBetPickLabel', '').lower()
        odd_value = pick.get('oddValue')
        special_value = pick.get('specialValue')

        # 1X2
        if market_name in ['full time', 'final result']:
            if 'ft 1' in pick_label:
                pick_type = '1'
            elif 'ft x' in pick_label:
                pick_type = 'X'
            elif 'ft 2' in pick_label:
                pick_type = '2'
            else:
                return False
            return {
                "type_name": '1X2',
                "type": pick_type,
                "line": 0,
                "odds": float(odd_value)
            }

        # Тоталы
        elif market_name in ['total goals live',
                             'total goals - without overtime']:
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'goals ft<' in pick_label:
                pick_type = 'U'
            elif 'goals ft>' in pick_label:
                pick_type = 'O'
            else:
                return False
            return {
                "type_name": 'Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Индивидуальные тоталы команды 1 (домашней)
        elif market_name == 'home team total goals' or market_name == 'home team total goals live':
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'team1goal tg<' in pick_label:
                pick_type = 'THU'
            elif 'team1goal tg>' in pick_label:
                pick_type = 'THO'
            else:
                return False
            return {
                "type_name": 'Individual Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Индивидуальные тоталы команды 2 (гостевой)
        elif market_name == 'away team total goals live' or market_name == 'away team total goals':
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'team2goal tg<' in pick_label:
                pick_type = 'TAU'
            elif 'team2goal tg>' in pick_label:
                pick_type = 'TAO'
            else:
                return False
            return {
                "type_name": 'Individual Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Азиатские гандикапы
        elif 'handicap' in market_name:
            if not handicap_param_value:
                return False
            try:
                line = float(handicap_param_value)
            except ValueError:
                return False

            # Определяем, какая команда (1 или 2)
            if 'h 1' in pick_label:
                pick_type = 'AH1'
                # Корректируем линию:
                # 1. Реверсируем для команды 1 (домашней) - не требуется, так как это первая команда
                # 2. Вычитаем 0.5 для преобразования европейского в азиатский
                # 3. Прибавляем разницу счета
                adjusted_line = line - 0.5 + score_difference
                absolute_line = line - 0.5
            elif 'h 2' in pick_label:
                pick_type = 'AH2'
                # Для команды 2 (гостевой):
                # 1. Реверсируем линию (меняем знак)
                # 2. Вычитаем 0.5 для преобразования
                # 3. Вычитаем разницу счета
                adjusted_line = -line - 0.5 - score_difference
                absolute_line = -line - 0.5
            else:
                return False

            return {
                "type_name": 'Asian Handicap',
                "type": pick_type,
                "line": adjusted_line,
                "odds": float(odd_value),
                "absolute_line": absolute_line
            }

        # 1X2 по таймам (Первый тайм)
        elif market_name == 'first half':
            if 'h 1' in pick_label:
                pick_type = '1H1'
            elif 'h x' in pick_label:
                pick_type = '1HX'
            elif 'h 2' in pick_label:
                pick_type = '1H2'
            else:
                return False
            return {
                "type_name": 'First Half 1X2',
                "type": pick_type,
                "line": 0,
                "odds": float(odd_value)
            }

        # Тоталы по таймам (Первый тайм)
        elif market_name == 'total goals first half' or market_name == 'total goals first half live':
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'goals ht<' in pick_label or 'h<' in pick_label:
                pick_type = '1HU'
            elif 'goals ht>' in pick_label or 'h>' in pick_label:
                pick_type = '1HO'
            else:
                return False
            return {
                "type_name": 'First Half Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # 1X2 по таймам (Второй тайм)
        elif market_name == 'second half':
            if 'h 1' in pick_label:
                pick_type = '2H1'
            elif 'h x' in pick_label:
                pick_type = '2HX'
            elif 'h 2' in pick_label:
                pick_type = '2H2'
            else:
                return False
            return {
                "type_name": 'Second Half 1X2',
                "type": pick_type,
                "line": 0,
                "odds": float(odd_value)
            }

        # Тоталы по таймам (Второй тайм)
        elif market_name == 'total goals second half' or market_name == 'total goals second half live':
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'goals st<' in pick_label or 'h<' in pick_label:
                pick_type = '2HU'
            elif 'goals st>' in pick_label or 'h>' in pick_label:
                pick_type = '2HO'
            else:
                return False
            return {
                "type_name": 'Second Half Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        else:
            return False

    def convert_live_pick_tennis(self, pick: Dict, market_name: str,
                                 handicap_param_value: str = None) -> Dict | bool:
        market_name = market_name.lower()
        pick_label = pick.get('liveBetPickLabel', '').lower()
        odd_value = pick.get('oddValue')
        special_value = pick.get('specialValue')

        if pick['oddValue'] == 0:
            return False

        # Moneyline (1X2)
        if market_name == 'final outcome':
            if 'ft 1' in pick_label:
                pick_type = '1'
            elif 'ft 2' in pick_label:
                pick_type = '2'
            else:
                return False
            return {
                "type_name": 'Moneyline',
                "type": pick_type,
                "line": 0,
                "odds": float(odd_value)
            }

        # Тотал геймов на весь матч
        # elif 'total games match' in market_name:
        #     if not special_value:
        #         return False
        #     try:
        #         line = float(special_value)
        #     except ValueError:
        #         return False
        #
        #     if 'tg<' in pick_label:
        #         pick_type = 'GU'
        #     elif 'tg>' in pick_label:
        #         pick_type = 'GO'
        #     else:
        #         return False
        #     return {
        #         "type_name": 'Total',
        #         "type": pick_type,
        #         "line": line,
        #         "odds": float(odd_value)
        #     }

        # Индивидуальные тоталы геймов для игрока 1 (домашнего)
        elif 'team1 total games' in market_name:
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'team1 tg<' in pick_label:
                pick_type = 'GTHU'
            elif 'team1 tg>' in pick_label:
                pick_type = 'GTHO'
            else:
                return False
            return {
                "type_name": 'Individual Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Индивидуальные тоталы геймов для игрока 2 (гостевого)
        elif 'team2 total games' in market_name:
            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'team2 tg<' in pick_label:
                pick_type = 'GTAU'
            elif 'team2 tg>' in pick_label:
                pick_type = 'GTAO'
            else:
                return False
            return {
                "type_name": 'Individual Total',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Гандикап в геймах
        elif 'hendicap in games' in market_name:
            if not handicap_param_value:
                return False
            try:
                line = float(handicap_param_value)
            except ValueError:
                return False

            if 'hg 1' in pick_label:
                pick_type = 'GAH1'
            elif 'hg 2' in pick_label:
                pick_type = 'GAH2'
            else:
                return False
            return {
                "type_name": 'Handicap',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Тотал геймов по сетам
        elif any(
                f"{ordinal} set" in market_name and "games" in market_name
                for ordinal in
                ["first", "second", "third", "fourth", "fifth"]):
            set_number = next((i for i, ordinal in enumerate(
                ["first", "second", "third", "fourth", "fifth"], 1)
                               if
                               f"{ordinal} set" in market_name and "games" in market_name),
                              None)
            if set_number is None:
                return False

            if not special_value:
                return False
            try:
                line = float(special_value)
            except ValueError:
                return False

            if 'sg<' in pick_label:
                pick_type = f'{set_number}HGU'
            elif 'sg>' in pick_label:
                pick_type = f'{set_number}HGO'
            else:
                return False

            return {
                "type_name": f'Set {set_number} Total Games',
                "type": pick_type,
                "line": line,
                "odds": float(odd_value)
            }

        # Сетовый победитель
        elif any(f"{ordinal} set" in market_name for ordinal in
                 ["first", "second", "third", "fourth", "fifth"]):
            set_number = next((i for i, ordinal in enumerate(
                ["first", "second", "third", "fourth", "fifth"], 1)
                               if f"{ordinal} set" in market_name), None)
            if set_number is None:
                return False

            if 's 1' in pick_label:
                pick_type = f'{set_number}H1'
            elif 's 2' in pick_label:
                pick_type = f'{set_number}H2'
            else:
                return False
            return {
                "type_name": f'Set {set_number} Winner',
                "type": pick_type,
                "line": 0,
                "odds": float(odd_value)
            }

        else:
            return False

    def convert_timestamp_to_lobbet_format(self, timestamp: int) -> str:
        timestamp_seconds = timestamp / 1000
        dt = datetime.fromtimestamp(timestamp_seconds)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

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
                country = league_name.split(',')[
                    0].strip() if league_name else ''
            # Добавьте другие виды спорта по необходимости
            else:
                sport = 'Unknown'
        return sport, country

    async def get_live_odds(self):
        semaphore = asyncio.Semaphore(
            20)  # Максимум 20 одновременных запросов
        async with ClientSession(headers=self.HEADERS) as session:
            live_matches = await self.fetch_live_odds(session)
            live_tasks = [self.process_live_match(match, session, semaphore)
                          for match in live_matches]
            await asyncio.gather(*live_tasks)
            logging.info(f"Total live matches: {len(live_matches)}")
        return self.parsed_matches


# Пример использования
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     parser = LiveOddsParser()
#     parsed = asyncio.run(parser.get_live_odds())
#     print(parsed)
