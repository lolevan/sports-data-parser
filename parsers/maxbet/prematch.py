import asyncio
import logging
import time
import random
from typing import List, Dict, Union
from datetime import datetime, timedelta
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from parsers.maxbet.utils import (
    process_tennis_team_name, process_league_name, process_country_name,
    extract_city_from_league_name, process_football_team_names, get_country_by_city
)

BOOKIE = 'maxbet_me'
MAIN_URL = 'https://api.maxbet.me'

PROXIES = [
    # "http://ymuwnyuv:iryi7xvd3347@198.23.239.134:6540",
    "http://njghlerv:zhrkadbpglxc@107.172.163.27:6543",
    "http://njghlerv:zhrkadbpglxc@64.137.42.112:5157",
    "http://njghlerv:zhrkadbpglxc@173.211.0.148:6641",
    # "http://ymuwnyuv:iryi7xvd3347@167.160.180.203:6754",
    "http://njghlerv:zhrkadbpglxc@154.36.110.199:6853",
    "http://njghlerv:zhrkadbpglxc@173.0.9.70:5653",
    "http://njghlerv:zhrkadbpglxc@173.0.9.209:5792",
    # "http://ymuwnyuv:iryi7xvd3347@207.244.217.165:6712",
    # "http://ymuwnyuv:iryi7xvd3347@161.123.152.115:6360",
]


class PreMatchOddsParser:
    def __init__(self):
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.lock = asyncio.Lock()
        self.HEADERS = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=utf-8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Referer': 'https://www.maxbet.me/',
            'Origin': 'https://www.maxbet.me',
            'Connection': 'keep-alive',
        }

    async def fetch(self, url: str, session: ClientSession, proxy: str = None) -> Union[Dict, List, None]:
        try:
            async with session.get(url, proxy=proxy) as response:
                return await response.json(content_type=None)
        except Exception as e:
            logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
            return None

    async def fetch_with_retry(self, url: str, session: ClientSession, max_retries: int = 3) -> Union[Dict, List, None]:
        for attempt in range(max_retries):
            proxy = random.choice(PROXIES)
            result = await self.fetch(url, session, proxy)
            if result is not None:
                return result
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for URL {url}")
        logging.error(f"All {max_retries} attempts failed for URL {url}")
        return None

    async def get_all_leagues(self, session: ClientSession) -> List[str]:
        now = datetime.utcnow()
        end_time = now + timedelta(days=1)
        start_str = now.isoformat() + 'Z'
        end_str = end_time.isoformat() + 'Z'

        leagues_url = f"{MAIN_URL}/sport_filter?live=false&offer_plan=true&lang=en&start={start_str}&end={end_str}&categories=false"
        response = await self.fetch_with_retry(leagues_url, session)

        if not response:
            return []

        tournament_ids = [
            tournament['id']
            for sport in response
            for tournament in sport.get('tournaments', [])
        ]
        return tournament_ids

    async def fetch_tournament_events(self, t_id: str, session: ClientSession, semaphore: asyncio.Semaphore,
                                      start_str: str, end_str: str, all_matches: List[Dict]):
        events_url = (f"{MAIN_URL}/events?lang=en&live=false&limit=1000&fields=all"
                      f"&tournaments={t_id}&markets=&orderBy[]=startTimeAsc&start={start_str}&end={end_str}")
        async with semaphore:
            events_data = await self.fetch_with_retry(events_url, session)
            if events_data is None:
                return
            matches = events_data.get('events', [])
            all_matches.extend(matches)

    async def fetch_prematch_events(self, session: ClientSession) -> List[Dict]:
        tournament_ids = await self.get_all_leagues(session)
        all_matches = []
        now = datetime.utcnow()
        end_time = now + timedelta(days=1)
        start_str = now.isoformat() + 'Z'
        end_str = end_time.isoformat() + 'Z'

        semaphore = asyncio.Semaphore(10)  # до 10 параллельных запросов к турнирам
        tasks = [self.fetch_tournament_events(t_id, session, semaphore, start_str, end_str, all_matches)
                 for t_id in tournament_ids]
        await asyncio.gather(*tasks)

        return all_matches

    async def process_prematch_event(self, event: Dict, session: ClientSession, semaphore: asyncio.Semaphore):
        async with semaphore:
            match_id = event.get('id', '')
            competitors = event.get('competitors', [])
            if len(competitors) >= 2:
                home_team = competitors[0].get('name', 'Unknown').strip()
                away_team = competitors[1].get('name', 'Unknown').strip()
            else:
                home_team = 'Unknown'
                away_team = 'Unknown'

            kickoff_time_str = event.get('utc_scheduled', '')
            start_time = self.convert_datetime_to_timestamp(kickoff_time_str, offset_hours=8)
            league_id = event.get('tournament', {}).get('id', '')
            country = process_country_name(event.get('category', {}).get('name', ''))
            sport = event.get('sport', {}).get('name', '')
            league = process_league_name(event.get('tournament', {}).get('name', ''), sport)

            if sport.lower() == 'tennis':
                home_team = process_tennis_team_name(home_team)
                away_team = process_tennis_team_name(away_team)
                city = extract_city_from_league_name(league)
                if city:
                    country = get_country_by_city(city)
                else:
                    country = 'World'
            elif sport.lower() == 'football':
                home_team, away_team = process_football_team_names(home_team, away_team)

            match_data = {
                'match_id': match_id,
                'name': f'{home_team.replace("/", "")} vs {away_team.replace("/", "")}',
                'home_team': home_team,
                'away_team': away_team,
                'start_time': start_time,
                'league': league,
                'league_id': league_id,
                'country': country,
                'sport': sport,
                'outcomes': [],
                'time': time.time()
            }

            odds = event.get('odds', {})
            markets = []
            for odd_key, odd_data in odds.items():
                market = self.convert_odd_to_scanner_format(odd_key, odd_data)
                if market:
                    markets.append(market)
            match_data['outcomes'] = markets

            async with self.lock:
                self.ALL_MATCHES[match_id] = match_data
                self.parsed_matches[match_id] = match_data

    async def get_match_details(self, match_id: str, session: ClientSession) -> Dict:
        markets = 'lfb,lbb,ltn,lvb,lhb,lih,laf,lbv,ltt,lvf,lft,lsn,lrb,lbs,ldt,lwp,lbm,les,lef,l3x3,lrg,lbf,lb20'
        event_details_url = f"{MAIN_URL}/events?events={match_id}&markets={markets}&lang=en"

        event_data = await self.fetch_with_retry(event_details_url, session)
        if event_data is None or 'events' not in event_data or not event_data['events']:
            logging.warning(f"Failed to retrieve details for match ID {match_id}")
            return {}

        event = event_data['events'][0]

        competitors = event.get('competitors', [])
        if len(competitors) >= 2:
            home_team = competitors[0].get('name', 'Unknown').strip()
            away_team = competitors[1].get('name', 'Unknown').strip()
        else:
            home_team = 'Unknown'
            away_team = 'Unknown'

        kickoff_time_str = event.get('utc_scheduled', '')
        start_time = self.convert_datetime_to_timestamp(kickoff_time_str, offset_hours=8)
        league_id = event.get('tournament', {}).get('id', '')
        country = process_country_name(event.get('category', {}).get('name', ''))
        sport = event.get('sport', {}).get('name', '')
        league = process_league_name(event.get('tournament', {}).get('name', ''), sport)

        if sport.lower() == 'tennis':
            home_team = process_tennis_team_name(home_team)
            away_team = process_tennis_team_name(away_team)
            city = extract_city_from_league_name(league)
            if city:
                country = get_country_by_city(city)
            else:
                country = 'World'
        elif sport.lower() == 'football':
            home_team, away_team = process_football_team_names(home_team, away_team)

        match_data = {
            'match_id': match_id,
            'name': f'{home_team.replace("/", "")} vs {away_team.replace("/", "")}',
            'home_team': home_team,
            'away_team': away_team,
            'start_time': start_time,
            'league': league,
            'league_id': league_id,
            'country': country,
            'sport': sport,
            'outcomes': [],
            'time': time.time()
        }

        odds = event.get('odds', {})
        markets = []
        for odd_key, odd_data in odds.items():
            market = self.convert_odd_to_scanner_format(odd_key, odd_data)
            if market:
                markets.append(market)
        match_data['outcomes'] = markets

        return match_data

    def convert_odd_to_scanner_format(self, odd_key: str, odd_data: Dict, score_difference: int = 0) -> Dict:
        """
        Преобразует данные коэффициентов в стандартный формат сканера.

        :param odd_key: Ключ коэффициента
        :param odd_data: Данные коэффициента
        :param score_difference: Разница в счете между командами (не используется в данном контексте)
        :return: Словарь с преобразованными данными
        """
        # Получаем значение коэффициента
        odd_value = odd_data.get('value')

        # Проверяем наличие коэффициента
        if odd_value == 0 or odd_value is None:
            odd_value = 0.0  # Устанавливаем значение по умолчанию

        parts = odd_key.split(':')

        # Проверяем корректность ключа
        if len(parts) < 3:
            return {
                "type_name": "Unknown",
                "type": "Unknown",
                "line": 0.0,
                "odds": float(odd_value)
            }

        market_key = parts[1]  # Например, 'ft' для Full Time
        pick_type = parts[2]   # Например, '1', 'X', '2', 'over', 'under'

        elem_outcomes = {
            "type_name": market_key,
            "type": pick_type,
            "line": 0.0,
            "odds": float(odd_value)
        }

        # Преобразуем market_key и pick_type в понятные названия
        # if market_key == "dw":
        #     elem_outcomes["type_name"] = "Draw"
        # elif market_key == "fs":
        #     elem_outcomes["type_name"] = "Full Time Score"
        # elif market_key == "ht":
        #     elem_outcomes["type_name"] = "Half Time"
        # elif market_key == "sh":
        #     elem_outcomes["type_name"] = "Second Half"
        # elif market_key == "dc":
        #     elem_outcomes["type_name"] = "Double Chance"
        # elif market_key == "oe":
        #     elem_outcomes["type_name"] = "Odd/Even"
        # elif market_key == "tg":
        #     elem_outcomes["type_name"] = "Total Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "atg":
        #     elem_outcomes["type_name"] = "Alternative Total Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "cr":
        #     elem_outcomes["type_name"] = "Correct Score"
        #     elem_outcomes["type"] = f"{parts[2]}:{parts[3]}" if len(parts) > 3 else "Unknown"
        # elif market_key == "gg":
        #     elem_outcomes["type_name"] = "Both Teams to Score"
        # elif market_key == "fhg":
        #     elem_outcomes["type_name"] = "First Half Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "shg":
        #     elem_outcomes["type_name"] = "Second Half Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0

        # Обрабатываем различные типы ставок
        if market_key == "fs":
            # 1X2 (Основной исход)
            elem_outcomes["type_name"] = "1X2"
            elem_outcomes["bet_type"] = "MONEYLINE"
            if pick_type == "1":
                elem_outcomes["type"] = "1"
                elem_outcomes["team"] = "TEAM1"
            elif pick_type == "2":
                elem_outcomes["type"] = "2"
                elem_outcomes["team"] = "TEAM2"
            else:
                elem_outcomes["type"] = "Unknown"

        elif market_key in ["1s", "2s"]:
            # Исходы по периодам (например, первый сет)
            period = int(market_key[0])
            elem_outcomes["type_name"] = f"{period}H1X2"
            elem_outcomes["bet_type"] = "MONEYLINE"
            elem_outcomes["period_number"] = period
            if pick_type == "1":
                elem_outcomes["type"] = f"{period}H1"
                elem_outcomes["team"] = "TEAM1"
            elif pick_type == "2":
                elem_outcomes["type"] = f"{period}H2"
                elem_outcomes["team"] = "TEAM2"
            else:
                elem_outcomes["type"] = "Unknown"

        # elif market_key == "hg":
        #     # Азиатский гандикап
        #     elem_outcomes["type_name"] = "Asian Handicap"
        #     elem_outcomes["bet_type"] = "SPREAD"
        #     try:
        #         team, line = pick_type.split('|')
        #         elem_outcomes["line"] = float(line)
        #         if team == "1":
        #             elem_outcomes["type"] = "AH1"
        #             elem_outcomes["team"] = "TEAM1"
        #         elif team == "2":
        #             elem_outcomes["type"] = "AH2"
        #             elem_outcomes["team"] = "TEAM2"
        #         else:
        #             elem_outcomes["type"] = "Unknown"
        #     except ValueError:
        #         elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = 0.0

        # elif market_key == "hs":
        #     # Азиатский гандикап по сетам
        #     elem_outcomes["type_name"] = "Asian Handicap"
        #     elem_outcomes["bet_type"] = "SPREAD"
        #     try:
        #         team, line = pick_type.split('|')
        #         elem_outcomes["line"] = float(line)
        #         if team == "1":
        #             elem_outcomes["type"] = "AH1"
        #             elem_outcomes["team"] = "TEAM1"
        #         elif team == "2":
        #             elem_outcomes["type"] = "AH2"
        #             elem_outcomes["team"] = "TEAM2"
        #         else:
        #             elem_outcomes["type"] = "Unknown"
        #     except ValueError:
        #         elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = 0.0

        elif market_key in ["g", "tg", "GO", "GU"]:
            # Тотал голов
            elem_outcomes["type_name"] = "Total Goals"
            elem_outcomes["bet_type"] = "TOTAL_POINTS"
            if market_key in ["g", "tg"]:
                try:
                    side, line = pick_type.split('|')
                    elem_outcomes["line"] = float(line)
                    if side in ["(+)", "+"]:
                        elem_outcomes["type"] = "O"
                        elem_outcomes["side"] = "OVER"
                    elif side in ["(-)", "-"]:
                        elem_outcomes["type"] = "U"
                        elem_outcomes["side"] = "UNDER"
                    else:
                        elem_outcomes["type"] = "Unknown"
                except ValueError:
                    elem_outcomes["type"] = "Unknown"
                    elem_outcomes["line"] = 0.0
            else:
                elem_outcomes["line"] = odd_data.get('special_value', 0.0)
                if market_key == "GO":
                    elem_outcomes["type"] = "O"
                    elem_outcomes["side"] = "OVER"
                elif market_key == "GU":
                    elem_outcomes["type"] = "U"
                    elem_outcomes["side"] = "UNDER"

        elif market_key in ["GTHO", "GTHU", "GTAO", "GTAU"]:
            # Индивидуальный тотал команды
            if market_key.startswith("GTH"):
                elem_outcomes["type_name"] = "Team Total Home"
                elem_outcomes["team"] = "TEAM1"
            else:
                elem_outcomes["type_name"] = "Team Total Away"
                elem_outcomes["team"] = "TEAM2"
            elem_outcomes["bet_type"] = "TEAM_TOTAL_POINTS"
            elem_outcomes["line"] = odd_data.get('special_value', 0.0)
            if market_key.endswith("O"):
                elem_outcomes["type"] = market_key
                elem_outcomes["side"] = "OVER"
            elif market_key.endswith("U"):
                elem_outcomes["type"] = market_key
                elem_outcomes["side"] = "UNDER"

        elif market_key.startswith("1H"):
            # Ставки на первый период (например, первый тайм)
            elem_outcomes["period_number"] = 1
            if market_key == "1H1X2":
                elem_outcomes["type_name"] = "1H1X2"
                elem_outcomes["bet_type"] = "MONEYLINE"
                if pick_type == "1":
                    elem_outcomes["type"] = "1H1"
                    elem_outcomes["team"] = "TEAM1"
                elif pick_type == "2":
                    elem_outcomes["type"] = "1H2"
                    elem_outcomes["team"] = "TEAM2"
                else:
                    elem_outcomes["type"] = "Unknown"
            elif market_key in ["1HGO", "1HGU"]:
                elem_outcomes["type_name"] = "1H Total Goals"
                elem_outcomes["bet_type"] = "TOTAL_POINTS"
                elem_outcomes["line"] = odd_data.get('special_value', 0.0)
                if market_key == "1HGO":
                    elem_outcomes["type"] = "1HO"
                    elem_outcomes["side"] = "OVER"
                elif market_key == "1HGU":
                    elem_outcomes["type"] = "1HU"
                    elem_outcomes["side"] = "UNDER"

        else:
            # Если тип рынка не распознан, оставляем оригинальные данные
            elem_outcomes["type_name"] = market_key
            elem_outcomes["type"] = pick_type

        # Если линия не установлена, пытаемся получить ее из odd_data
        if elem_outcomes["line"] == 0.0:
            line = odd_data.get('special_value') or odd_data.get('line')
            if line is not None:
                try:
                    elem_outcomes["line"] = float(line)
                except (ValueError, TypeError):
                    elem_outcomes["line"] = 0.0

        return elem_outcomes

        # if market_key == "dw":
        #     elem_outcomes["type_name"] = "Draw"
        # elif market_key == "fs":
        #     elem_outcomes["type_name"] = "Full Time Score"
        # elif market_key == "ht":
        #     elem_outcomes["type_name"] = "Half Time"
        # elif market_key == "sh":
        #     elem_outcomes["type_name"] = "Second Half"
        # elif market_key == "dc":
        #     elem_outcomes["type_name"] = "Double Chance"
        # elif market_key == "oe":
        #     elem_outcomes["type_name"] = "Odd/Even"
        # elif market_key == "tg":
        #     elem_outcomes["type_name"] = "Total Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "atg":
        #     elem_outcomes["type_name"] = "Alternative Total Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "cr":
        #     elem_outcomes["type_name"] = "Correct Score"
        #     elem_outcomes["type"] = f"{parts[2]}:{parts[3]}" if len(parts) > 3 else "Unknown"
        # elif market_key == "gg":
        #     elem_outcomes["type_name"] = "Both Teams to Score"
        # elif market_key == "fhg":
        #     elem_outcomes["type_name"] = "First Half Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "shg":
        #     elem_outcomes["type_name"] = "Second Half Goals"
        #     if pick_type.endswith('+'):
        #         try:
        #             elem_outcomes["line"] = float(pick_type[:-1])
        #         except ValueError:
        #             elem_outcomes["line"] = 0.0
        # elif market_key == "1s":  # Победитель 1-го сета
        #     elem_outcomes["type_name"] = "Set Winner"
        #     elem_outcomes["set_number"] = 1
        # elif market_key == "2s":  # Победитель 2-го сета
        #     elem_outcomes["type_name"] = "Set Winner"
        #     elem_outcomes["set_number"] = 2
        # elif market_key == "Full Time Score":  # Победитель матча
        #     elem_outcomes["type_name"] = "Match Winner"
        # elif market_key == "hg":  # Гандикап по геймам
        #     elem_outcomes["type_name"] = "Game Handicap"
        #     team, line = pick_type.split('|')
        #     elem_outcomes["type"] = team
        #     elem_outcomes["line"] = float(line)
        # elif market_key == "g":  # Тотал геймов
        #     elem_outcomes["type_name"] = "Total Games"
        #     side, line = pick_type.split('|')
        #     elem_outcomes["type"] = side  # '+' или '-'
        #     elem_outcomes["line"] = float(line)
        # else:
        #     # Если тип рынка не распознан, оставляем оригинальные данные
        #     elem_outcomes["type_name"] = market_key
        #     elem_outcomes["type"] = pick_type

        # Обрабатываем теннисные рынки
        # if market_key == "1s" or market_key == "2s":
        #     # Победитель 1-го или 2-го сета
        #     elem_outcomes["type_name"] = "Set Winner"
        #     elem_outcomes["set_number"] = int(market_key[0])  # Номер сета
        #     if pick_type == "1":
        #         elem_outcomes["type"] = "1"
        #     elif pick_type == "2":
        #         elem_outcomes["type"] = "2"
        #     else:
        #         elem_outcomes["type"] = "Unknown"
        #
        # elif market_key == "fs":
        #     # Победитель матча
        #     elem_outcomes["type_name"] = "Match Winner"
        #     if pick_type == "1":
        #         elem_outcomes["type"] = "1"
        #     elif pick_type == "2":
        #         elem_outcomes["type"] = "2"
        #     else:
        #         elem_outcomes["type"] = "Unknown"
        #
        # elif market_key == "hg":
        #     # Гандикап по геймам
        #     elem_outcomes["type_name"] = "Game Handicap"
        #     try:
        #         team, line = pick_type.split('|')
        #         elem_outcomes["type"] = f"AH{team}"
        #         elem_outcomes["line"] = float(line)
        #     except ValueError:
        #         elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = 0.0
        #
        # elif market_key == "hs":
        #     # Гандикап по сетам
        #     elem_outcomes["type_name"] = "Set Handicap"
        #     try:
        #         team, line = pick_type.split('|')
        #         elem_outcomes["type"] = f"AH{team}"
        #         elem_outcomes["line"] = float(line)
        #     except ValueError:
        #         elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = 0.0
        #
        # elif market_key == "g":
        #     # Тотал геймов
        #     elem_outcomes["type_name"] = "Total Games"
        #     try:
        #         side, line = pick_type.split('|')
        #         if side == "(+)":
        #             elem_outcomes["type"] = "O"  # Over
        #         elif side == "(-)":
        #             elem_outcomes["type"] = "U"  # Under
        #         else:
        #             elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = float(line)
        #     except ValueError:
        #         elem_outcomes["type"] = "Unknown"
        #         elem_outcomes["line"] = 0.0
        #
        # elif market_key == "cr":
        #     # Точный счет
        #     elem_outcomes["type_name"] = "Correct Score"
        #     if len(parts) >= 4:
        #         score = f"{parts[2]}:{parts[3]}"
        #         elem_outcomes["type"] = score
        #     else:
        #         elem_outcomes["type"] = pick_type
        #
        # # Добавьте другие рынки при необходимости
        #
        # elif market_key in ["GTHO", "GTHU", "GTAO", "GTAU"]:
        #     # Индивидуальный тотал по геймам
        #     if market_key.startswith("GTH"):
        #         elem_outcomes["type_name"] = "Team Total Home"
        #     else:
        #         elem_outcomes["type_name"] = "Team Total Away"
        #     elem_outcomes["type"] = market_key
        #     elem_outcomes["line"] = float(odd_data.get('special_value', 0.0))
        #
        # else:
        #     # Если тип рынка не распознан, оставляем оригинальные данные
        #     elem_outcomes["type_name"] = market_key
        #     elem_outcomes["type"] = pick_type
        #
        # return elem_outcomes

    def convert_datetime_to_timestamp(self, datetime_str: str, offset_hours: int = 0) -> float:
        try:
            dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logging.warning(f"Failed to parse datetime: {datetime_str}")
                return 0.0
        dt += timedelta(hours=offset_hours)
        return dt.timestamp()

    async def get_prematch_odds(self) -> Dict[str, Dict]:
        timeout = ClientTimeout(total=15)
        connector = TCPConnector(limit_per_host=5)
        async with ClientSession(headers=self.HEADERS, timeout=timeout, connector=connector) as session:
            prematch_events = await self.fetch_prematch_events(session)
            semaphore = asyncio.Semaphore(30)
            tasks = [self.process_prematch_event(event, session, semaphore) for event in prematch_events]
            await asyncio.gather(*tasks)
            logging.info(f"Total pre-match events: {len(prematch_events)}")
        return self.parsed_matches

# Пример использования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = PreMatchOddsParser()

    start_time = time.time()
    parsed = asyncio.run(parser.get_prematch_odds())
    end_time = time.time()

    total_matches = len(parsed)
    elapsed_time = end_time - start_time

    for match_id, match_data in parsed.items():
        print(match_data)

    print(f"\nОбработано матчей: {total_matches}")
    print(f"Время выполнения: {elapsed_time:.2f} секунд")
