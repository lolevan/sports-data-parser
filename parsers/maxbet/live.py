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

# Список прокси-серверов для использования при запросах
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


class LiveOddsParser:
    def __init__(self):
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.lock = asyncio.Lock()  # Блокировка для обеспечения потокобезопасности
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

    async def fetch(self, url: str, session: ClientSession,
                    proxy: str = None, method: str = 'GET',
                    data: Dict = None) -> Union[Dict, List, None]:
        try:
            if method == 'GET':
                async with session.get(url, proxy=proxy) as response:
                    return await response.json(content_type=None)
            elif method == 'POST':
                async with session.post(url, proxy=proxy, json=data) as response:
                    return await response.json(content_type=None)
            else:
                logging.error(f"Unsupported method {method} for URL {url}")
                return None
        except Exception as e:
            logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
            return None

    async def fetch_with_retry(self, url: str, session: ClientSession,
                               max_retries: int = 3, method: str = 'GET',
                               data: Dict = None) -> Union[Dict, List, None]:
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
        # Получаем список live-турниров
        tournaments_url = f'{MAIN_URL}/sport_filter?live=true&offer_plan=true&lang=en&categories=false'
        data = await self.fetch_with_retry(tournaments_url, session)
        if data is None:
            return []

        tournament_ids = []
        for sport in data:
            for tournament in sport.get('tournaments', []):
                tournament_ids.append(tournament['id'])

        # Параллельная загрузка матчей для турниров с семафором
        # чтобы не перегрузить соединения
        semaphore = asyncio.Semaphore(10)  # до 10 параллельных запросов турниров
        tasks = []
        all_matches = []

        async def fetch_tournament_matches(t_id):
            # Уменьшаем limit для оптимизации
            events_url = f"{MAIN_URL}/events?lang=en&live=true&limit=1000&fields=all&tournaments={t_id}&markets=&orderBy[]=startTimeAsc"
            async with semaphore:
                events_data = await self.fetch_with_retry(events_url, session)
                if events_data is None:
                    return
                matches = events_data.get('events', [])
                all_matches.extend(matches)

        for t_id in tournament_ids:
            tasks.append(asyncio.create_task(fetch_tournament_matches(t_id)))

        await asyncio.gather(*tasks)
        return all_matches

    async def process_live_match(self, match: Dict, session: ClientSession,
                                 semaphore: asyncio.Semaphore):
        async with semaphore:
            match_id = match.get('id')
            competitors = match.get('competitors', [])
            if len(competitors) >= 2:
                home_team = competitors[0].get('name', 'Unknown').strip()
                away_team = competitors[1].get('name', 'Unknown').strip()
            else:
                home_team = 'Unknown'
                away_team = 'Unknown'

            kickoff_time_str = match.get('utc_scheduled', '')
            start_time = self.convert_datetime_to_timestamp(kickoff_time_str, offset_hours=8)
            league_id = match.get('tournament', {}).get('id', '')
            country = process_country_name(match.get('category', {}).get('name', ''))
            sport = match.get('sport', {}).get('name', '')
            league = process_league_name(match.get('tournament', {}).get('name', ''), sport)

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

            current_score = match.get('scores', {}).get('current_score', {})
            score_h = current_score.get('home_score') or 0
            score_a = current_score.get('away_score') or 0

            try:
                score_h = int(score_h)
                score_a = int(score_a)
            except (ValueError, TypeError):
                score_h = 0
                score_a = 0

            score_difference = score_h - score_a

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
                'current_score': f'{score_h}-{score_a}',
                'phase': match.get('period'),
                'outcomes': [],
                'time': time.time()
            }

            odds = match.get('odds', {})
            markets = []
            for odd_key, odd_data in odds.items():
                market = self.convert_odd_to_scanner_format(odd_key, odd_data, score_difference)
                if market:
                    markets.append(market)
            match_data['outcomes'] = markets
            match_data['time'] = time.time()

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

        current_score = event.get('scores', {}).get('current_score', {})
        score_h = current_score.get('home_score') or 0
        score_a = current_score.get('away_score') or 0

        try:
            score_h = int(score_h)
            score_a = int(score_a)
        except (ValueError, TypeError):
            score_h = 0
            score_a = 0

        score_difference = score_h - score_a

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
            'current_score': f'{score_h}-{score_a}',
            'phase': event.get('period'),
            'outcomes': [],
            'time': time.time()
        }

        odds = event.get('odds', {})
        markets = []
        for odd_key, odd_data in odds.items():
            market = self.convert_odd_to_scanner_format(odd_key, odd_data, score_difference)
            if market:
                markets.append(market)
        match_data['outcomes'] = markets

        return match_data

    def convert_odd_to_scanner_format(self, odd_key: str, odd_data: Dict, score_difference: int = 0) -> Dict:
        """
        Преобразует данные коэффициентов в стандартный формат сканера.

        :param odd_key: Ключ коэффициента
        :param odd_data: Данные коэффициента
        :param score_difference: Разница в счете между командами
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
        if market_key in ["ft"]:
            elem_outcomes["type_name"] = "Final Score"
            elem_outcomes["type"] = pick_type
        elif market_key == "dc":
            elem_outcomes["type_name"] = "Double Chance"
            elem_outcomes["type"] = pick_type
        elif market_key == "cs":
            elem_outcomes["type_name"] = "Correct Score"
            elem_outcomes["type"] = pick_type
        elif market_key == "tg":
            elem_outcomes["type_name"] = "Total Goals"
            if "over" in odd_key:
                elem_outcomes["type"] = "Over"
                elem_outcomes["line"] = float(parts[-1].split("|")[-1])
            elif "under" in odd_key:
                elem_outcomes["type"] = "Under"
                elem_outcomes["line"] = float(parts[-1].split("|")[-1])
        elif market_key == "hf":
            elem_outcomes["type_name"] = "Half Time - Full Time"
            elem_outcomes["type"] = pick_type
        elif market_key == "1x2":
            elem_outcomes["type_name"] = "1X2"
            elem_outcomes["type"] = pick_type
        elif market_key == "dnb":
            elem_outcomes["type_name"] = "Draw No Bet"
            elem_outcomes["type"] = pick_type
        elif market_key == "eo":
            elem_outcomes["type_name"] = "Even/Odd"
            elem_outcomes["type"] = pick_type
        elif market_key == "btts":
            elem_outcomes["type_name"] = "Both Teams to Score"
            elem_outcomes["type"] = pick_type
        elif market_key == "ng":
            elem_outcomes["type_name"] = "First Team to Score"
            elem_outcomes["type"] = pick_type.split("|")[-1]
        elif market_key == "tg1sth":
            elem_outcomes["type_name"] = "Total Goals First Half"
            if "over" in odd_key:
                elem_outcomes["type"] = "Over"
                elem_outcomes["line"] = float(odd_key.split("|")[-1])
            elif "under" in odd_key:
                elem_outcomes["type"] = "Under"
                elem_outcomes["line"] = float(odd_key.split("|")[-1])
        elif market_key == "tg2ndh":
            elem_outcomes["type_name"] = "Total Goals Second Half"
            if "over" in odd_key:
                elem_outcomes["type"] = "Over"
                elem_outcomes["line"] = float(odd_key.split("|")[-1])
            elif "under" in odd_key:
                elem_outcomes["type"] = "Under"
                elem_outcomes["line"] = float(odd_key.split("|")[-1])
        # else:
        #     # Если тип рынка не распознан, оставляем оригинальные данные
        #     elem_outcomes["type_name"] = market_key
        #     elem_outcomes["type"] = pick_type

        # Process different bet types
        if market_key == "fr":
            # Match Winner (Final Result)
            elem_outcomes["type_name"] = "Match Winner"
            elem_outcomes["bet_type"] = "MONEYLINE"
            if pick_type == "1":
                elem_outcomes["type"] = "1"
                elem_outcomes["team"] = "PLAYER1"
            elif pick_type == "2":
                elem_outcomes["type"] = "2"
                elem_outcomes["team"] = "PLAYER2"
            else:
                elem_outcomes["type"] = "Unknown"

        elif market_key in ["1sw", "2sw"]:
            # Set Winner (1st Set Winner, 2nd Set Winner)
            set_number = int(market_key[0])
            elem_outcomes["type_name"] = f"Set {set_number} Winner"
            elem_outcomes["bet_type"] = "MONEYLINE"
            elem_outcomes["period_number"] = set_number
            if pick_type == "1":
                elem_outcomes["type"] = f"S{set_number}W1"
                elem_outcomes["team"] = "PLAYER1"
            elif pick_type == "2":
                elem_outcomes["type"] = f"S{set_number}W2"
                elem_outcomes["team"] = "PLAYER2"
            else:
                elem_outcomes["type"] = "Unknown"

        elif market_key == "tg":
            # Total Games
            elem_outcomes["type_name"] = "Total Games"
            elem_outcomes["bet_type"] = "TOTAL_POINTS"
            try:
                side, line = pick_type.split('|')
                elem_outcomes["line"] = float(line)
                if side.lower() == "over":
                    elem_outcomes["type"] = "O"
                    elem_outcomes["side"] = "OVER"
                elif side.lower() == "under":
                    elem_outcomes["type"] = "U"
                    elem_outcomes["side"] = "UNDER"
                else:
                    elem_outcomes["type"] = "Unknown"
            except ValueError:
                elem_outcomes["type"] = "Unknown"
                elem_outcomes["line"] = 0.0

        elif market_key in ["tnoght", "tnogat"]:
            # Total Number of Games Home/Away Team
            if market_key == "tnoght":
                elem_outcomes["type_name"] = "Team Total Home"
                elem_outcomes["team"] = "PLAYER1"
                type_prefix = "TH"
            elif market_key == "tnogat":
                elem_outcomes["type_name"] = "Team Total Away"
                elem_outcomes["team"] = "PLAYER2"
                type_prefix = "TA"
            elem_outcomes["bet_type"] = "TEAM_TOTAL_POINTS"
            try:
                side, line = pick_type.split('|')
                elem_outcomes["line"] = float(line)
                if side.lower() == "over":
                    elem_outcomes["type"] = f"{type_prefix}O"
                    elem_outcomes["side"] = "OVER"
                elif side.lower() == "under":
                    elem_outcomes["type"] = f"{type_prefix}U"
                    elem_outcomes["side"] = "UNDER"
                else:
                    elem_outcomes["type"] = "Unknown"
            except ValueError:
                elem_outcomes["type"] = "Unknown"
                elem_outcomes["line"] = 0.0

        elif market_key == "eog":
            # Even/Odd Games
            elem_outcomes["type_name"] = "Even/Odd Games"
            elem_outcomes["bet_type"] = "ODD_EVEN"
            if pick_type.lower() == "even":
                elem_outcomes["type"] = "Even"
            elif pick_type.lower() == "odd":
                elem_outcomes["type"] = "Odd"
            else:
                elem_outcomes["type"] = "Unknown"

        elif market_key == "1stseog":
            # 1st Set Even/Odd Games
            elem_outcomes["type_name"] = "1st Set Even/Odd Games"
            elem_outcomes["bet_type"] = "ODD_EVEN"
            elem_outcomes["period_number"] = 1
            if pick_type.lower() == "even":
                elem_outcomes["type"] = "1stSetEven"
            elif pick_type.lower() == "odd":
                elem_outcomes["type"] = "1stSetOdd"
            else:
                elem_outcomes["type"] = "Unknown"

        elif market_key == "cs":
            # Correct Score
            elem_outcomes["type_name"] = "Correct Score"
            elem_outcomes["bet_type"] = "CORRECT_SCORE"
            elem_outcomes["type"] = pick_type

        elif market_key == "nos23":
            # Number of Sets 2 or 3
            elem_outcomes["type_name"] = "Number of Sets"
            elem_outcomes["bet_type"] = "NUMBER_OF_SETS"
            elem_outcomes["type"] = pick_type

        else:
            # For other markets, keep the original names for easier debugging
            elem_outcomes["type_name"] = market_key
            elem_outcomes["type"] = pick_type

        # Process 'line' field from odd_data if it's not set and 'special_value' exists
        if elem_outcomes["line"] == 0.0:
            line = odd_data.get('special_value')
            if line is not None:
                try:
                    elem_outcomes["line"] = float(line)
                except (ValueError, TypeError):
                    elem_outcomes["line"] = 0.0

        return elem_outcomes

        # Обрабатываем рынки
        # if market_key == 'fr':
        #     # Результат матча (Match Winner)
        #     elem_outcomes['type_name'] = 'Match Winner'
        #     elem_outcomes['type'] = pick_type  # '1' или '2'
        #
        # elif market_key.endswith('sw'):
        #     # Победитель сета (Set Winner)
        #     set_number = market_key[:-2]
        #     if set_number.isdigit():
        #         set_number = int(set_number)
        #         elem_outcomes['type_name'] = f'Set {set_number} Winner'
        #         elem_outcomes['type'] = pick_type
        #         elem_outcomes['set_number'] = set_number
        #     else:
        #         return None
        #
        # elif market_key == 'hg':
        #     # Гандикап по геймам (Game Handicap)
        #     team_line = pick_type.split('|')
        #     if len(team_line) == 2:
        #         team = team_line[0]
        #         line = float(team_line[1])
        #         elem_outcomes['type_name'] = 'Game Handicap'
        #         elem_outcomes['type'] = f'AH{team}'
        #         elem_outcomes['line'] = line
        #     else:
        #         return None
        #
        # elif market_key in ['g', 'tg', 'tnoght', 'tnogat', 'tnogis1', 'tnogis2']:
        #     # Тотал геймов (Total Games), включая тотал по командам и сетам
        #     side_line = pick_type.split('|')
        #     if len(side_line) == 2:
        #         side = side_line[0].lower()
        #         line = float(side_line[1])
        #         if market_key == 'tnoght':
        #             elem_outcomes['type_name'] = 'Total Games Home Team'
        #         elif market_key == 'tnogat':
        #             elem_outcomes['type_name'] = 'Total Games Away Team'
        #         elif market_key == 'tnogis1':
        #             elem_outcomes['type_name'] = 'Total Games Set 1'
        #             elem_outcomes['set_number'] = 1
        #         elif market_key == 'tnogis2':
        #             elem_outcomes['type_name'] = 'Total Games Set 2'
        #             elem_outcomes['set_number'] = 2
        #         else:
        #             elem_outcomes['type_name'] = 'Total Games'
        #         if side in ['over', '(+)']:
        #             elem_outcomes['type'] = 'O'
        #         elif side in ['under', '(-)']:
        #             elem_outcomes['type'] = 'U'
        #         else:
        #             return None  # Некорректный тип тотала
        #         elem_outcomes['line'] = line
        #     else:
        #         return None
        #
        # elif market_key == 'cs':
        #     # Точный счет по сетам (Correct Score)
        #     elem_outcomes['type_name'] = 'Correct Score'
        #     elem_outcomes['type'] = pick_type
        #
        # elif market_key == 'eog':
        #     # Чет/Нечет по геймам (Even/Odd Games)
        #     outcome = pick_type.lower()
        #     if outcome in ['even', 'odd']:
        #         elem_outcomes['type_name'] = 'Even/Odd Games'
        #         elem_outcomes['type'] = 'Even' if outcome == 'even' else 'Odd'
        #     else:
        #         return None
        #
        # else:
        #     # Для остальных рынков используем названия из БК
        #     elem_outcomes['type_name'] = market_key
        #     elem_outcomes['type'] = pick_type
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

    async def get_live_odds(self) -> Dict[str, Dict]:
        timeout = ClientTimeout(total=15)
        connector = TCPConnector(limit_per_host=5)
        async with ClientSession(headers=self.HEADERS, timeout=timeout, connector=connector) as session:
            live_matches = await self.fetch_live_odds(session)

            # Обработка матчей также параллельно, но с ограничением по семафору
            semaphore = asyncio.Semaphore(20)
            tasks = [self.process_live_match(match, session, semaphore) for match in live_matches]
            await asyncio.gather(*tasks)

            logging.info(f"Total live matches: {len(live_matches)}")
        return self.parsed_matches


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = LiveOddsParser()

    start_time = time.time()
    parsed = asyncio.run(parser.get_live_odds())
    end_time = time.time()

    total_matches = len(parsed)
    elapsed_time = end_time - start_time

    # Опционально вывести матчи
    for match_id, match_data in parsed.items():
        print(match_data)

    print(f"\nОбработано матчей: {total_matches}")
    print(f"Время выполнения: {elapsed_time:.2f} секунд")
