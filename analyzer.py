# analyzer.py
import asyncio
import ujson as json
import logging
import websockets
import time
from typing import Dict, Any
from matching.match_finder import MatchFinder

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdvancedAnalyzer:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.values: Dict[str, Any] = {}
        self.bookmaker_data: Dict[str, Dict[str, Dict[str, Any]]] = {
            bookmaker: {
                'live': {},
                'prematch': {}
            } for bookmaker in self.config
        }

        # Константы
        self.MARGIN = 1.08
        self.MAX_ODDS = 3.7
        self.MIN_ODDS = 1.1
        self.extra_percents = [
            (2.29, 2.75, 1.03),
            (2.75, 3.2, 1.04),
            (3.2, 3.7, 1.05)
        ]
        self.match_finders = {
            bookmaker: MatchFinder(bookmaker)
            for bookmaker in self.config if bookmaker != 'pinnacle'
        }
        self.PREMATCH_MAX_AGE = 30  # секунд
        self.LIVE_MAX_AGE = 3  # секунд

        # Блокировки для синхронизации доступа к общим данным
        self.values_lock = asyncio.Lock()
        self.bookmaker_data_lock = asyncio.Lock()

        # Set of connected clients
        self.connected_clients = set()
        self.clients_lock = asyncio.Lock()

        # Event to signal data updates
        self.data_updated_event = asyncio.Event()

    async def start(self):
        tasks = [
            asyncio.create_task(
                self.connect_to_bookmaker(bookmaker, config)
            )
            for bookmaker, config in self.config.items()
            if config['enabled']
        ]
        tasks += [
            asyncio.create_task(self.analyze_loop()),
            asyncio.create_task(self.broadcast_data_loop()),
            asyncio.create_task(self.start_websocket_server()),
            asyncio.create_task(self.cleanup_values_loop()),
            asyncio.create_task(self.cleanup_bookmaker_data_loop())
        ]
        await asyncio.gather(*tasks)

    async def connect_to_bookmaker(self, bookmaker: str,
                                   config: Dict[str, Any]):
        uri = f"ws://localhost:{config['port']}"
        while True:
            try:
                async with websockets.connect(
                        uri, max_size=None, timeout=10, ping_interval=20,
                        ping_timeout=10
                ) as websocket:
                    logger.info(
                        f"Connected to {bookmaker} on port {config['port']}")
                    async for message in websocket:
                        data = json.loads(message)
                        # Обработка сообщения
                        await self.handle_message(bookmaker, data)
            except (websockets.exceptions.ConnectionClosed,
                    asyncio.TimeoutError) as e:
                logger.warning(
                    f"Connection to {bookmaker} closed. Reconnecting...")
                logger.debug(f"ConnectionClosed exception details: {e}")
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.exception(
                    f"Error in connect_to_bookmaker for {bookmaker}: {e}")
                await asyncio.sleep(0.1)

    async def handle_message(self, bookmaker: str, data: Dict[str, Any]):
        try:
            async with self.bookmaker_data_lock:
                self.update_bookmaker_data(bookmaker, data)
        except Exception as e:
            logger.error(f"Error in handle_message: {e}")

    def update_bookmaker_data(self, bookmaker: str,
                              data: Dict[str, Any]):
        for match_id, match_data in data.items():
            if not match_data or not match_data.get('outcomes'):
                # Запланировать удаление соответствующих значений
                asyncio.create_task(
                    self.delete_match_values(bookmaker, match_id))
                continue
            sport = match_data.get('sport', 'unknown')
            match_type = match_data.get('type', 'prematch').lower()

            if sport not in self.bookmaker_data[bookmaker][match_type]:
                self.bookmaker_data[bookmaker][match_type][sport] = {}

            self.bookmaker_data[bookmaker][match_type][sport][
                match_id] = match_data

    async def delete_match_values(self, bookmaker: str, match_id: str):
        prefix = f"{bookmaker}_{match_id}_"
        async with self.values_lock:
            keys_to_delete = [key for key in self.values if
                              key.startswith(prefix)]
            for key in keys_to_delete:
                del self.values[key]
            # Логирование при необходимости
            # logger.debug(f"Deleted {len(keys_to_delete)} values for match {match_id} by {bookmaker}")

    async def analyze_loop(self):
        while True:
            try:
                # Получаем копию данных под блокировкой
                async with self.bookmaker_data_lock:
                    pinnacle_data = self.bookmaker_data.get('pinnacle',
                                                            {}).copy()
                    other_bookmaker_data = {
                        bookmaker: self.bookmaker_data.get(bookmaker,
                                                           {}).copy()
                        for bookmaker in self.config
                        if bookmaker != 'pinnacle' and self.config[bookmaker][
                            'enabled']
                    }

                for bookmaker, other_data in other_bookmaker_data.items():
                    await self.analyze_bookmaker(pinnacle_data, other_data,
                                                 bookmaker)
                print("Analyzed all bookmakers")
                # Signal that data has been updated
                self.data_updated_event.set()
                # await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error in analyze_loop: {e}")
            await asyncio.sleep(2)

    async def analyze_bookmaker(self, pinnacle_data: Dict[str, Any],
                                other_data: Dict[str, Any], bookmaker: str):
        match_finder = self.match_finders[bookmaker]
        current_time = time.time()

        for match_type in ['live', 'prematch']:
            max_age = self.LIVE_MAX_AGE if match_type == 'live' else self.PREMATCH_MAX_AGE

            # Собираем все виды спорта
            pinnacle_sports = pinnacle_data.get(match_type, {}).keys()
            other_sports = other_data.get(match_type, {}).keys()
            all_sports = set(pinnacle_sports).union(other_sports)

            for sport in all_sports:
                if not sport or sport == 'unknown':
                    continue

                fresh_pinnacle_data = pinnacle_data.get(match_type, {}).get(
                    sport, {})
                fresh_other_data = other_data.get(match_type, {}).get(sport,
                                                                      {})

                logger.info(
                    f"Analyzing {len(fresh_pinnacle_data)} {match_type} {sport} matches from Pinnacle for {bookmaker}"
                )
                set_to_delete = set()
                for pinnacle_id, pinnacle_match in fresh_pinnacle_data.items():
                    corresponding_match = match_finder.find_corresponding_match(
                        pinnacle_match,
                        fresh_other_data)

                    if corresponding_match:
                        # print(corresponding_match)
                        other_id = corresponding_match['id']
                        await self.analyze_match(
                            bookmaker, pinnacle_id, pinnacle_match, other_id,
                            corresponding_match
                        )
                    else:
                        # print("No corresponding match found")
                        set_to_delete.add((bookmaker, pinnacle_id))
                        # await self.delete_match_by_pinnacle_id(bookmaker,
                        #                                        pinnacle_id)

                if set_to_delete:
                    await self.delete_matches_by_pinnacle_ids(bookmaker,
                                                              set_to_delete)

                logger.info(
                    f"Finished analyzing {len(fresh_pinnacle_data)} {match_type} {sport} matches from Pinnacle for {bookmaker}"
                )

    async def delete_matches_by_pinnacle_ids(self, bookmaker: str,
                                             pinnacle_ids: set):
        if not pinnacle_ids:
            return
        async with self.values_lock:
            keys_to_delete = [
                key for key, value in self.values.items()
                if value.get('bookmaker') == bookmaker and value.get(
                    'pinnacle_id') in pinnacle_ids
            ]
            for key in keys_to_delete:
                del self.values[key]
            logger.debug(
                f"Deleted {len(keys_to_delete)} values for {len(pinnacle_ids)} Pinnacle matches by {bookmaker}")

    async def delete_match_by_pinnacle_id(self, bookmaker: str,
                                          pinnacle_id: str):
        async with self.values_lock:
            keys_to_delete = [
                key for key, value in self.values.items()
                if value.get('pinnacle_id') == pinnacle_id and value.get(
                    'bookmaker') == bookmaker
            ]
            for key in keys_to_delete:
                del self.values[key]
            # Логирование при необходимости
            # logger.debug(f"Deleted {len(keys_to_delete)} values for Pinnacle match {pinnacle_id} by {bookmaker}")

    def filter_fresh_data(self, data: Dict[str, Any], current_time: float,
                          max_age: float) -> Dict[str, Any]:
        return {
            match_id: match_data
            for match_id, match_data in data.items()
            if current_time - match_data.get('time', 0) <= max_age
        }

    async def analyze_match(self, bookmaker: str, pinnacle_id: str,
                            pinnacle_match: Dict[str, Any],
                            other_id: str, other_match: Dict[str, Any]):
        if not pinnacle_match.get('outcomes') or not other_match.get(
                'outcomes'):
            await self.delete_match_values(bookmaker, other_id)
            return

        pinnacle_outcomes = {(o['type'], o['line']): o for o in
                             pinnacle_match.get('outcomes', [])}
        other_outcomes = {(o['type'], o['line']): o for o in
                          other_match.get('outcomes', [])}

        common_outcomes = set(pinnacle_outcomes.keys()) & set(
            other_outcomes.keys())

        update_tasks = []
        for outcome_key in common_outcomes:
            pinnacle_outcome = pinnacle_outcomes[outcome_key]
            other_outcome = other_outcomes[outcome_key]
            key = f"{bookmaker}_{other_id}_{pinnacle_outcome['type']}_{pinnacle_outcome['line']}"
            yield_value = self.calculate_yield(pinnacle_outcome,
                                               other_outcome)
            if yield_value is None:
                continue
            await self.update_value(
                bookmaker, pinnacle_id, other_id, pinnacle_match, other_match,
                pinnacle_outcome, other_outcome, yield_value, key
            )

    def calculate_yield(self, pinnacle_outcome: Dict[str, Any],
                        other_outcome: Dict[str, Any]) -> float:
        pinnacle_odds = pinnacle_outcome.get('odds', 0)
        other_odds = other_outcome.get('odds', 0)

        if pinnacle_odds < self.MIN_ODDS or pinnacle_odds > self.MAX_ODDS:
            return None

        extra_percent = self.get_extra_percent(pinnacle_odds)
        yield_value = (other_odds / (
                pinnacle_odds * self.MARGIN * extra_percent) - 1) * 100
        return yield_value

    def get_extra_percent(self, odds: float) -> float:
        for start, end, percent in self.extra_percents:
            if start < odds <= end:
                return percent
        return 1.0

    async def update_value(self, bookmaker: str, pinnacle_id: str,
                           other_id: str,
                           pinnacle_match: Dict[str, Any],
                           other_match: Dict[str, Any],
                           pinnacle_outcome: Dict[str, Any],
                           other_outcome: Dict[str, Any],
                           yield_value: float, key: str):
        current_time = time.time()
        async with self.values_lock:
            if key not in self.values:
                self.values[key] = {
                    'pinnacle_id': pinnacle_id,
                    'other_id': other_id,
                    'bookmaker': bookmaker,
                    'home_team': pinnacle_match['home_team'],
                    'away_team': pinnacle_match['away_team'],
                    'outcome': pinnacle_outcome['type'],
                    'betOfferId': other_outcome.get('betOfferId'),
                    'id': other_outcome.get('id'),
                    'line': pinnacle_outcome['line'],
                    'start_time': current_time,
                    'positive_start_time': current_time if yield_value > 0 else None,
                    "match_start_time": pinnacle_match.get('start_time'),
                    'sport': pinnacle_match['sport'],
                    'league': other_match.get('league',
                                              pinnacle_match.get('league')),
                    'league_pin': pinnacle_match.get('league'),
                    'country': pinnacle_match['country'],
                    'criterion': other_outcome.get('criterion'),
                    'path': other_outcome.get('path'),
                    'home_team_other': other_match['home_team'],
                    'away_team_other': other_match['away_team'],
                    'type': other_outcome.get('type'),
                    'type_event': pinnacle_match.get('type',
                                                     'prematch').lower(),
                    'single': other_outcome.get('single', True),
                    # Pinnacle-specific data
                    'line_id': pinnacle_outcome.get('line_id'),
                    'alt_line_id': pinnacle_outcome.get('alt_line_id'),
                    'period_number': pinnacle_outcome.get('period_number'),
                    'team': pinnacle_outcome.get('team'),
                    'side': pinnacle_outcome.get('side'),
                    'bet_type': pinnacle_outcome.get('bet_type'),
                }

            value = self.values[key]
            # print(pinnacle_match)
            value.update({
                'type_event': pinnacle_match.get('type',
                                                 'prematch').lower(),
                'pinnacle_odds': pinnacle_outcome['odds'],
                'other_odds': other_outcome['odds'],
                'yield': yield_value,
                'last_update_time': current_time,
                'betOfferId': other_outcome.get('betOfferId'),
                'id': other_outcome.get('id'),
                'criterion': other_outcome.get('criterion'),
                # Обновляем Pinnacle-специфичные данные, если они изменились
                'line_id': pinnacle_outcome.get('line_id'),
                'alt_line_id': pinnacle_outcome.get('alt_line_id'),
                'period_number': pinnacle_outcome.get('period_number'),
                'team': pinnacle_outcome.get('team'),
                'side': pinnacle_outcome.get('side'),
                'bet_type': pinnacle_outcome.get('bet_type'),
                # Для гандикапа
                'absolute_line': other_outcome.get('absolute_line'),
            })

            if yield_value > 0 and value.get('positive_start_time') is None:
                value['positive_start_time'] = current_time

            if yield_value <= 0:
                value['positive_start_time'] = None

    async def start_websocket_server(self):
        server = await websockets.serve(
            self.websocket_handler,
            "localhost",
            8765,
            max_size=None,
            ping_interval=10,
            ping_timeout=10
        )
        logger.info("WebSocket server started on ws://localhost:8765")
        await server.wait_closed()

    async def broadcast_data_loop(self):
        while True:
            await self.data_updated_event.wait()
            self.data_updated_event.clear()

            async with self.values_lock:
                data = list(self.values.values())
            logger.info(f"Broadcasting data len {len(data)}")
            json_data = json.dumps(data, default=str)

            async with self.clients_lock:
                clients = list(self.connected_clients)

            if not clients:
                continue  # Нет подключенных клиентов, пропускаем отправку

            # Создаем задачи для отправки данных всем клиентам параллельно
            send_tasks = [
                self.send_to_client(client, json_data)
                for client in clients
            ]

            # Запускаем все задачи параллельно и ждем их завершения
            await asyncio.gather(*send_tasks, return_exceptions=True)

    async def send_to_client(self, client, data):
        try:
            await client.send(data)

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Клиент отключился: {client.remote_address}")
            async with self.clients_lock:
                self.connected_clients.remove(client)
        except Exception as e:
            logger.error(
                f"Ошибка при отправке данных клиенту {client.remote_address}: {e}")

    async def cleanup_bookmaker_data_loop(self):
        while True:
            try:
                current_time = time.time()
                async with self.bookmaker_data_lock:
                    for bookmaker in self.bookmaker_data:
                        for match_type in ['live', 'prematch']:
                            sports = list(self.bookmaker_data[bookmaker][
                                              match_type].keys())
                            for sport in sports:
                                matches = \
                                    self.bookmaker_data[bookmaker][
                                        match_type][
                                        sport]
                                match_ids = list(matches.keys())
                                for match_id in match_ids:
                                    match_data = matches[match_id]
                                    match_time = match_data.get('time', 0)
                                    max_age = self.LIVE_MAX_AGE if match_type == 'live' else self.PREMATCH_MAX_AGE
                                    if current_time - match_time > max_age:
                                        del matches[match_id]
                                # Удаляем пустые виды спорта
                                if not matches:
                                    del self.bookmaker_data[bookmaker][
                                        match_type][sport]
            except Exception as e:
                logger.error(f"Error in cleanup_bookmaker_data_loop: {e}")
            await asyncio.sleep(2)  # Периодичность очистки

    async def cleanup_values_loop(self):
        while True:
            try:
                current_time = time.time()
                async with self.values_lock:
                    keys_to_delete = [
                        key for key, value in self.values.items()
                        if not self.is_value_recent(value, current_time)
                    ]
                    for key in keys_to_delete:
                        del self.values[key]
                    if keys_to_delete:
                        logger.debug(
                            f"Cleaned up {len(keys_to_delete)} outdated values")

            except Exception as e:
                logger.error(f"Error in cleanup_values_loop: {e}")
            await asyncio.sleep(
                2)  # Период очистки, например, каждые 2 секунд

    async def websocket_handler(self, websocket, path):
        logger.info(f"New client connected: {websocket.remote_address}")
        try:
            async with self.clients_lock:
                self.connected_clients.add(websocket)
            while True:
                try:
                    await asyncio.sleep(1)
                except websockets.exceptions.ConnectionClosed:
                    logger.info(
                        f"Client disconnected: {websocket.remote_address}")
                    break
                except Exception as e:
                    logger.error(f"Error in websocket_handler: {e}")
        except Exception as e:
            logger.error(f"Unhandled error in websocket_handler: {e}")
        finally:
            async with self.clients_lock:
                self.connected_clients.discard(websocket)
            logger.info(
                f"Client connection closed: {websocket.remote_address}")

    def is_value_recent(self, value: Dict[str, Any],
                        current_time: float) -> bool:

        update_age = current_time - value['last_update_time']
        # print(value.get('type_event'))
        if value.get('is_live', False) or value.get('type_event',
                                                    '') == 'live':
            # print(value.get('type_event'))
            return update_age <= self.LIVE_MAX_AGE
        else:
            return update_age <= self.PREMATCH_MAX_AGE


async def main():
    analyzer = AdvancedAnalyzer('bookmakers.json')
    await analyzer.start()


if __name__ == "__main__":
    asyncio.run(main())
