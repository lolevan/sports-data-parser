import asyncio
import logging
import time
import json
import os
import aiohttp
import websockets
from parsers.maxbet.live import LiveOddsParser
from parsers.maxbet.prematch import PreMatchOddsParser
from parsers.utils import save_odds_to_jsonl

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S'
)

BOOKIE = 'maxbet_me'  # Идентификатор букмекера

# Интервалы обновления
LIVE_UPDATE_INTERVAL = 2.0
PREMATCH_UPDATE_INTERVAL = 16.0

# Директория для логирования данных
LOG_DIR = 'odds_data'
os.makedirs(LOG_DIR, exist_ok=True)


class MaxbetClient:
    def __init__(self):
        self.live_parser = LiveOddsParser()
        self.prematch_parser = PreMatchOddsParser()
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.connected_clients = set()
        self.lock = asyncio.Lock()

    async def get_live_odds(self) -> dict:
        """Получает live коэффициенты и обновляет внутренние структуры."""
        live_matches = await self.live_parser.get_live_odds()
        async with self.lock:
            self.ALL_MATCHES.update(self.live_parser.ALL_MATCHES)
            self.parsed_matches.update(self.live_parser.parsed_matches)
        logging.info(f"Total live matches: {len(self.live_parser.ALL_MATCHES)}")
        return self.parsed_matches

    async def get_prematch_odds(self) -> dict:
        """Получает предматчевые коэффициенты и обновляет внутренние структуры."""
        prematch_matches = await self.prematch_parser.get_prematch_odds()
        async with self.lock:
            self.ALL_MATCHES.update(self.prematch_parser.ALL_MATCHES)
            self.parsed_matches.update(self.prematch_parser.parsed_matches)
        logging.info(f"Total prematch matches: {len(self.prematch_parser.ALL_MATCHES)}")
        return self.parsed_matches

    async def update_odds_periodically(self, get_odds_func, match_type: str, interval: float):
        """
        Периодически вызывает функцию получения коэффициентов, нормализует данные и рассылает их клиентам.

        :param get_odds_func: асинхронная функция для получения коэффициентов (например, get_live_odds)
        :param match_type: строка 'Live' или 'PreMatch'
        :param interval: интервал обновления в секундах
        """
        while True:
            start_time = time.time()
            try:
                odds = await get_odds_func()
                logging.info(f"{match_type} odds updated. Total {match_type.lower()} matches: {len(odds)}")
                normalized_data = await self.normalize_odds(odds, match_type=match_type)

                if self.connected_clients:
                    data_to_send = json.dumps(normalized_data)
                    # Отправляем данные всем подключенным клиентам
                    coros = [self.send_data_to_client(client, data_to_send) for client in self.connected_clients]
                    await asyncio.gather(*coros, return_exceptions=True)

            except Exception as e:
                logging.error(f"Error updating {match_type} odds: {e}")

            execution_time = time.time() - start_time
            sleep_time = max(0, interval - execution_time)
            logging.info(f"{match_type} update time: {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

    async def normalize_odds(self, matches: dict, match_type: str) -> dict:
        """
        Нормализует данные для отправки клиентам.

        :param matches: словарь матчей
        :param match_type: 'Live' или 'PreMatch'
        :return: словарь нормализованных данных
        """
        odds_data = {}
        async with self.lock:
            for match_id, match in matches.items():
                normalized_match = self.process_match_data(match, match_type)
                if normalized_match:
                    odds_data[match_id] = normalized_match
        return odds_data

    def process_match_data(self, match: dict, match_type: str) -> dict:
        """
        Обрабатывает данные одного матча.

        :param match: словарь данных матча
        :param match_type: 'Live' или 'PreMatch'
        :return: нормализованные данные матча или None
        """
        outcomes = match.get('outcomes', [])
        # Если требуется фильтровать матчи без исходов, можно раскомментировать:
        # if not outcomes:
        #     return None

        result = {
            "event_id": match['match_id'],
            "match_name": match['name'],
            "start_time": match['start_time'],  # Unix timestamp
            "home_team": match['home_team'],
            "away_team": match['away_team'],
            "league_id": match['league_id'],
            "league": match['league'],
            "country": match['country'],
            "sport": match['sport'],
            "type": match_type,
            "outcomes": outcomes,
            "time": match['time'],
            "bookmaker": BOOKIE,
        }

        # Сохраняем данные матча в файл
        filename = f"{match['home_team']} vs {match['away_team']}"
        save_odds_to_jsonl(filename, result)
        return result

    async def send_data_to_client(self, client, data: str):
        """
        Отправляет данные подключенному WebSocket клиенту.

        :param client: вебсокет клиент
        :param data: json-строка данных
        """
        try:
            await client.send(data)
        except websockets.exceptions.ConnectionClosed:
            logging.info(f"Client disconnected: {client.remote_address}")
            self.connected_clients.discard(client)

    async def websocket_handler(self, websocket, path):
        """
        Обработчик входящих WebSocket-соединений.
        """
        self.connected_clients.add(websocket)
        logging.info(f"Client connected: {websocket.remote_address}")
        try:
            await websocket.wait_closed()
        finally:
            self.connected_clients.discard(websocket)
            logging.info(f"Client disconnected: {websocket.remote_address}")

    async def run(self):
        """
        Запускает сервер и периодические обновления коэффициентов.
        """
        server = await websockets.serve(self.websocket_handler, '0.0.0.0', 6008)
        logging.info("WebSocket server started on ws://0.0.0.0:6008")

        live_update_task = asyncio.create_task(self.update_odds_periodically(self.get_live_odds, "Live", LIVE_UPDATE_INTERVAL))
        prematch_update_task = asyncio.create_task(self.update_odds_periodically(self.get_prematch_odds, "PreMatch", PREMATCH_UPDATE_INTERVAL))

        await asyncio.gather(live_update_task, prematch_update_task)

    async def reparse_match(self, match_id: str, match_type: str):
        """
        Повторно парсит данные одного матча по его ID.

        :param match_id: ID матча
        :param match_type: 'Live' или 'PreMatch'
        :return: обновленные данные или None
        """
        async with aiohttp.ClientSession() as session:
            if match_type == 'Live':
                updated_match = await self.live_parser.get_match_details(match_id, session=session)
            else:
                updated_match = await self.prematch_parser.get_match_details(match_id, session=session)

        if updated_match:
            async with self.lock:
                self.ALL_MATCHES[match_id] = updated_match
                self.parsed_matches[match_id] = updated_match
            logging.info(f"Successfully reparsed match ID {match_id}")
            return updated_match
        else:
            logging.warning(f"Failed to reparse match ID {match_id}")
            return None


if __name__ == "__main__":
    maxbet_client = MaxbetClient()
    try:
        asyncio.run(maxbet_client.run())
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
