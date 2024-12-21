from typing import List

import websockets

import config
from my_utils import *

logging.basicConfig(level=getattr(logging, config.LOGGING_LEVEL),
                    format=config.LOGGING_FORMAT)


class RateLimiter:

    def __init__(self):
        self.max_requests = config.RATE_LIMIT
        self.period = config.RATE_LIMIT_PERIOD
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()

            # Удаляем устаревшие запросы
            self.requests = [req for req in self.requests if
                             now - req < self.period]

            if len(self.requests) >= self.max_requests:
                # Если достигнут лимит, ждем до освобождения места
                wait_time = self.requests[0] + self.period - now
                await asyncio.sleep(wait_time)

            # Добавляем текущий запрос
            self.requests.append(now)


class BettingSystem:
    def __init__(self):
        self.clients = set()
        # self.last_update_time = {}

    async def register(self, websocket):
        self.clients.add(websocket)
        logging.info(
            f"Новый клиент подключен. Всего клиентов: {len(self.clients)}")

    async def unregister(self, websocket):
        self.clients.remove(websocket)
        logging.info(
            f"Клиент отключен. Осталось клиентов: {len(self.clients)}")

    async def broadcast_odds(self, odds_data):
        if not self.clients:
            logging.info("Нет подключенных клиентов для отправки данных.")
            return

        logging.info(f"Отправка данных {len(odds_data)}")
        message = json.dumps(odds_data)
        await asyncio.gather(
            *[self.send_data_to_client(client, message) for client in
              self.clients]
        )
        logging.info("Завершена отправка данных клиентам")

    async def send_data_to_client(self, websocket, message):
        try:
            await websocket.send(message)
        except websockets.exceptions.ConnectionClosed:
            logging.warning(
                f"Соединение с клиентом закрыто. Удаление клиента.")
            await self.unregister(websocket)

    async def fetch_and_save_odds(self, sport: str, mode: str):
        sleep_time = config.LIVE_UPDATE_INTERVAL if mode == "Live" else config.PRE_MATCH_UPDATE_INTERVAL
        rate_limiter = RateLimiter()
        is_live = mode == "Live"

        while True:
            try:
                start_time = time.time()
                event_ids = self.get_all_event_ids(sport, is_live)

                if not event_ids:
                    await asyncio.sleep(0.2)
                    continue

                for i in range(0, len(event_ids), config.EVENTS_BATCH_SIZE):
                    batch_event_ids = event_ids[
                                      i:i + config.EVENTS_BATCH_SIZE]
                    await rate_limiter.acquire()

                    res = await pinMarket.get_events_odds(
                        sportid=SPORT_IDs[sport],
                        live=1 if is_live else 0,
                        eventIds=batch_event_ids,
                        since=0
                    )
                    # print("res:", res)

                    if res and 'leagues' in res:
                        processed_odds = {}
                        for league in res.get('leagues', []):
                            for event in league.get('events', []):
                                event_id = str(event['id'])
                                processed_event = process_match_data(event,
                                                                     is_live)
                                if processed_event:
                                    processed_odds[event_id] = processed_event

                        for event_id_ in batch_event_ids:
                            event_id_ = str(event_id_)
                            if event_id_ not in processed_odds:
                                processed_odds[
                                    event_id_] = process_match_data(
                                    {"id": int(event_id_)}, is_live
                                )
                        # print(processed_odds)
                        await self.broadcast_odds(processed_odds)

                        # # Обновление времени последнего обновления для обработанных событий
                        # for league in res.get('leagues', []):
                        #     for event in league.get('events', []):
                        #         self.last_update_time[str(event[
                        #                                       'id'])] = asyncio.get_event_loop().time()

            except Exception as e:
                logging.error(
                    f"Ошибка при получении коэффициентов для {sport} ({mode}): {e}",
                    exc_info=True)

            end_time = time.time()
            elapsed_time = end_time - start_time
            remaining_time = max(0, sleep_time - elapsed_time)
            await asyncio.sleep(remaining_time)

    def get_all_event_ids(self, sport: str, is_live: bool) -> List[str]:
        global matches_data, matches_data_live
        if is_live:
            events = matches_data_live.get(sport, {}).get("events", {})
        else:
            events = matches_data.get(sport, {}).get("events", {})

        if not isinstance(events, dict):
            logging.error(f"Неожиданный тип данных событий: {type(events)}")
            return []

        return [str(event_id) for event_id in events]


betting_system = BettingSystem()


async def websocket_handler(websocket, path):
    await betting_system.register(websocket)
    try:
        async for message in websocket:
            # Здесь можно добавить обработку входящих сообщений от клиентов, если это необходимо
            pass
    finally:
        await betting_system.unregister(websocket)


async def main():
    server = await websockets.serve(
        websocket_handler,
        config.SOCKET_SERVER_HOST,
        config.SOCKET_SERVER_PORT,
        max_size=None
    )
    logging.info(
        f"WebSocket сервер запущен на {config.SOCKET_SERVER_HOST}:{config.SOCKET_SERVER_PORT}")

    tasks = [
        asyncio.create_task(fetch_and_save_matches()),
        asyncio.create_task(fetch_and_save_matches_live()),
        asyncio.create_task(server.wait_closed())
    ]

    for sport, mode in config.SPORTS_TO_RUN:
        if mode in ["PreMatch", "Both"]:
            tasks.append(asyncio.create_task(
                betting_system.fetch_and_save_odds(sport, "PreMatch")))
        if mode in ["Live", "Both"]:
            tasks.append(asyncio.create_task(
                betting_system.fetch_and_save_odds(sport, "Live")))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.info("Получен сигнал на завершение работы.")
    finally:
        server.close()
        await server.wait_closed()
        logging.info("Сервер успешно остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем.")
