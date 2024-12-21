import asyncio
import logging
import websockets
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class WebSocketClient:
    def __init__(self, bookmakers_config):
        self.bookmakers = bookmakers_config
        self.data = {}

    async def connect_to_pinnacle(self, config):
        uri = f"ws://localhost:{config['port']}"
        logging.info(f"Попытка подключения к Pinnacle: {uri}")
        while True:
            try:
                async with websockets.connect(uri, ping_interval=30,
                                              ping_timeout=90,
                                              close_timeout=60, max_size=None,
                                              open_timeout=60) as websocket:
                    logging.info("Подключено к Pinnacle")
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(),
                                                             timeout=90)
                            data = json.loads(message)
                            self.process_pinnacle_data(data)
                            logging.info(
                                f"Получены данные от Pinnacle: {len(message)} байт")
                        except asyncio.TimeoutError:
                            logging.warning(
                                "Нет данных от Pinnacle в течение 90 секунд. Проверка соединения...")
                            try:
                                pong = await websocket.ping()
                                await asyncio.wait_for(pong, timeout=15)
                                logging.info("Соединение с Pinnacle активно")
                            except Exception as e:
                                logging.error(
                                    f"Соединение с Pinnacle потеряно: {str(e)}. Переподключение...")
                                break
                        except websockets.exceptions.ConnectionClosed as e:
                            logging.error(
                                f"Соединение с Pinnacle закрыто: {str(e)}. Переподключение...")
                            break
                        except json.JSONDecodeError as e:
                            logging.error(
                                f"Ошибка декодирования JSON от Pinnacle: {str(e)}")
                        except Exception as e:
                            logging.error(
                                f"Неожиданная ошибка при работе с Pinnacle: {str(e)}",
                                exc_info=True)
            except websockets.exceptions.WebSocketException as e:
                logging.error(
                    f"Ошибка WebSocket при подключении к Pinnacle: {str(e)}")
            except asyncio.TimeoutError:
                logging.error(
                    "Таймаут при подключении к Pinnacle. Повторная попытка...")
            except Exception as e:
                logging.error(
                    f"Ошибка подключения к Pinnacle: {str(e)}. Повторная попытка через 10 секунд...",
                    exc_info=True)
            await asyncio.sleep(10)

    def process_pinnacle_data(self, data):
        if 'pinnacle' not in self.data:
            self.data['pinnacle'] = {}

        for match_id, match_data in data.items():
            if not match_data:
                continue
            if match_data.get('type') == 'PreMatch':
                sport = match_data.get('sport', 'Unknown')
                if sport not in self.data['pinnacle']:
                    self.data['pinnacle'][sport] = {}
                self.data['pinnacle'][sport][match_id] = match_data

    async def connect_to_bookmaker(self, name, config):
        uri = f"ws://localhost:{config['port']}"
        logging.info(f"Попытка подключения к {name}: {uri}")
        while True:
            try:
                async with websockets.connect(uri, ping_interval=20,
                                              ping_timeout=60,
                                              close_timeout=30, max_size=None,
                                              open_timeout=30) as websocket:
                    logging.info(f"Подключено к {name}")
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(),
                                                             timeout=30)
                            info = json.loads(message)
                            self.process_bookmaker_data(name, info)
                            logging.info(
                                f"Получены данные от {name}: {len(message)} байт")
                        except asyncio.TimeoutError:
                            logging.warning(
                                f"Нет данных от {name} в течение 30 секунд. Проверка соединения...")
                            try:
                                pong = await websocket.ping()
                                await asyncio.wait_for(pong, timeout=10)
                                logging.info(f"Соединение с {name} активно")
                            except:
                                logging.error(
                                    f"Соединение с {name} потеряно. Переподключение...")
                                break
                        except websockets.exceptions.ConnectionClosed:
                            logging.error(
                                f"Соединение с {name} закрыто. Переподключение...")
                            break
                        except json.JSONDecodeError as e:
                            logging.error(
                                f"Ошибка декодирования JSON от {name}: {str(e)}")
                        except Exception as e:
                            logging.error(
                                f"Неожиданная ошибка при работе с {name}: {str(e)}",
                                exc_info=True)
            except websockets.exceptions.WebSocketException as e:
                logging.error(
                    f"Ошибка WebSocket при подключении к {name}: {str(e)}")
            except asyncio.TimeoutError:
                logging.error(
                    f"Таймаут при подключении к {name}. Повторная попытка...")
            except Exception as e:
                logging.error(
                    f"Ошибка подключения к {name}: {e}. Повторная попытка через 5 секунд...",
                    exc_info=True)
            await asyncio.sleep(5)

    def process_bookmaker_data(self, bookmaker, data):
        if bookmaker not in self.data:
            self.data[bookmaker] = {}

        for match_id, match_data in data.items():
            if match_data.get('type', '').lower() != 'live':
                sport = match_data.get('sport', 'Unknown')
                if sport not in self.data[bookmaker]:
                    self.data[bookmaker][sport] = {}
                self.data[bookmaker][sport][match_id] = match_data

    async def start(self):
        tasks = []
        for name, config in self.bookmakers.items():
            if config['enabled']:
                if name == 'pinnacle':
                    tasks.append(
                        asyncio.create_task(self.connect_to_pinnacle(config)))
                else:
                    tasks.append(asyncio.create_task(
                        self.connect_to_bookmaker(name, config)))
        await asyncio.gather(*tasks)

    def get_data(self, bookmaker, sport=None):
        if sport:
            return self.data.get(bookmaker, {}).get(sport, {})
        return self.data.get(bookmaker, {})


class PinnacleDataManager:
    def __init__(self, max_age_minutes=0.35):
        self.pinnacle_matches = {}
        self.max_age = timedelta(minutes=max_age_minutes).total_seconds()

    def update_matches(self, new_matches):
        # logging.info(f"Обновление матчей Pinnacle. Тип данных: {type(new_matches)}")

        current_time = datetime.now().timestamp()
        if isinstance(new_matches, dict):
            for sport, sport_data in new_matches.items():
                if sport not in self.pinnacle_matches:
                    self.pinnacle_matches[sport] = {}
                for match_id, match_data in sport_data.items():
                    if match_data.get('type') == 'PreMatch':
                        self.pinnacle_matches[sport][match_id] = {
                            **match_data,
                            'last_updated': current_time
                        }
            logging.info(f"Обновлено {sum(len(sport_data) for sport_data in self.pinnacle_matches.values())} матчей Pinnacle")
        else:
            logging.error(f"Ошибка: новые матчи Pinnacle должны быть в формате словаря, получено {type(new_matches)}")

        # Удаление старых матчей
        old_count = sum(len(sport_data) for sport_data in self.pinnacle_matches.values())
        for sport in self.pinnacle_matches:
            self.pinnacle_matches[sport] = {
                match_id: match_data
                for match_id, match_data in self.pinnacle_matches[sport].items()
                if current_time - match_data['last_updated'] <= self.max_age
            }
        new_count = sum(len(sport_data) for sport_data in self.pinnacle_matches.values())
        logging.info(f"Удалено {old_count - new_count} устаревших матчей Pinnacle")

    def get_matches(self):
        return self.pinnacle_matches