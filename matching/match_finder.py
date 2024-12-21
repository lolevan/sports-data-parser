import json
import os
import logging
import time
from typing import Dict, Any, Optional


class MatchFinder:
    def __init__(self, bookmaker: str):
        self.bookmaker = bookmaker
        self.matched_events_path = self.get_matched_events_path()
        self.matched_events = self.load_matched_events()
        self.matched_events_dict = self.create_matched_events_dict()
        self.time_of_last_update = time.time()

    def get_matched_events_path(self) -> str:
        """
        Получает путь к файлу matched_events.json для указанного букмекера.
        """
        # Получаем абсолютный путь к директории, где находится текущий файл (match_finder.py)
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Переходим вверх, чтобы достичь корневой директории проекта
        project_root = os.path.dirname(current_dir)

        # Формируем путь к файлу matched_events.json
        base_path = os.path.join(project_root, 'matching',
                                 'bookmaker_mappings', self.bookmaker)
        file_path = os.path.join(base_path, 'matched_events.json')

        # Проверяем существование файла
        if not os.path.exists(file_path):
            logging.warning(f"Файл {file_path} не найден.")
        else:
            logging.info(f"Файл найден: {file_path}")

        return file_path

    def load_matched_events(self) -> list:
        """
        Загружает события из файла matched_events.json.
        """
        try:
            if os.path.exists(self.matched_events_path):
                with open(self.matched_events_path, 'r') as f:
                    events = json.load(f)
                logging.info(
                    f"Загружено {len(events)} событий из файла {self.matched_events_path}")
                return events
            else:
                logging.error(f"Файл {self.matched_events_path} не найден.")
                return []
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных: {e}")
            return []

    def create_matched_events_dict(self) -> Dict:
        """
        Создает словарь для быстрого поиска соответствий матчей.
        Ключом является кортеж (league, country, home_team, away_team).
        """
        matched_events_dict = {}
        for event in self.matched_events:
            pinnacle_key = (
                event['pinnacle_league'],
                event['country'],
                event['pinnacle_home_team'],
                event['pinnacle_away_team']
            )
            # Проверяем уникальность ключа
            # if pinnacle_key in matched_events_dict:
            #     logging.warning(f"Дублирующий ключ найден: {pinnacle_key}. Перезапись существующего события.")
            matched_events_dict[pinnacle_key] = event
        return matched_events_dict

    def reload_matched_events(self):
        """
        Перезагружает события из файла и обновляет словарь соответствий.
        """
        self.matched_events = self.load_matched_events()
        self.matched_events_dict = self.create_matched_events_dict()
        self.time_of_last_update = time.time()
        logging.info(f"Перезагружены события для букмекера {self.bookmaker}.")

    def find_corresponding_match(self, pinnacle_match: Dict[str, Any],
                                 other_bookmaker_data: Dict[str, Any]) -> \
            Optional[Dict[str, Any]]:
        """
        Ищет соответствующий матч для данного Pinnacle матча.
        """
        # Проверяем, необходимо ли обновить данные
        if time.time() - self.time_of_last_update > 60:
            self.reload_matched_events()

        pinnacle_key = (
            pinnacle_match['league'],
            pinnacle_match['country'],
            pinnacle_match['home_team'],
            pinnacle_match['away_team']
        )

        event = self.matched_events_dict.get(pinnacle_key)
        if event:
            other_id = event['other_id']
            if other_id in other_bookmaker_data:
                other_match = other_bookmaker_data[other_id]
                other_match['id'] = other_id  # Добавляем 'id' к other_match для совместимости
                # logging.debug(
                #     f"Найдено соответствие: Pinnacle {pinnacle_key} -> {self.bookmaker} {event.get('other_match_name', 'Unknown')}")
                return other_match

        # logging.debug(f"Не найдено соответствие для Pinnacle матча: {pinnacle_key}")
        return None

    def find_corresponding_match_by_id(self, pinnacle_id: str) -> Optional[Dict[str, Any]]:
        """
        Ищет соответствующий матч по идентификатору Pinnacle.
        """
        # Проверяем, необходимо ли обновить данные
        if time.time() - self.time_of_last_update > 60:
            self.reload_matched_events()

        for event in self.matched_events:
            if event['pinnacle_id'] == pinnacle_id:
                # logging.debug(f"Найдено соответствие по ID: {pinnacle_id} -> {event['other_id']}")
                return event['other_id']
        # logging.debug(f"Не найдено соответствие по ID: {pinnacle_id}")
        return None
