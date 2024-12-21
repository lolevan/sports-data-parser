import json
import os
import threading
import tempfile
import shutil
import logging
from typing import Dict, Any, List, Tuple

# Настройка логирования
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Mappings:
    def __init__(self, mappings_dir: str = 'bookmaker_mappings'):
        self.mappings_dir = mappings_dir
        self.bookmaker_mappings: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.lock = threading.RLock()  # Переиспользуемая блокировка для предотвращения дедлоков
        self.load_all_mappings()

    def load_mapping(self, bookmaker: str, mapping_type: str) -> Dict[
        str, Any]:
        file_path = os.path.join(self.mappings_dir, bookmaker,
                                 f"{mapping_type}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logging.debug(f"Загружен маппинг из {file_path}")
                return data
            except Exception as e:
                logging.error(f"Ошибка при загрузке файла {file_path}: {e}")
        else:
            logging.debug(
                f"Файл {file_path} не найден. Возвращаем пустой маппинг.")
        return {}

    def load_all_mappings(self):
        if not os.path.exists(self.mappings_dir):
            os.makedirs(self.mappings_dir)
            logging.info(
                f"Создана директория для маппингов: {self.mappings_dir}")

        for bookmaker in os.listdir(self.mappings_dir):
            bookmaker_dir = os.path.join(self.mappings_dir, bookmaker)
            if os.path.isdir(bookmaker_dir):
                self.bookmaker_mappings[bookmaker] = {
                    'countries': self.load_mapping(bookmaker, 'countries'),
                    'leagues': self.load_mapping(bookmaker, 'leagues'),
                    'teams': self.load_mapping(bookmaker, 'teams')
                }
                logging.info(f"Загружены маппинги для букмекера: {bookmaker}")

    def get_mapped_value(self, bookmaker: str, mapping_type: str,
                         value: str) -> str:
        if bookmaker == 'pinnacle':
            return value
        mapped = self.bookmaker_mappings.get(bookmaker, {}).get(mapping_type,
                                                                {}).get(value,
                                                                        value)
        logging.debug(
            f"get_mapped_value: {bookmaker}, {mapping_type}, {value} -> {mapped}")
        return mapped

    def get_country(self, bookmaker: str, country: str) -> str:
        return self.get_mapped_value(bookmaker, 'countries', country)

    def get_league(self, bookmaker: str, league: str) -> str:
        return self.get_mapped_value(bookmaker, 'leagues', league)

    def get_team(self, bookmaker: str, country: str, league: str,
                 team: str) -> str:
        if bookmaker == 'pinnacle':
            return team
        country_league = f"{country}_{league}"
        mapped_team = self.bookmaker_mappings.get(bookmaker, {}).get('teams',
                                                                     {}).get(
            country_league, {}).get(team, team)
        logging.debug(
            f"get_team: {bookmaker}, {country_league}, {team} -> {mapped_team}")
        return mapped_team

    def add_mapping(self, bookmaker: str, mapping_type: str, original: str,
                    mapped: str, country: str = None, league: str = None):
        logging.debug(
            f"Начало добавления маппинга: {bookmaker}, {mapping_type}, {original} -> {mapped}")
        if original == "" or mapped == "" or country == "None":
            logging.debug(
                "Не добавляем пустые маппинги или маппинги с country='None'")
            return  # Не добавляем пустые маппинги или маппинги с country="None"

        with self.lock:
            if bookmaker not in self.bookmaker_mappings:
                self.bookmaker_mappings[bookmaker] = {}
            if mapping_type not in self.bookmaker_mappings[bookmaker]:
                self.bookmaker_mappings[bookmaker][mapping_type] = {}

            if mapping_type == 'teams':
                if country is None or league is None:
                    raise ValueError(
                        "Country and league must be provided for team mappings")
                country_league = f"{country}_{league}"
                if country_league not in self.bookmaker_mappings[bookmaker][
                    mapping_type]:
                    self.bookmaker_mappings[bookmaker][mapping_type][
                        country_league] = {}
                self.bookmaker_mappings[bookmaker][mapping_type][
                    country_league][original] = mapped
                logging.debug(
                    f"Добавлен маппинг команды: {original} -> {mapped} в {country_league}")
            else:
                self.bookmaker_mappings[bookmaker][mapping_type][
                    original] = mapped
                logging.debug(
                    f"Добавлен маппинг {mapping_type}: {original} -> {mapped}")

            self.save_mapping(bookmaker, mapping_type)
            logging.info(
                f"Добавлен маппинг для {bookmaker} {mapping_type}: {original} -> {mapped}")

    def remove_mapping(self, bookmaker: str, mapping_type: str, original: str,
                       mapped: str, country: str = None, league: str = None):
        logging.debug(
            f"Начало удаления маппинга: {bookmaker}, {mapping_type}, {original} -> {mapped}")
        with self.lock:
            if mapping_type == 'teams':
                if country is None or league is None:
                    raise ValueError(
                        "Country and league must be provided for team mappings")
                country_league = f"{country}_{league}"
                if country_league in self.bookmaker_mappings[bookmaker].get(
                        mapping_type, {}):
                    self.bookmaker_mappings[bookmaker][mapping_type][
                        country_league].pop(original, None)
                    logging.debug(
                        f"Удалён маппинг команды: {original} -> {mapped} из {country_league}")
            else:
                if original in self.bookmaker_mappings[bookmaker].get(
                        mapping_type, {}):
                    self.bookmaker_mappings[bookmaker][mapping_type].pop(
                        original, None)
                    logging.debug(
                        f"Удалён маппинг {mapping_type}: {original} -> {mapped}")

            self.save_mapping(bookmaker, mapping_type)
            logging.info(
                f"Удалён маппинг для {bookmaker} {mapping_type}: {original} -> {mapped}")

    def save_mapping(self, bookmaker: str, mapping_type: str):
        file_path = os.path.join(self.mappings_dir, bookmaker,
                                 f"{mapping_type}.json")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        logging.debug(
            f"Сохранение маппингов {mapping_type} для {bookmaker} в {file_path}")
        try:
            with self.lock:
                # Загрузка существующих маппингов
                existing_mappings = {}
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_mappings = json.load(f)
                        logging.debug(
                            f"Загружены существующие маппинги из {file_path}")

                # Обновление существующих маппингов новыми данными
                existing_mappings.update(
                    self.bookmaker_mappings[bookmaker][mapping_type])
                logging.debug(
                    f"Обновлённые маппинги для {bookmaker} {mapping_type}: {existing_mappings}")

                # Сохранение обновлённых маппингов с использованием атомарной записи
                self.save_json_file_atomic(file_path, existing_mappings)
                logging.info(
                    f"Успешно сохранены маппинги {mapping_type} для {bookmaker}")
        except Exception as e:
            logging.error(
                f"Ошибка при сохранении маппингов {mapping_type} для {bookmaker}: {e}")

    def save_matched_events(self, bookmaker: str,
                            new_matched_events: List[Dict[str, Any]]):
        file_path = os.path.join(self.mappings_dir, bookmaker,
                                 "matched_events.json")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        logging.debug(f"Сохранение сопоставленных событий в {file_path}")

        with self.lock:
            try:
                # Загрузка существующих сопоставленных событий
                existing_events = []
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_events = json.load(f)
                        logging.debug(
                            f"Загружены существующие сопоставленные события из {file_path}")

                # Добавление новых событий к существующим
                all_events = existing_events + new_matched_events
                logging.debug(
                    f"Всего событий после добавления новых: {len(all_events)}")

                # Удаление дубликатов
                unique_events = self.remove_duplicates(all_events)
                logging.debug(
                    f"Уникальных событий после удаления дубликатов: {len(unique_events)}")

                # Сохранение обновлённого списка событий с использованием атомарной записи
                self.save_json_file_atomic(file_path, unique_events)

                logging.info(
                    f"Сохранено {len(unique_events)} уникальных сопоставленных событий для {bookmaker}")
            except Exception as e:
                logging.error(
                    f"Ошибка при сохранении сопоставленных событий для {bookmaker}: {e}")

    def remove_duplicates(self, events: List[Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        unique_events = {}
        for event in events:
            key = (event['pinnacle_id'], event['other_id'])
            unique_events[key] = event
        return list(unique_events.values())

    def save_unmatched_events(self, bookmaker: str,
                              pinnacle_unmatched: Dict[int, Dict[str, Any]],
                              other_unmatched: Dict[int, Dict[str, Any]]):
        pinnacle_file_path = os.path.join(self.mappings_dir, bookmaker,
                                          "pinnacle_unmatched.json")
        other_file_path = os.path.join(self.mappings_dir, bookmaker,
                                       f"{bookmaker}_unmatched.json")

        os.makedirs(os.path.dirname(pinnacle_file_path), exist_ok=True)
        os.makedirs(os.path.dirname(other_file_path), exist_ok=True)
        logging.debug(
            f"Сохранение несопоставленных событий Pinnacle в {pinnacle_file_path} и {other_file_path}")

        with self.lock:
            try:
                # Загрузка и обновление несопоставленных событий Pinnacle
                existing_pinnacle_unmatched = self.load_json_file(
                    pinnacle_file_path)
                existing_pinnacle_unmatched.update(pinnacle_unmatched)
                logging.debug(
                    f"Обновлённые несопоставленные события Pinnacle: {len(existing_pinnacle_unmatched)}")
                self.save_json_file_atomic(pinnacle_file_path,
                                           existing_pinnacle_unmatched)

                # Загрузка и обновление несопоставленных событий другого букмекера
                existing_other_unmatched = self.load_json_file(
                    other_file_path)
                existing_other_unmatched.update(other_unmatched)
                logging.debug(
                    f"Обновлённые несопоставленные события {bookmaker}: {len(existing_other_unmatched)}")
                self.save_json_file_atomic(other_file_path,
                                           existing_other_unmatched)

                logging.info(
                    f"Сохранено {len(existing_pinnacle_unmatched)} несопоставленных событий Pinnacle и {len(existing_other_unmatched)} несопоставленных событий {bookmaker}")
            except Exception as e:
                logging.error(
                    f"Ошибка при сохранении несопоставленных событий для {bookmaker}: {e}")

    def load_json_file(self, file_path: str) -> Dict:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logging.debug(f"Загружены данные из {file_path}")
                return data
            except Exception as e:
                logging.error(f"Ошибка при загрузке файла {file_path}: {e}")
        else:
            logging.debug(
                f"Файл {file_path} не найден. Возвращаем пустой словарь.")
        return {}

    def save_json_file_atomic(self, file_path: str, data: Dict):
        dir_name = os.path.dirname(file_path)
        try:
            # Используем временный файл для атомарной записи
            with tempfile.NamedTemporaryFile('w', delete=False, dir=dir_name,
                                             encoding='utf-8') as tmp_file:
                json.dump(data, tmp_file, indent=2, ensure_ascii=False)
                temp_name = tmp_file.name
            # Перемещаем временный файл на место целевого файла
            shutil.move(temp_name, file_path)
            logging.debug(f"Атомарно сохранён файл {file_path}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении файла {file_path}: {e}")

    def load_matched_events(self, bookmaker: str) -> List[Dict[str, Any]]:
        file_path = os.path.join(self.mappings_dir, bookmaker,
                                 "matched_events.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logging.debug(
                    f"Загружены сопоставленные события из {file_path}")
                return data
            except Exception as e:
                logging.error(
                    f"Ошибка при загрузке сопоставленных событий для {bookmaker}: {e}")
        else:
            logging.debug(
                f"Файл {file_path} не найден. Возвращаем пустой список.")
        return []

    def load_unmatched_events(self, bookmaker: str) -> Tuple[
        Dict[int, Dict[str, Any]], Dict[int, Dict[str, Any]]]:
        pinnacle_file_path = os.path.join(self.mappings_dir, bookmaker,
                                          "pinnacle_unmatched.json")
        other_file_path = os.path.join(self.mappings_dir, bookmaker,
                                       f"{bookmaker}_unmatched.json")

        pinnacle_unmatched = {}
        other_unmatched = {}

        try:
            if os.path.exists(pinnacle_file_path):
                pinnacle_unmatched = self.load_json_file(pinnacle_file_path)

            if os.path.exists(other_file_path):
                other_unmatched = self.load_json_file(other_file_path)
        except Exception as e:
            logging.error(
                f"Ошибка при загрузке несопоставленных событий для {bookmaker}: {e}")

        return pinnacle_unmatched, other_unmatched


# Создание глобального экземпляра
mappings = Mappings()
