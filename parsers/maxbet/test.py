from geopy.geocoders import Nominatim
from functools import lru_cache

# Инициализируем геокодер
geolocator = Nominatim(user_agent="tennis_league_processor")


@lru_cache(maxsize=1000)
def get_country_by_city(city: str) -> str:
    """
    Получает страну по названию города с помощью geopy.

    :param city: Название города
    :return: Страна, в которой находится город
    """
    # location = geolocator.geocode(city, language='en', exactly_one=False)
    location = geolocator.geocode(city, language='en')
    if location:
        # Возвращаем страну из полученной информации
        # print(location[-1].address.split(","))
        # # print(location.address.split(","))
        # return location[-1].address.split(",")[-1].strip()

        country = location.address.split(",")[-1].strip()

        # Обрабатываем специальные случаи
        if country == "United States":
            return "USA"
        return country

    return "World"


# Пример использования
league_name = "Madrid Open"
city = 'Maia'
country = get_country_by_city(city)
print(f"City: {city}, Country: {country}")

#live
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
    pick_type = parts[2]  # Например, '1', 'X', '2', 'over', 'under'

    elem_outcomes = {
        "type_name": market_key,
        "type": pick_type,
        "line": 0.0,
        "odds": float(odd_value)
    }

    # Преобразуем market_key и pick_type в понятные названия
    if market_key in ["ft", "fr"]:
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
    else:
        # Если тип рынка не распознан, оставляем оригинальные данные
        elem_outcomes["type_name"] = market_key
        elem_outcomes["type"] = pick_type

    return elem_outcomes


#prematch
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
    if market_key == "dw":
        elem_outcomes["type_name"] = "Draw"
    elif market_key == "fs":
        elem_outcomes["type_name"] = "Full Time Score"
    elif market_key == "ht":
        elem_outcomes["type_name"] = "Half Time"
    elif market_key == "sh":
        elem_outcomes["type_name"] = "Second Half"
    elif market_key == "dc":
        elem_outcomes["type_name"] = "Double Chance"
    elif market_key == "oe":
        elem_outcomes["type_name"] = "Odd/Even"
    elif market_key == "tg":
        elem_outcomes["type_name"] = "Total Goals"
        if pick_type.endswith('+'):
            try:
                elem_outcomes["line"] = float(pick_type[:-1])
            except ValueError:
                elem_outcomes["line"] = 0.0
    elif market_key == "atg":
        elem_outcomes["type_name"] = "Alternative Total Goals"
        if pick_type.endswith('+'):
            try:
                elem_outcomes["line"] = float(pick_type[:-1])
            except ValueError:
                elem_outcomes["line"] = 0.0
    elif market_key == "cr":
        elem_outcomes["type_name"] = "Correct Score"
        elem_outcomes["type"] = f"{parts[2]}:{parts[3]}" if len(parts) > 3 else "Unknown"
    elif market_key == "gg":
        elem_outcomes["type_name"] = "Both Teams to Score"
    elif market_key == "fhg":
        elem_outcomes["type_name"] = "First Half Goals"
        if pick_type.endswith('+'):
            try:
                elem_outcomes["line"] = float(pick_type[:-1])
            except ValueError:
                elem_outcomes["line"] = 0.0
    elif market_key == "shg":
        elem_outcomes["type_name"] = "Second Half Goals"
        if pick_type.endswith('+'):
            try:
                elem_outcomes["line"] = float(pick_type[:-1])
            except ValueError:
                elem_outcomes["line"] = 0.0
    elif market_key == "1s":  # Победитель 1-го сета
        elem_outcomes["type_name"] = "Set Winner"
        elem_outcomes["set_number"] = 1
    elif market_key == "2s":  # Победитель 2-го сета
        elem_outcomes["type_name"] = "Set Winner"
        elem_outcomes["set_number"] = 2
    elif market_key == "Full Time Score":  # Победитель матча
        elem_outcomes["type_name"] = "Match Winner"
    elif market_key == "hg":  # Гандикап по геймам
        elem_outcomes["type_name"] = "Game Handicap"
        team, line = pick_type.split('|')
        elem_outcomes["type"] = team
        elem_outcomes["line"] = float(line)
    elif market_key == "g":  # Тотал геймов
        elem_outcomes["type_name"] = "Total Games"
        side, line = pick_type.split('|')
        elem_outcomes["type"] = side  # '+' или '-'
        elem_outcomes["line"] = float(line)
    else:
        # Если тип рынка не распознан, оставляем оригинальные данные
        elem_outcomes["type_name"] = market_key
        elem_outcomes["type"] = pick_type

    return elem_outcomes
