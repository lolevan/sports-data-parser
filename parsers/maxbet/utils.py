import re
from typing import Tuple, Optional
from geopy.geocoders import Nominatim
from functools import lru_cache

# Инициализируем геокодер с параметром для английского языка
geolocator = Nominatim(user_agent="tennis_league_processor")


def process_tennis_team_name(team_name: str) -> str:
    """
    Обрабатывает имя теннисного игрока или пары игроков для стандартизации.

    :param team_name: Исходное имя команды или игрока
    :return: Обработанное имя
    """
    # Убираем запятые и точки
    team_name = team_name.replace(',', '').replace('.', '').strip()

    # Проверяем на парный матч
    if '/' in team_name:
        players = team_name.split('/')
        processed_players = []
        for player in players:
            player = player.strip()
            processed_name = process_single_player_name(player)
            processed_players.append(processed_name)
        return ' / '.join(processed_players)
    else:
        # Одиночный матч
        return process_single_player_name(team_name)


def process_single_player_name(name: str) -> str:
    """
    Обрабатывает имя одиночного игрока по заданным правилам.

    :param name: Имя игрока
    :return: Обработанное имя игрока
    """
    # Разбиваем имя на части
    parts = name.strip().split()

    # Если в имени 2 и более слов и последнее слово состоит из 1 или 2 букв
    if len(parts) >= 2 and len(parts[-1]) <= 2:
        last_part = parts.pop(-1)  # Удаляем последнее слово
        # Если последнее слово состоит из 2 букв в верхнем регистре, разделяем его
        if len(last_part) == 2 and last_part.isupper():
            last_part = ' '.join(last_part)
        # Перемещаем последнее слово в начало
        parts.insert(0, last_part)
        return ' '.join(parts)

    # Если в имени 2 слова и первое слово состоит из 2 заглавных букв
    if len(parts) == 2 and len(parts[0]) == 2 and parts[0].isupper():
        # Разделяем первую часть на отдельные буквы
        first_part = ' '.join(parts[0])
        parts[0] = first_part
        return ' '.join(parts)

    # В остальных случаях возвращаем имя без изменений
    return name



def process_football_team_name(team_name: str) -> str:
    """
    Обрабатывает название футбольной команды, применяя специальные правила замены.

    :param team_name: Исходное название команды
    :return: Обработанное название команды
    """
    team_name = team_name.strip()

    # Заменяем '&' на 'and'
    team_name = team_name.replace('&', 'and')

    # Разделяем слова с точкой, например, 'F.Islands' на 'F Islands'
    team_name = team_name.replace('.', ' ')

    # Заменяем 'Utd' на 'United'
    team_name = re.sub(r'\bUtd\b', 'United', team_name, flags=re.IGNORECASE)

    # Удаляем слова, начинающиеся с 'U' и заканчивающиеся цифрами, например, 'U21'
    team_name = re.sub(r'\bU[-_]?\d+\b', '', team_name)

    # Удаляем лишние пробелы
    team_name = ' '.join(team_name.split())

    return team_name


def process_football_team_names(home_team: str, away_team: str) -> Tuple[str, str]:
    """
    Обрабатывает названия футбольных команд, применяя специальные правила замены.
    Если обе команды заканчиваются на ' W', удаляет ' W' из конца обоих названий.

    :param home_team: Название домашней команды
    :param away_team: Название гостевой команды
    :return: Кортеж с обработанными названиями команд
    """
    home_team_processed = process_football_team_name(home_team)
    away_team_processed = process_football_team_name(away_team)

    # Проверяем, заканчиваются ли обе команды на ' W'
    if home_team_processed.endswith(' W') and away_team_processed.endswith(' W'):
        home_team_processed = home_team_processed[:-2]  # Убираем ' W'
        away_team_processed = away_team_processed[:-2]  # Убираем ' W'

    return home_team_processed, away_team_processed


def swap_names(name: str) -> str:
    """
    Меняет местами имя и фамилию.

    :param name: Имя для обработки
    :return: Имя с поменянными местами именем и фамилией
    """
    parts = name.strip().split()
    if len(parts) == 2:
        # Поменять местами
        return f"{parts[1]} {parts[0]}"
    else:
        return name.strip()


def can_swap_names(home_team: str, away_team: str) -> bool:
    """
    Проверяет, можно ли менять местами имена в названиях команд.

    :param home_team: Название домашней команды
    :param away_team: Название гостевой команды
    :return: True, если можно менять местами, иначе False
    """
    # Проверяем, что в именах нет запятой
    if ',' in home_team or ',' in away_team:
        return False

    # Считаем количество слов в именах команд
    home_words = len(home_team.split())
    away_words = len(away_team.split())

    # Если количество слов разное, не меняем имена
    if home_words != away_words:
        return False

    return True


def process_league_name(league_name: str, sport: str) -> str:
    """
    Обрабатывает название лиги, применяя специальные правила замены.

    :param league_name: Исходное название лиги
    :param sport: Вид спорта ('Tennis', 'Football' и т.д.)
    :return: Обработанное название лиги
    """
    league_name = league_name.strip()

    if sport.lower() == 'tennis':
        # Если в названии есть 'Qual.', заменяем на '- Qualifiers'
        if 'Qual.' in league_name:
            league_name = league_name.replace('Qual.', '- Qualifiers')
        elif ' - ' not in league_name:
            league_name += ' - '
        return league_name
    elif sport.lower() == 'football':
        # Словарь замены для лиг (футбол)
        replacements = {
            r'\bMsfl\b': 'Liga MSFL',
            r'\bCfl\b': 'Liga CFL',
            r'\bQual\.?\b': 'Qualifiers',
            r'\bWc\b': 'World Cup',
            r'\bS\.?america\b': 'South America',
            r'\bEfl\b': 'EFL',
            r'\bwe\b': 'Women Empowerment',
            r'\bVietnam 2\b': 'Vietnam - V League 2',
            r'\bEngland 4\b': 'England - League 2',
            r'\bEngland 6 south\b': 'England - National League South',
            r'\bEngland 7 isthmian\b': 'England - Isthmian Premier League',
            r'\bEngland EFL trophy\b': 'England - EFL Trophy',
            r'\bUruguay 1\b': 'Uruguay - Primera Division',
            r'\bVietnam 1\b': 'Vietnam - V League',
            r'\bAlgeria 2\b': 'Algeria - Ligue 2',
            r'\bColombia 1 - quadrangular\b': 'Colombia - Primera A',
            r'\bCosta rica 1\b': 'Costa Rica - Primera Division',
            r'\bEngland 7 southern\b': 'England - Southern Premier League',
            # Новые пары замен
            r'\bUEFA Champions League Women\b': 'UEFA - Champions League Women',
            r'\bAlbania Cup\b': 'Albania - Cup',
            r'\bArgentina Primera C\b': 'Argentina - Primera C Metropolitana',
            r'\bArmenia 1\b': 'Armenia - Premier League',
            r'\bBolivia 1\b': 'Bolivia - Primera Division',
            r'\bBosnia & Herz\.? 1\b': 'Bosnia and Herzegovina - Premier Liga',
            r'\bBrazil 1\b': 'Brazil - Serie A',
            r'\bBrazil Camp\.? Carioca B2\b': 'Brazil - Carioca B2',
            r'\bChile Cup\b': 'Chile - Cup',
            r'\bCroatia Cup\b': 'Croatia - Cup',
            r'\bEngland Premier League Cup\b': 'England - Premier League Cup U21',
            r'\bEngland Women\'?s? League Cup\b': 'England - League Cup Women',
            r'\bGermany 4 North\b': 'Germany - Regionalliga North',
            r'\bCoppa Italia Serie D\b': 'Italy - Serie D Cup',
            r'\bLatvia 1 - Relegation\b': 'Latvia - Virsliga',
            r'\bMexico Liga Mx U23\b': 'Mexico - U23 League',
            r'\bSaudi Arabia 2\b': 'Saudi Arabia - Division 1',
            r'\bSerbia 2\b': 'Serbia - Prva Liga',
            r'\bSpain 2\b': 'Spain - Segunda Division',
            r'\bSpain 4 - Group 3\b': 'Spain - Segunda RFEF Group 3',
            r'\bSpain Tercera Rfef - Group \d+\b': 'Spain - Tercera Division',
            r'\bSpain Copa - Women\b': 'Spain - Cup Women',
            r'\bUganda 1\b': 'Uganda - Premier League',
            r'\bWales 1\b': 'Wales - Premier League',
            # Динамическая замена для Algeria U21 League X
            r'\bAlgeria U21 League (\d+)\b': r'Algeria - Ligue \1',
            # Новые лиги:
            r'\bGermany 1\b': 'Germany - Bundesliga',
            r'\bSpain 1\b': 'Spain - La Liga',
            r'\bFrance 1\b': 'France - Ligue 1',
            r'\bSerbia 1\b': 'Serbia - Super Liga',
            r'\bAlgeria 1\b': 'Algeria - Ligue 1',
            r'\bAustralia 1 - Women\b': 'Australia - A-League Women',
            r'\bAustralia 1\b': 'Australia - A League',
        }

        # Применяем замены из словаря
        for pattern, replacement in replacements.items():
            league_name = re.sub(pattern, replacement, league_name, flags=re.IGNORECASE)

        # Обработка 'Rep.' и 'Rep. X' для футбола
        league_name = re.sub(r'\bRep\.', 'Republic', league_name, flags=re.IGNORECASE)
        league_name = re.sub(r'Republic\s?(\d)', r'Republic - \1', league_name, flags=re.IGNORECASE)

        # Удаляем повторяющиеся слова
        words = league_name.split()
        seen = set()
        deduped_words = []
        for word in words:
            lower_word = word.lower()
            if lower_word not in seen:
                seen.add(lower_word)
                deduped_words.append(word)
        league_name = ' '.join(deduped_words)

        # Проверка на наличие "-" между словами
        words = league_name.split()
        if len(words) == 2:
            # Если два слова, проверяем и добавляем тире
            if words[0].lower() not in ['republic'] and not words[1].isdigit():
                if '-' not in words[0] and '-' not in words[1]:
                    league_name = f"{words[0]} - {words[1]}"
        elif len(words) > 2:
            # Если больше двух слов, добавляем тире, если его нет
            if words[0].lower() not in ['republic'] and not words[1].isdigit():
                if '-' not in words[0] and '-' not in words[1]:
                    league_name = f"{words[0]} - {words[1]} {' '.join(words[2:])}"

        # Обработка исключений для капитализации
        uppercase_exceptions = ['UEFA', 'EFL', 'Liga']

        # Капитализируем слова
        words = league_name.split()
        for i in range(len(words)):
            if words[i].upper() in uppercase_exceptions:
                words[i] = words[i].upper()
            else:
                words[i] = words[i].capitalize()
        league_name = ' '.join(words)

        return league_name
    else:
        # Для других видов спорта можно добавить другую логику или вернуть исходное название
        return league_name


def process_country_name(country_name: str) -> str:
    """
    Обрабатывает название страны, применяя специальные правила замены.

    :param country_name: Исходное название страны
    :return: Обработанное название страны
    """
    country_name = country_name.strip().lower()

    # Применяем регулярные выражения для замены
    country_name = re.sub(r'\bCzech\s*Rep\.?\b', 'Czech Republic', country_name, flags=re.IGNORECASE)
    country_name = re.sub(r'\bInternational Youth\b', 'Europe', country_name, flags=re.IGNORECASE)
    country_name = re.sub(r'\bRussia\b', 'Russian Federation', country_name, flags=re.IGNORECASE)

    # Восстанавливаем регистр первой буквы каждого слова
    country_name = ' '.join(word.capitalize() for word in country_name.split())

    return country_name


def extract_city_from_league_name(league_name: str) -> Optional[str]:
    """
    Извлекает название города из названия лиги, игнорируя слова-исключения.

    :param league_name: Название лиги
    :return: Название города или 'World', если город не удалось определить
    """
    # Убираем лишние пробелы и запятые
    league_name = league_name.strip().replace(",", "").replace("-", "")

    # Список слов-исключений
    exceptions = {'Doubles', 'ATP', 'Challenger', 'ITF', 'Women', 'Men',
                  'Singles', 'Greece', 'Qualifiers', 'WTA'}

    # Разбиваем название лиги на слова
    words = league_name.split()

    # Убираем слова, которые являются исключениями или имеют формат буква+цифра
    filtered_words = [
        word for word in words
        if word not in exceptions and not re.match(r'^[A-Za-z]*\d+[A-Za-z]*$', word)
    ]

    # Если после фильтрации остались слова, возвращаем их как город
    if filtered_words:
        # Считаем городом все оставшиеся слова после исключений
        return ' '.join(filtered_words)

    # Если ничего не подошло, возвращаем None
    return None


@lru_cache(maxsize=1000)
def get_country_by_city(city: str) -> str:
    """
    Получает страну по названию города с помощью geopy.

    :param city: Название города
    :return: Страна, в которой находится город
    """
    location = geolocator.geocode(city, language='en')
    if location:
        # Получаем страну из адреса
        country = location.address.split(",")[-1].strip()

        # Обрабатываем специальные случаи
        if country == "United States":
            return "USA"
        return country
    return "World"
