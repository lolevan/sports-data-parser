# info_by_bets.py
import os
import json
import pandas as pd
import re


def collect_all_bets(automation_dir='automation'):
    """
    Собирает все ставки из файлов bets_log.jsonl в директории automation и её подпапках.

    :param automation_dir: Путь к директории automation.
    :return: Список всех ставок.
    """
    all_bets = []

    # Проходим по всем директориям и подпапкам
    for root, dirs, files in os.walk(automation_dir):
        if 'bets_log.jsonl' in files:
            file_path = os.path.join(root, 'bets_log.jsonl')
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_number, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue  # Пропускаем пустые строки
                        try:
                            bet = json.loads(line)
                            all_bets.append(bet)
                        except json.JSONDecodeError as e:
                            print(
                                f"Ошибка декодирования JSON в файле {file_path}, строка {line_number}: {e}")
            except (IOError, OSError) as e:
                print(f"Не удалось открыть файл {file_path}: {e}")

    return all_bets


def load_latest_prematch_odds(match_name, bet_type, bet_line,
                              odds_data_dir='parsers/pinnacle2/odds_data'):
    """
    Загружает последнюю строку премачевых коэффициентов, содержащую нужный тип ставки и линию.

    :param match_name: Название матча.
    :param bet_type: Тип ставки.
    :param bet_line: Линия ставки.
    :param odds_data_dir: Путь к директории с премачевыми данными.
    :return: Словарь с премачевыми коэффициентами или None, если не найдено.
    """
    file_path = os.path.join(odds_data_dir, f"{match_name}.jsonl")
    if not os.path.exists(file_path):
        print(f"Файл с коэффициентами Pinnacle не найден: {file_path}")
        return None

    try:
        # Используем pandas для чтения файла
        df = pd.read_json(file_path, lines=True, encoding='utf-8')

        # Фильтруем только PreMatch
        df_prematch = df[df['type'] == 'PreMatch']

        # Перебираем строки с конца для поиска последнего соответствия
        for _, row in df_prematch.iloc[::-1].iterrows():
            outcomes = row.get('outcomes', [])
            for outcome in outcomes:
                if outcome.get('type') == bet_type and float(
                        outcome.get('line', 0)) == bet_line:
                    return row.to_dict()  # Преобразуем строку в словарь

        print(
            f"Не найдено подходящих премачевых коэффициентов для матча {match_name}, ставки {bet_type} {bet_line}")
        return None

    except ValueError as e:
        print(f"Ошибка чтения JSON файла {file_path}: {e}")
        return None


def get_opposite_bet_type(bet_type):
    """
    Возвращает противоположный тип ставки, сохраняя период, если он есть.

    Например:
        '1H1' -> ['1HX', '1H2']
        '1H2' -> ['1HX', '1H1']
        'AH1' -> 'AH2'
        'O' -> 'U'
        'U' -> 'O'
        '2' -> ['1', 'X']

    :param bet_type: Исходный тип ставки.
    :return: Противоположный тип ставки или список противоположных типов.
    """
    # Определяем паттерн для разделения период и базового типа
    # Период: начинается с цифры и 'H', например '1H'
    # Остальная часть - базовый тип
    pattern = r'^(\d+H)?(.*)$'
    match = re.match(pattern, bet_type)

    if not match:
        print(f"Неподдерживаемый формат типа ставки: {bet_type}")
        return None

    period, base_type = match.groups()

    opposites = {
        'O': 'U',
        'U': 'O',
        'AH1': 'AH2',
        'AH2': 'AH1',
        '1': ['X', '2'],
        'X': ['1', '2'],
        '2': ['1', 'X'],
        # Добавьте другие базовые типы ставок при необходимости
    }

    opposite_base = opposites.get(base_type)

    if not opposite_base:
        print(
            f"Противоположный тип ставки не найден для базового типа: {base_type}")
        return None

    # Функция для объединения периода и базового типа
    def combine(period, base):
        if period:
            return f"{period}{base}"
        return base

    if isinstance(opposite_base, list):
        # Если противоположный тип возвращает список, применяем период к каждому
        return [combine(period, opp) for opp in opposite_base]
    else:
        # Иначе, просто комбинируем период с противоположным базовым типом
        return combine(period, opposite_base)


def calculate_margin(odds_list):
    """
    Рассчитывает маржу букмекера.

    :param odds_list: Список коэффициентов.
    :return: Значение маржи.
    """
    try:
        total_inverse_odds = sum(1 / odds for odds in odds_list)
        margin = total_inverse_odds - 1  # Десятичная маржа
        return margin
    except ZeroDivisionError:
        return None


def process_bets(bets, odds_data_dir='parsers/pinnacle2/odds_data'):
    """
    Обрабатывает список ставок, вычисляет маржу и true_coef, и собирает результаты.

    :param bets: Список ставок.
    :param odds_data_dir: Путь к директории с премачевыми данными.
    :return: Список результатов.
    """
    results = []
    for bet in bets:
        # Формируем название матча
        match_name = f"{bet['home_pin']} vs {bet['away_pin']}"
        bet_type = bet['type']
        bet_line = float(bet['line'])

        # Получаем коэффициент Pinnacle, использованный при ставке
        bet_odds = float(bet.get('bookmaker_odds', 0))
        if bet_odds == 0:
            print(f"Нет Pinnacle коэффициента в ставке {bet['match_id']}")
            continue

        # Загружаем последнюю подходящую строку премача
        latest_prematch = load_latest_prematch_odds(match_name, bet_type,
                                                    bet_line, odds_data_dir)
        if latest_prematch is None:
            continue  # Пропускаем ставку, если нет подходящих коэффициентов

        outcomes = latest_prematch.get('outcomes', [])

        # Найти последний Pinnacle коэффициент для этой ставки
        latest_pinnacle_odds = None
        for outcome in outcomes:
            if outcome.get('type') == bet_type and float(
                    outcome.get('line', 0)) == bet_line:
                latest_pinnacle_odds = float(outcome.get('odds'))
                break

        if latest_pinnacle_odds is None:
            print(
                f"Последний Pinnacle коэффициент для ставки {bet_type} {bet_line} не найден в матче {match_name}")
            continue

        # Найти противоположные коэффициенты
        opposite_bet_types = get_opposite_bet_type(bet_type)
        if not opposite_bet_types:
            print(
                f"Противоположный тип ставки не найден для {bet_type} в ставке {bet['match_id']}")
            continue

        # Убедимся, что opposite_bet_types - это список
        if not isinstance(opposite_bet_types, list):
            opposite_bet_types = [opposite_bet_types]

        opposite_odds_list = []
        for opp_type in opposite_bet_types:
            found = False
            for outcome in outcomes:
                if outcome.get('type') == opp_type and float(
                        outcome.get('line', 0)) == bet_line:
                    opposite_odds_list.append(float(outcome.get('odds')))
                    found = True
                    break
            if not found:
                print(
                    f"Противоположный коэффициент для типа {opp_type} и линии {bet_line} не найден в матче {match_name}")

        if not opposite_odds_list:
            print(
                f"Противоположные коэффициенты для ставки {bet_type} {bet_line} не найдены в матче {match_name}")
            continue

        # Формируем список коэффициентов для расчета маржи
        odds_list = [latest_pinnacle_odds] + opposite_odds_list

        # Рассчитываем маржу
        margin = calculate_margin(odds_list)
        if margin is None:
            print(
                f"Ошибка при расчете маржи для ставки {bet['match_id']} ({bet_type} {bet_line})")
            continue

        # Рассчитываем true_coef: latest_pinnacle_odds * (margin + 1)
        true_odds = latest_pinnacle_odds * (margin + 1)

        # Сохраняем результат
        result = {
            'match_id': bet['match_id'],
            'match_name': match_name,
            'bet_type': bet_type,
            'bet_line': bet_line,
            'bet_odds': bet_odds,  # Коэффициент при ставке
            'latest_pinnacle_odds': latest_pinnacle_odds,
            # Последний Pinnacle коэффициент
            'margin': margin,
            'true_odds': true_odds
        }
        results.append(result)

    return results


def save_results(results, output_file='results.jsonl'):
    """
    Сохраняет результаты в JSONL файл.

    :param results: Список результатов.
    :param output_file: Имя выходного файла.
    """
    with open(output_file, 'a', encoding='utf-8') as f:
        for result in results:
            f.write(json.dumps(result, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    bets = collect_all_bets()
    results = process_bets(bets)
    save_results(results)
    print(
        f"Обработка завершена. Результаты сохранены в файл {os.path.abspath('results.jsonl')}")
