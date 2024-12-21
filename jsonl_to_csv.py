import datetime
import os
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def load_bookmakers(config_path):
    """
    Загружает конфигурацию букмекеров из JSON файла и возвращает только включенных букмекеров.
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        bookmakers = json.load(f)
    # Фильтруем только включенных букмекеров
    enabled_bookmakers = {k: v for k, v in bookmakers.items() if v.get('enabled', False)}
    return enabled_bookmakers

def sanitize_column_name(type_name, line):
    """
    Создает безопасное имя столбца, объединяя type_name и line.
    """
    type_clean = type_name
    line_clean = str(line)
    return f"{type_clean}_{line_clean}"

def process_jsonl_file(file_path, bookmaker_name):
    """
    Обрабатывает один JSONL файл и возвращает список событий с извлеченными коэффициентами.
    """
    events = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                event = json.loads(line)
                event_id = event.get('event_id')
                match_name = event.get('match_name')
                start_time = event.get('start_time')
                home_team = event.get('home_team')
                away_team = event.get('away_team')
                league = event.get('league')
                country = event.get('country')
                sport = event.get('sport')
                outcomes = event.get('outcomes', [])
                time = event.get('time')
                time= datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

                # Инициализируем словарь для текущего события
                event_dict = {
                    'time': time,
                    'event_id': event_id,
                    'match_name': match_name,
                    'start_time': start_time,
                    'home_team': home_team,
                    'away_team': away_team,
                    'league': league,
                    'country': country,
                    'sport': sport,
                    'bookmaker': bookmaker_name
                }

                # Извлекаем все необходимые коэффициенты
                for outcome in outcomes:
                    type_name = outcome.get('type_name')
                    type_short = outcome.get('type')
                    line = float(outcome.get('line'))
                    odds = outcome.get('odds')

                    if type_name and type_short and line is not None and odds is not None:
                        column_name = sanitize_column_name(type_short, line)
                        event_dict[column_name] = odds

                events.append(event_dict)
            except json.JSONDecodeError as e:
                print(f"Ошибка декодирования JSON в файле {file_path}: {e}")
            except Exception as e:
                print(f"Неизвестная ошибка при обработке файла {file_path}: {e}")
    return events

def save_to_csv(events, output_path):
    """
    Сохраняет список событий в CSV файл.
    """
    if not events:
        print(f"Нет данных для сохранения в {output_path}")
        return
    df = pd.DataFrame(events)
    # Заполняем отсутствующие значения пустыми строками
    df.fillna('', inplace=True)
    # Сортируем столбцы: основные поля сначала, затем коэффициенты
    basic_columns = ['time', 'event_id', 'match_name', 'start_time', 'home_team', 'away_team', 'league', 'country', 'sport', 'bookmaker']
    coefficient_columns = [col for col in df.columns if col not in basic_columns]
    df = df[basic_columns + sorted(coefficient_columns)]
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Сохранено {len(df)} строк в {output_path}")

def main():
    config_path = 'bookmakers.json'  # Путь к вашему конфигурационному файлу
    bookmakers = load_bookmakers(config_path)

    for bookmaker, details in bookmakers.items():
        data_path = details.get('data_path')
        if not data_path:
            print(f"Пропуск {bookmaker}: отсутствует data_path")
            continue
        odds_data_path = os.path.join(data_path, 'odds_data')
        odds_data_csv_path = os.path.join(data_path, 'odds_data_csv')

        # Создаем папку для CSV, если ее нет
        Path(odds_data_csv_path).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(odds_data_path):
            print(f"Путь {odds_data_path} не существует для {bookmaker}")
            continue

        # Получаем список всех JSONL файлов
        jsonl_files = [os.path.join(root, file)
                      for root, dirs, files in os.walk(odds_data_path)
                      for file in files if file.endswith('.jsonl')]

        if not jsonl_files:
            print(f"Нет JSONL файлов в {odds_data_path} для {bookmaker}")
            continue

        print(f"Обработка {len(jsonl_files)} файлов для {bookmaker}")

        for jsonl_file_path in tqdm(jsonl_files, desc=f"Обработка {bookmaker}"):
            events = process_jsonl_file(jsonl_file_path, bookmaker)

            # Определяем путь для сохранения CSV
            relative_path = os.path.relpath(jsonl_file_path, odds_data_path)
            csv_file_name = os.path.splitext(relative_path)[0].replace(os.sep, '_') + '.csv'
            csv_file_path = os.path.join(odds_data_csv_path, csv_file_name)

            # Сохраняем данные в CSV
            save_to_csv(events, csv_file_path)

if __name__ == "__main__":
    main()
