# get_all_bets.py
import os
import json


def collect_all_bets(automation_dir='automation'):
    """
    Собирает все ставки из файлов bets_log.jsonl в папке automation и её подпапках.

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
                    for line_number, line in enumerate(f):
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


# # Пример использования:
# if __name__ == "__main__":
#     bets = collect_all_bets('automation')
#     print(f"Найдено {len(bets)} ставок.")
#     # Вы можете добавить дополнительную обработку списка bets здесь
