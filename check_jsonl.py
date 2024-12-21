import os
import json
import concurrent.futures
import orjson
from tqdm import tqdm


def check_one_file(file_path):
    result = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            prev_obj = None
            for line in file:
                obj = orjson.loads(line)
                if prev_obj:
                    time_diff = obj['time'] - prev_obj['time']
                    if time_diff >= 1800:  # 1800 секунд = 30 минут
                        result.append([prev_obj, obj])
                prev_obj = obj
    except Exception as e:
        print(f"Ошибка при обработке файла {file_path}: {e}")
    return os.path.basename(file_path), result


def check_odds_data_files():
    odds_data_dir = 'odds_data'
    if not os.path.exists(odds_data_dir):
        print(f"Директория '{odds_data_dir}' не существует.")
        return

    file_names = os.listdir(odds_data_dir)
    file_paths = [os.path.join(odds_data_dir, fname) for fname in file_names]

    results = {}
    total_files = len(file_paths)

    # Используем ThreadPoolExecutor для I/O-ограниченных задач
    max_workers = min(32,
                      os.cpu_count() * 5)  # Ограничиваем количество потоков
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers) as executor:
        # Используем tqdm для отображения прогресса
        futures = {executor.submit(check_one_file, path): path for path in
                   file_paths}

        with open("result.json", 'w', encoding='utf-8') as out_file:
            out_file.write("{\n")  # Начало JSON объекта
            first_entry = True
            for future in tqdm(concurrent.futures.as_completed(futures),
                               total=total_files, desc="Обработка файлов"):
                file_name, file_result = future.result()
                # Формируем строку JSON для текущего файла
                json_entry = orjson.dumps({file_name: file_result},
                                          option=orjson.OPT_INDENT_2).decode(
                    'utf-8')
                # Убираем фигурные скобки, чтобы вставить в общий JSON
                json_entry = json_entry.strip().lstrip("{").rstrip("}")
                if not first_entry:
                    out_file.write(",\n")
                else:
                    first_entry = False
                out_file.write(json_entry)
            out_file.write("\n}\n")  # Конец JSON объекта

    print("Результаты успешно записаны в 'result.json'.")


if __name__ == '__main__':
    check_odds_data_files()
