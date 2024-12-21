import json
import os
import struct
from snappy import snappy


def save_odds_to_jsonl(match_id, odds_data):
    if not os.path.exists("odds_data"):
        os.makedirs("odds_data")

    safe_match_id = match_id.replace("/", "")
    # file_name = f"odds_data/{safe_match_id}.bin"
    # универсальный формат для всех ОС
    file_name = os.path.join("odds_data", f"{safe_match_id}.bin")

    with open(file_name, "ab") as file:
        # Сериализуем данные в JSON и кодируем в UTF-8
        json_data = json.dumps(odds_data).encode('utf-8')
        # Сжимаем данные с помощью snappy
        compressed_odds_data = snappy.compress(json_data)
        # Записываем длину сжатых данных (4 байта, big-endian)
        file.write(struct.pack('>I', len(compressed_odds_data)))
        # Записываем сами сжатые данные
        file.write(compressed_odds_data)
