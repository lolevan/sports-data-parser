# Создаем тестовый JSONL файл для Pinnacle
import asyncio
import json
import os
from datetime import datetime, timedelta

from csv_generator.csv_creator import CSVCreator
#
# pinnacle_data = [
#     {
#         "time": int((datetime.now() - timedelta(minutes=5)).timestamp()),
#         "outcomes": [
#             {"type": "1", "type_name": "Home", "line": 0, "odds": 2.0},
#             {"type": "X", "type_name": "Draw", "line": 0, "odds": 3.0},
#             {"type": "2", "type_name": "Away", "line": 0, "odds": 4.0}
#         ]
#     },
#     {
#         "time": int(datetime.now().timestamp()),
#         "outcomes": [
#             {"type": "1", "type_name": "Home", "line": 0, "odds": 1.95},
#             {"type": "X", "type_name": "Draw", "line": 0, "odds": 3.1},
#             {"type": "2", "type_name": "Away", "line": 0, "odds": 4.2}
#         ]
#     }
# ]
#
# # Создаем директорию для данных Pinnacle, если она не существует
# os.makedirs("parsers/pinnacle2/odds_data", exist_ok=True)
#
# # Записываем данные в JSONL файл
# with open("parsers/pinnacle2/odds_data/Team A vs Team B.jsonl", 'w') as f:
#     for item in pinnacle_data:
#         f.write(json.dumps(item) + '\n')

import asyncio
from csv_creator import CSVCreator

async def test_csv_creator():
    csv_creator = CSVCreator(
        bot_token="7182904182:AAFeeNer_HX-syWNcO4eotuP3CK8FiVuu3M",
        group_id="-4548075627"
    )

    await csv_creator.create_and_send_csv(
        outcome={"type": "1", "line": 0},
        placed_at="2024-09-07 9:20:00",
        match_name="Team A vs Team B",
        pinnacle_match_name="Team A vs Team B",
        bookmaker_name="sansabet",
        bookmaker_match_name="TeamA - TeamB",
        bookmaker_odds=2.0,
        pinnacle_odds=1.95,
        bet_size=100,
        country="TestCountry",
        league="TestLeague",
        is_live=False
    )

    print("Test completed. Check the generated CSV file and console output for results.")


# Запускаем тестовую функцию
asyncio.run(test_csv_creator())