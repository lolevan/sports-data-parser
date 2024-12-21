# csv_creator.py
import asyncio
import csv
import json
import os
import logging
import struct
from datetime import datetime, timedelta
from typing import Dict, Any, List, Callable, Iterator, Tuple
from aiogram import Bot
from aiogram.types import FSInputFile
import snappy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVCreator:
    def __init__(self, bot_token: str, group_id: str,
                 csv_dir: str = 'csv_files',
                 bookmakers_file: str = 'bookmakers.json'):
        self.bot_token = bot_token
        self.group_id = group_id
        self.csv_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), csv_dir)
        self.bookmakers = self.load_bookmakers(bookmakers_file)
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)

    def load_bookmakers(self, bookmakers_file: str) -> Dict[
        str, Dict[str, Any]]:
        absolute_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', bookmakers_file))
        logger.info(f"Loading bookmakers from: {absolute_path}")
        with open(absolute_path, 'r') as f:
            bookmakers_data = json.load(f)

        project_root = os.path.dirname(os.path.dirname(__file__))
        for bookmaker, data in bookmakers_data.items():
            if 'data_path' in data:
                data['data_path'] = os.path.abspath(
                    os.path.join(project_root, data['data_path']))
                logger.info(
                    f"Bookmaker {bookmaker} data path: {data['data_path']}")

        return bookmakers_data

    def get_bookmaker_data_path(self, bookmaker_name: str) -> str:
        bookmaker = self.bookmakers.get(bookmaker_name)
        if not bookmaker:
            raise ValueError(
                f"Bookmaker {bookmaker_name} not found in configuration")
        return bookmaker.get('data_path', '')

    def read_jsonl(self, file_path: str) -> Iterator[Dict[str, Any]]:
        with open(file_path, 'rb') as file:
            while True:
                # Читаем первые 4 байта, представляющие длину следующего блока
                length_bytes = file.read(4)
                if not length_bytes:
                    break  # Конец файла
                if len(length_bytes) < 4:
                    raise ValueError(
                        "Некорректные данные: недостаточно байтов для длины блока.")

                # Распаковываем длину блока (big-endian)
                length = struct.unpack('>I', length_bytes)[0]

                # Читаем сам сжатый блок
                compressed_data = file.read(length)
                if len(compressed_data) != length:
                    raise ValueError(
                        "Некорректные данные: ожидаемая длина блока не совпадает.")

                try:
                    # Декомпрессируем данные
                    decompressed_data = snappy.decompress(compressed_data)
                    # Загружаем JSON из декомпрессированных данных
                    yield json.loads(decompressed_data.decode('utf-8'))
                except snappy.UncompressError as e:
                    print(f"Ошибка декомпрессии: {e}")
                except json.JSONDecodeError as e:
                    print(f"Ошибка декодирования JSON: {e}")

    async def create_and_send_csv(
            self, outcome: Dict[str, Any], placed_at: str, match_name: str,
            pinnacle_match_name: str, bookmaker_name: str,
            bookmaker_match_name: str, bookmaker_odds: float,
            pinnacle_odds: float, bet_size: float, country: str,
            league: str, is_live: bool, time_before: int = 300,
            time_after: int = 1200, roi: float = 0, status_bet: bool = True
    ):
        if is_live and time_after == 1200 and time_before == 300:
            time_before = 120
            time_after = 120
        try:
            filename, gaps_found = await self.save_data_to_csv(
                outcome, placed_at, pinnacle_match_name,
                bookmaker_name, bookmaker_match_name,
                time_before, time_after
            )

            if not filename:
                logger.error("CSV file not created")
                return

            await self.send_csv_file(
                match_name=match_name,
                bet_type=outcome['type'],
                bet_line=outcome['line'],
                pinnacle_odds=pinnacle_odds,
                bookmaker_odds=bookmaker_odds,
                time=placed_at,
                bet_size=bet_size,
                country=country,
                league=league,
                filename=filename,
                is_live=is_live,
                bookmaker_name=bookmaker_name,
                roi=roi,
                status_bet=status_bet,
                gaps_found=gaps_found  # Pass the flag here
            )
        except Exception as e:
            logger.error(f"Error in create_and_send_csv: {e}", exc_info=True)

    async def save_data_to_csv(
            self, placed: Dict[str, Any], placed_at: str,
            pinnacle_match_name: str, bookmaker_name: str,
            bookmaker_match_name: str, time_before: int,
            time_after: int
    ) -> Tuple[str, bool]:
        await asyncio.sleep(time_after)
        placed_key = (placed.get("type"), float(placed.get("line")))
        placed_at_dt = datetime.strptime(placed_at, "%Y-%m-%d %H:%M:%S")

        filename = self.get_csv_filename(
            pinnacle_match_name, placed_at_dt, placed['type'], placed['line']
        )

        pinnacle_data_path = self.get_bookmaker_data_path('pinnacle')
        bookmaker_data_path = self.get_bookmaker_data_path(bookmaker_name)

        pinnacle_file = os.path.join(pinnacle_data_path, "odds_data",
                                     f"{pinnacle_match_name}.bin")
        bookmaker_file = os.path.join(bookmaker_data_path, "odds_data",
                                      f"{bookmaker_match_name}.bin")

        logger.info(f"Checking Pinnacle file: {pinnacle_file}")
        logger.info(f"Checking {bookmaker_name} file: {bookmaker_file}")

        outcomes_data = []

        if os.path.exists(pinnacle_file):
            outcomes_data.extend(
                self.process_jsonl_file(
                    'pinnacle', pinnacle_file, placed_key,
                    placed_at_dt, time_before, time_after
                )
            )
        else:
            logger.warning(f"Pinnacle JSONL file not found: {pinnacle_file}")

        if os.path.exists(bookmaker_file):
            outcomes_data.extend(
                self.process_jsonl_file(
                    bookmaker_name, bookmaker_file,
                    placed_key, placed_at_dt, time_before, time_after
                )
            )
        else:
            logger.warning(
                f"{bookmaker_name} JSONL file not found: {bookmaker_file}")

        if not outcomes_data:
            logger.error("No data found for CSV creation")
            return "", False

        # Sort the data by time
        outcomes_data.sort(key=lambda x: x[1])

        # Check for gaps > 2 minutes
        gaps_found = False
        previous_time = None
        for data in outcomes_data:
            current_time = data[1]
            if previous_time and (
                    current_time - previous_time).total_seconds() > 120:
                gaps_found = True
                break
            previous_time = current_time

        self.write_csv(filename, outcomes_data, placed_at_dt)
        return filename, gaps_found

    def process_jsonl_file(
            self, source: str, file_path: str,
            placed_key: tuple, placed_at: datetime,
            time_before: int, time_after: int
    ) -> List:
        outcomes = []
        for data in self.read_jsonl(file_path):
            outcome_time = datetime.fromtimestamp(data['time'])
            if outcome_time > (placed_at + timedelta(seconds=time_after)):
                break
            if (
                    (
                            outcome_time < placed_at and placed_at - outcome_time <= timedelta(
                        seconds=time_before)) or
                    (
                            outcome_time >= placed_at and outcome_time - placed_at <= timedelta(
                        seconds=time_after))
            ):
                for outcome in data['outcomes']:
                    outcome_key = (
                        outcome.get("type"), float(outcome.get("line"))
                    )
                    if outcome_key == placed_key:
                        outcomes.append([source, outcome_time, outcome])
                        break
        return outcomes

    def get_csv_filename(
            self, match_name: str, placed_at: datetime,
            bet_type: str, bet_line: float
    ) -> str:
        safe_match_name = "".join(
            c for c in match_name if c.isalnum() or c in (' ', '.', '_')
        ).rstrip()
        return os.path.join(
            self.csv_dir,
            f"{safe_match_name}_{placed_at.strftime('%Y%m%d_%H%M%S')}_{bet_type}_{bet_line}.csv"
        )

    def write_csv(
            self, filename: str, outcomes_data: List,
            placed_at: datetime
    ):
        with open(filename, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows([
                ["Time", "Bookmaker", "Bookmaker", "Bookmaker", "Bookmaker",
                 "",
                 "Pinnacle", "Pinnacle", "Pinnacle", "Pinnacle"],
                ["Time", "TypeName", "Type", "Line", "Odds", "",
                 "TypeName", "Type", "Line", "Odds"]
            ])

            outcomes_data.sort(key=lambda x: x[1])
            bet_time_row_added = False

            for data in outcomes_data:
                current_time = data[1]
                if not bet_time_row_added and placed_at <= current_time:
                    csv_writer.writerow(["=" * 15] + ["=" * 8] * 9)
                    csv_writer.writerow(
                        [placed_at.strftime("%Y-%m-%d %H:%M:%S")] + [""] * 9
                    )
                    bet_time_row_added = True

                if data[0] == 'pinnacle':
                    csv_writer.writerow([
                        current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "", "", "", "", "",
                        data[2]['type_name'], data[2]['type'],
                        data[2]['line'], data[2]['odds']
                    ])
                else:
                    csv_writer.writerow([
                        current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        data[2]['type_name'], data[2]['type'],
                        data[2]['line'], data[2]['odds'],
                        "", "", "", "", ""
                    ])

            if not bet_time_row_added:
                csv_writer.writerow(["=" * 15] + ["=" * 8] * 9)
                csv_writer.writerow(
                    [placed_at.strftime("%Y-%m-%d %H:%M:%S")] + [""] * 9
                )

        logger.info(f"CSV file created: {filename}")

    async def send_csv_file(
            self, match_name: str, bet_type: str, bet_line: float,
            pinnacle_odds: float, bookmaker_odds: float, time: str,
            bet_size: float, country: str, league: str,
            filename: str, is_live: bool, bookmaker_name: str,
            roi: float = 0, status_bet: bool = True,
            gaps_found: bool = False  # New parameter
    ):
        bot = Bot(token=self.bot_token)
        try:
            text = (
                f"Bookmaker: {bookmaker_name}\n"
                f"{'LIVE' if is_live else 'PREMATCH'}\n"
                f"Country: {country}\n"
                f"League: {league}\n"
                f"Match: {match_name}\n"
                f"Bet: {bet_type} {bet_line}\n"
                f"Pinnacle odds: {pinnacle_odds}\n"
                f"{bookmaker_name} odds: {bookmaker_odds}\n"
                f"Roi: {roi:.2f}%\n"
                f"Bet size: {bet_size}\n"
                f"Time: {time}"
            )
            if not status_bet:
                text += "\n\nСтавка в пиннакле не прошла"

            if gaps_found:
                text += "\n\n⚠️ Обнаружены пробелы в данных более 2 минут."

            await bot.send_document(
                chat_id=self.group_id,
                document=FSInputFile(filename),
                caption=text
            )
            logger.info(f"CSV file sent: {filename}")
        except Exception as e:
            logger.error(f"Error sending document: {e}", exc_info=True)
        finally:
            await bot.session.close()
