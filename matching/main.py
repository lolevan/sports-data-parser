import asyncio
import json
import logging
import time
from aiogram import Bot
from websocket_client import WebSocketClient, PinnacleDataManager
from algo_matching import MatchPairer
from mappings import Mappings

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def process_pinnacle_data(ws_client, pinnacle_manager):
    while True:
        try:
            pinnacle_data = ws_client.get_data('pinnacle')
            if pinnacle_data:
                logging.info(
                    f"Получены данные Pinnacle: {len(pinnacle_data)} событий")
                pinnacle_manager.update_matches(pinnacle_data)
            else:
                logging.debug("Данные Pinnacle отсутствуют")
            await asyncio.sleep(
                0.1)  # Небольшая задержка, чтобы не перегружать процессор
        except Exception as e:
            logging.error(f"Ошибка при обработке данных Pinnacle: {e}",
                          exc_info=True)
            await asyncio.sleep(1)


async def process_other_bookmakers(ws_client, bookmakers_config,
                                   pinnacle_manager, bot, chat_id):
    last_sent_time = time.time() - 19 * 60  # Инициализация времени последней отправки
    report_interval = 20 * 60  # 20 минут в секундах

    while True:
        try:
            other_bookmakers_data = {}
            for bookmaker in bookmakers_config:
                if bookmaker != 'pinnacle' and bookmakers_config[bookmaker][
                    'enabled']:
                    logging.debug(f"Получение данных {bookmaker}...")
                    data = ws_client.get_data(bookmaker)
                    if data:
                        other_bookmakers_data[bookmaker] = data
                        logging.info(
                            f"Получены данные {bookmaker}: {sum(len(sport_data) for sport_data in data.values())} событий")
                    else:
                        logging.warning(f"Данные {bookmaker} отсутствуют")

            pinnacle_matches = pinnacle_manager.get_matches()
            logging.info(
                f"Всего матчей Pinnacle: {sum(len(sport_data) for sport_data in pinnacle_matches.values())}")

            matching_data = []

            for bookmaker, bookmaker_data in other_bookmakers_data.items():
                match_pairer = MatchPairer(bookmaker=bookmaker)
                for sport in pinnacle_matches.keys():
                    if sport in bookmaker_data:
                        new_matches, unmatched_pinnacle, unmatched_other = match_pairer.match_events(
                            pinnacle_matches[sport], bookmaker_data[sport])

                        total_pinnacle = len(pinnacle_matches[sport])
                        total_bookmaker = len(bookmaker_data[sport])
                        matched_percentage = 0
                        if total_pinnacle > 0 and total_bookmaker > 0:
                            if total_bookmaker > total_pinnacle:
                                matched_percentage = 100 - (
                                            len(unmatched_pinnacle) / total_pinnacle) * 100
                            else:
                                matched_percentage = 100 - (
                                            len(unmatched_other) / total_bookmaker) * 100
                            matched_percentage = round(matched_percentage, 2)
                        else:
                            matched_percentage = 0.0

                        matching_data.append({
                            'bookmaker': bookmaker,
                            'sport': sport,
                            'matched_percentage': matched_percentage,
                            'total_pinnacle': total_pinnacle,
                            'total_bookmaker': total_bookmaker
                        })
                        logging.info(
                            f"Новые совпадения с {bookmaker} для {sport}: {len(new_matches)}")
                        logging.info(
                            f"Несопоставленные матчи Pinnacle для {sport}: {len(unmatched_pinnacle)}")
                        logging.info(
                            f"Несопоставленные матчи {bookmaker} для {sport}: {len(unmatched_other)}")

            # Проверяем, прошло ли 20 минут
            current_time = time.time()
            if current_time - last_sent_time >= report_interval:
                # Подготавливаем и отправляем сообщение
                if matching_data:
                    message_text = "Отчет о проценте сопоставления:\n\n"
                    for data in matching_data:
                        message_text += f"Букмекер: {data['bookmaker']}, Спорт: {data['sport']}\n"
                        message_text += f"Процент сопоставления: {data['matched_percentage']}%\n"
                        message_text += f"Всего матчей Pinnacle: {data['total_pinnacle']}\n"
                        message_text += f"Всего матчей {data['bookmaker']}: {data['total_bookmaker']}\n\n"

                    try:
                        await bot.send_message(chat_id=chat_id,
                                               text=message_text)
                        logging.info("Сообщение отправлено в Telegram.")
                    except Exception as e:
                        logging.error(
                            f"Ошибка при отправке сообщения в Telegram: {e}",
                            exc_info=True)
                else:
                    logging.info("Нет данных для отправки в Telegram.")

                # Обновляем время последней отправки
                last_sent_time = current_time

            logging.info(
                "Ожидание 60 секунд перед следующим циклом сопоставления...")
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Ошибка в цикле сопоставления: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    try:
        with open('../bookmakers.json', 'r') as f:
            bookmakers_config = json.load(f)
    except FileNotFoundError:
        logging.error(
            "Файл bookmakers.json не найден. Убедитесь, что он находится в правильной директории.")
        return
    except json.JSONDecodeError:
        logging.error(
            "Ошибка при чтении файла bookmakers.json. Проверьте его формат.")
        return
    logging.info("Конфигурация букмекеров загружена успешно.")

    # Настройка Telegram Bot
    try:
        with open('telegram_config.json', 'r') as f:
            telegram_config = json.load(f)
        TELEGRAM_BOT_TOKEN = telegram_config['token']
        TELEGRAM_CHAT_ID = telegram_config['chat_id']
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
    except Exception as e:
        logging.error(
            "Ошибка при инициализации бота Telegram. Проверьте файл конфигурации.",
            exc_info=True)
        return

    ws_client = WebSocketClient(bookmakers_config)
    pinnacle_manager = PinnacleDataManager()

    while True:
        try:
            logging.info("Запуск WebSocket клиента...")
            ws_task = asyncio.create_task(ws_client.start())
            # Запуск задач для обработки данных Pinnacle и других букмекеров
            pinnacle_task = asyncio.create_task(
                process_pinnacle_data(ws_client, pinnacle_manager))
            other_bookmakers_task = asyncio.create_task(
                process_other_bookmakers(ws_client, bookmakers_config,
                                         pinnacle_manager, bot,
                                         TELEGRAM_CHAT_ID))

            # Ожидание завершения всех задач
            await asyncio.gather(ws_task, pinnacle_task,
                                 other_bookmakers_task)
        except asyncio.CancelledError:
            logging.info("Задача отменена")
            break
        except Exception as e:
            logging.error(
                f"Критическая ошибка: {e}. Перезапуск через 10 секунд...",
                exc_info=True)
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
