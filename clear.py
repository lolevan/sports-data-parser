# clear.py
import os
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))



def remove_old_files():
    print("Запуск")
    THREE_HOURS_AGO = time.time() - 3 * 3600

    for dirpath, dirnames, filenames in os.walk(PROJECT_PATH):
        if "odds_data" in dirnames:
            odds_data_path = os.path.join(dirpath, "odds_data")

            for filename in os.listdir(odds_data_path):
                file_path = os.path.join(odds_data_path, filename)

                file_mtime = os.path.getmtime(file_path)

                if file_mtime < THREE_HOURS_AGO:
                    print(f"Deleting {file_path} - last modified: {datetime.fromtimestamp(file_mtime)}")
                    os.remove(file_path)


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(remove_old_files, "interval", minutes=1)
    scheduler.start()
