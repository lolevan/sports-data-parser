from pathlib import Path
import traceback
import sys
import json

BASE_DIR = Path(__file__).parent

login_file = BASE_DIR / "login.json"
try:
    with open(login_file, "r") as f:
        loginData = json.load(f)
except Exception as e:
    print(e)
    traceback.print_exc(file=sys.stdout)
    exit()

PINNCALE_USERNAME = loginData["username"]
PINNCALE_PASSWORD = loginData["password"]
PINNCALE_PROXY = loginData["proxy"]

wait_match = 300
wait_odds = 5




# Sports to run (format: [(sport_name, mode), ...])
# Mode can be "Live", "PreMatch", or "Both"
SPORTS_TO_RUN = [
    # ("Football", "PreMatch"),
    # ("Tennis", "PreMatch"),
    # ("Football", "Live"),
    # ("Tennis", "Live"),
    ("Football", "Both"),
    ("Tennis", "Both"),
    # ("Ice Hockey", "Both")

]

# Update intervals
PRE_MATCH_UPDATE_INTERVAL = 18
LIVE_UPDATE_INTERVAL = 1.8

# Rate limiting
RATE_LIMIT = 4
RATE_LIMIT_PERIOD = 1.0

# Socket server configuration
SOCKET_SERVER_HOST = 'localhost'
SOCKET_SERVER_PORT = 6000

# Logging configuration
LOGGING_LEVEL = "INFO"
LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Other parameters
EVENTS_BATCH_SIZE = 100


PRE_MATCH_HOURS_AHEAD = 24