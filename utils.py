# utils.py
import json
import os
import logging
from datetime import datetime
import pandas as pd

def load_bookmakers(config_path='bookmakers.json'):
    """
    Loads bookmaker configurations from a JSON file and returns only enabled bookmakers.
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        bookmakers = json.load(f)
    # Filter only enabled bookmakers
    enabled_bookmakers = {k: v for k, v in bookmakers.items()}
    return enabled_bookmakers

def load_matched_events(active_bookmakers):
    """
    Loads matched events from JSON files for all active bookmakers except Pinnacle.
    """
    matched_events = []
    for bk in active_bookmakers:
        if bk.lower() != 'pinnacle':
            path = os.path.join('matching', 'bookmaker_mappings', bk, 'matched_events.json')
            if os.path.exists(path):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        events = json.load(f)
                        matched_events.extend(events)
                except Exception as e:
                    logging.error(f"Error reading file {path}: {e}")
    return matched_events

def load_matched_event(match_name, bookmaker):
    """
    Loads a single matched event for a specific bookmaker based on match name.
    """
    if bookmaker.lower() != 'pinnacle':
        path = os.path.join('matching', 'bookmaker_mappings', bookmaker, 'matched_events.json')
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    events = json.load(f)
                    for event in events:
                        if event.get('pinnacle_match_name', '').lower() == match_name.lower():
                            return event
            except Exception as e:
                logging.error(f"Error reading file {path}: {e}")
    return None


def load_bookmaker_data(bookmaker, bookmakers):
    """
    Loads all bookmaker data from JSONL files and returns a dictionary.
    """
    data = {}
    data_path = bookmakers[bookmaker]['data_path']
    odds_data_path = os.path.join(data_path, 'odds_data')

    if not os.path.exists(odds_data_path):
        logging.warning(f"Odds data folder not found for {bookmaker}: {odds_data_path}")
        return data

    for filename in os.listdir(odds_data_path):
        if filename.endswith('.jsonl'):
            match_name = filename[:-6]  # Remove ".jsonl"
            file_path = os.path.join(odds_data_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        event = json.loads(line)
                        data[match_name] = event  # Keyed by match name
            except Exception as e:
                logging.error(f"Error reading file {file_path}: {e}")

    return data
# utils.py

import json
import os
import logging
from datetime import datetime

def load_bookmaker_data_for_match(bookmaker, bookmakers, match_name):
    """
    Loads all data for a specific match from a bookmaker's JSONL file.
    """
    data = []
    data_path = bookmakers[bookmaker]['data_path']
    odds_data_path = os.path.join(data_path, 'odds_data')

    if not os.path.exists(odds_data_path):
        logging.warning(f"Odds data folder not found for {bookmaker}: {odds_data_path}")
        return data

    filename = f"{match_name}.jsonl"
    file_path = os.path.join(odds_data_path, filename)
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    event = json.loads(line)
                    data.append(event)
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")

    return data

# ... (остальные функции остаются без изменений)

def process_outcomes(outcomes):
    """
    Processes the outcomes to format them for comparison.
    """
    result = {}
    for outcome in outcomes:
        type_ = outcome.get('type', '')
        line = float(outcome.get('line', 0))
        key = format_line(line, type_)
        odds = outcome.get('odds', 0)
        if key:
            result[key] = odds
    return result

def format_line(line, type_):
    """
    Formats the line and type into a unique key.
    """
    return f"{type_} {line}"

def calculate_roi(pinnacle_odds, other_odds):
    """
    Calculates ROI based on Pinnacle and other bookmaker odds.
    """
    MARGIN = 1.08
    extra_percent = get_extra_percent(pinnacle_odds)
    roi = (other_odds / (pinnacle_odds * MARGIN * extra_percent) - 1) * 100
    return roi

def get_extra_percent(pinnacle_odds):
    """
    Determines the extra percentage based on Pinnacle odds.
    """
    if 2.29 <= pinnacle_odds < 2.75:
        return 1.03
    elif 2.75 <= pinnacle_odds < 3.2:
        return 1.04
    elif 3.2 <= pinnacle_odds <= 3.7:
        return 1.05
    else:
        return 1.0

def sanitize_column_name(type_name, line, bookmaker, metric):
    """
    Creates a safe column name by combining type_name, line, bookmaker, and metric.
    """
    type_clean = type_name.replace(" ", "_")
    line_clean = str(line).replace('.', '_').replace('-', 'minus_')
    bookmaker_clean = bookmaker.replace(" ", "_")
    return f"{type_clean}_{line_clean}_{bookmaker_clean}_{metric}"
