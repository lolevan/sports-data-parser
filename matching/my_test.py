from algo_matching import MatchPairer
from datetime import datetime, timedelta
import json


def create_test_event(event_id, home_team, away_team, league, country, sport,
                      start_time, outcomes):
    return {
        "id": event_id,
        "home_team": home_team,
        "away_team": away_team,
        "league_name": league,
        "league": league,
        "country": country,
        "sport": sport,
        "start_time": start_time.timestamp(),
        "outcomes": outcomes
    }


def create_test_data():
    start_time = (datetime.now() + timedelta(hours=2))

    pinnacle_events = {
        1: create_test_event(1, "Man Utd", "Liverpool",
                             "Premier League", "England", "Football",
                             start_time,
                             [{"type": "1", "odds": 2.1},
                              {"type": "X", "odds": 3.2},
                              {"type": "2", "odds": 3.5}]),
        2: create_test_event(2, "Real Madrid", "Barcelona", "La Liga",
                             "Spain", "Football", start_time,
                             [{"type": "1", "odds": 1.9},
                              {"type": "X", "odds": 3.4},
                              {"type": "2", "odds": 3.8}]),
        3: create_test_event(3, "Djokovic", "Nadal", "Wimbledon", "England",
                             "Tennis", start_time,
                             [{"type": "1", "odds": 1.8},
                              {"type": "2", "odds": 2.1}]),
        4: create_test_event(4, "Hashtag United", "Brackley Town",
                             "England - FA Trophy", "England", "Football", start_time,
                             [{"type": "1", "odds": 1.7},
                              {"type": "X", "odds": 3.6},
                              {"type": "2", "odds": 4.5}])
    }

    other_events = {
        101: create_test_event(101, "Man Utd", "Liverpool FC", "Premier League",
                               "England", "Football", start_time,
                               [{"type": "1", "odds": 2.15},
                                {"type": "X", "odds": 3.25},
                                {"type": "2", "odds": 3.45}]),
        102: create_test_event(102, "Real Madrid CF", "FC Barcelona",
                               "Primera Division", "Spain", "Football",
                               start_time,
                               [{"type": "1", "odds": 1.95},
                                {"type": "X", "odds": 3.35},
                                {"type": "2", "odds": 3.75}]),
        103: create_test_event(103, "Novak Djokovic", "Rafael Nadal",
                               "Wimbledon", "UK", "Tennis", start_time,
                               [{"type": "1", "odds": 1.85},
                                {"type": "2", "odds": 2.05}]),
        104: create_test_event(104, "Hashtag", "Brackley", "England Fa Trophy", "England",
                               "Football", start_time,
                               [{"type": "1", "odds": 1.5},
                                {"type": "X", "odds": 4.0},
                                {"type": "2", "odds": 6.0}])
    }

    return pinnacle_events, other_events


def test_match_pairer():
    pinnacle_events, other_events = create_test_data()

    # Обновленная строка: добавляем название букмекерской конторы
    match_pairer = MatchPairer(bookmaker="test_bookmaker", debug=True)

    print("Pinnacle events:")
    for event_id, event in pinnacle_events.items():
        print(
            f"ID: {event_id}, {event['home_team']} vs {event['away_team']}, League: {event['league_name']}, Country: {event['country']}")

    print("\nOther bookmaker events:")
    for event_id, event in other_events.items():
        print(
            f"ID: {event_id}, {event['home_team']} vs {event['away_team']}, League: {event['league_name']}, Country: {event['country']}")

    print("\nMatching events...")
    matched_events, unmatched_pinnacle, unmatched_other = match_pairer.match_events(
        pinnacle_events, other_events)

    print("\nMatched events:")
    for event in matched_events:
        print(json.dumps(event, indent=2))

    print("\nUnmatched Pinnacle events:")
    for event_id, event in unmatched_pinnacle.items():
        print(f"ID: {event_id}, {event['home_team']} vs {event['away_team']}")

    print("\nUnmatched Other Bookmaker events:")
    for event_id, event in unmatched_other.items():
        print(f"ID: {event_id}, {event['home_team']} vs {event['away_team']}")


if __name__ == "__main__":
    test_match_pairer()