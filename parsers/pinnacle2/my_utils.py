# my_utils.py
import asyncio
import logging
import struct
import threading
import time

from snappy import snappy

import config
import modules.pinnacleapi as ps
from datetime import datetime, timedelta, timezone
import os
import json

from parsers.utils import save_odds_to_jsonl

SPORT_IDs = {
    'Football': 29,
    'Tennis': 33,
    'Ice Hockey': 19,
}

pinAccount = ps.PinAccount()
pinMarket = ps.PinMarket()

matches_lock = threading.Lock()
matches_data = {
    'Football': {"leagues": {}, "events": {}},
    'Tennis': {"leagues": {}, "events": {}},
    'Ice Hockey': {"leagues": {}, "events": {}}
}

matches_lock_live = threading.Lock()
matches_data_live = {
    'Football': {"leagues": {}, "events": {}},
    'Tennis': {"leagues": {}, "events": {}},
    'Ice Hockey': {"leagues": {}, "events": {}}
}

last_matches = 0

odds_lock = threading.Lock()
odds_data = {}
last_odds = 0

leagues_data = {
    'Football': {},
    'Tennis': {},
    'Ice Hockey': {}
}


async def fetch_leagues():
    global leagues_data
    today = datetime.now().strftime("%Y-%m-%d")

    for sport, sport_id in SPORT_IDs.items():
        file_name = f"leagues/leagues_{sport}_{today}.json"
        if not os.path.exists("leagues"):
            os.makedirs("leagues")

        if not os.path.exists(file_name):
            try:
                leagues = await pinMarket.get_leagues(sport_id)
                if not leagues:
                    print(f"Error fetching {sport} leagues")
                    exit()
                with open(file_name, "w") as file:
                    json.dump(leagues, file)
                print(f"{sport} leagues data saved to {file_name}")

                leagues_data[sport] = {
                    league["id"]: {
                        "name": league["name"],
                        "homeTeamType": league["homeTeamType"],
                        "country": league["container"]
                    } for league in leagues["leagues"]
                }
                print(f"{sport} leagues data loaded into memory")

            except Exception as e:
                logging.error(f"Error fetching {sport} leagues: {e}")
        else:
            print(
                f"{sport} leagues data for today already exists in {file_name}")
            with open(file_name, "r") as file:
                leagues = json.load(file)
                leagues_data[sport] = {
                    league["id"]: {
                        "name": league["name"],
                        "homeTeamType": league["homeTeamType"],
                        "country": league["container"]
                    } for league in leagues["leagues"]
                }
            print(f"{sport} leagues data loaded from file into memory")


asyncio.run(fetch_leagues())



def calculate_margin(odds):
    return sum(1 / o for o in odds.values()) if odds else None


def get_team_names_by_event_id(event_id):
    with matches_lock:
        for sport in matches_data:
            event = matches_data[sport]["events"].get(event_id)
            if event:
                return event["home"], event["away"]
    with matches_lock_live:
        for sport in matches_data_live:
            event = matches_data_live[sport]["events"].get(event_id)
            if event:
                return event["home"], event["away"]
    return None, None


def add_outcome(processed_data, outcome_type_name, outcome_type, line, odds,
                line_id, alt_line_id=None, period_number=0, team=None,
                side=None, bet_type=None):
    """Adds a new outcome to processed_data."""
    if odds is None:
        return
    processed_data["outcomes"].append({
        "type_name": outcome_type_name,
        "type": outcome_type,
        "line": line,
        "odds": odds,
        "line_id": line_id,
        "alt_line_id": alt_line_id,
        "period_number": period_number,
        "team": team,
        "side": side,
        "bet_type": bet_type
    })


def handle_moneyline(processed_data, moneyline, period_prefix, period_number,
                     is_games, line_id):
    """Handles the moneyline bet type."""
    bet_type = "MONEYLINE"
    if is_games:

        add_outcome(processed_data, f"{period_prefix}1X2",
                    f"{period_number}G1", 0, moneyline.get("home"),
                    line_id, period_number=period_number, team="TEAM1",
                    bet_type=bet_type)
        add_outcome(processed_data, f"{period_prefix}1X2",
                    f"{period_number}G2", 0, moneyline.get("away"),
                    line_id, period_number=period_number, team="TEAM2",
                    bet_type=bet_type)
    else:
        add_outcome(processed_data, f"{period_prefix}1X2",
                    f"{period_prefix}1", 0, moneyline.get("home"), line_id,
                    period_number=period_number, team="TEAM1",
                    bet_type=bet_type)
        add_outcome(processed_data, f"{period_prefix}1X2",
                    f"{period_prefix}X", 0, moneyline.get("draw"), line_id,
                    period_number=period_number, team="DRAW",
                    bet_type=bet_type)
        add_outcome(processed_data, f"{period_prefix}1X2",
                    f"{period_prefix}2", 0, moneyline.get("away"), line_id,
                    period_number=period_number, team="TEAM2",
                    bet_type=bet_type)


def handle_spreads(processed_data, spreads, period_prefix, period_number,
                   is_games, line_id):
    """Handles the spread (handicap) bet type."""
    bet_type = "SPREAD"
    for spread in spreads:
        hdp = spread["hdp"]
        alt_line_id = spread.get("altLineId")
        home_odds = spread.get("home")
        away_odds = spread.get("away")

        if home_odds:
            if is_games:
                add_outcome(processed_data, "Asian Handicap",
                            f"{period_prefix}GAH1" if period_number else "GAH1",
                            hdp, home_odds, line_id, alt_line_id,
                            period_number, team="TEAM1", bet_type=bet_type)
            else:
                add_outcome(processed_data, f"{period_prefix}Asian Handicap",
                            f"{period_prefix}AH1", hdp, home_odds, line_id,
                            alt_line_id, period_number, team="TEAM1",
                            bet_type=bet_type)

        if away_odds:
            if is_games:
                add_outcome(processed_data, "Asian Handicap",
                            f"{period_prefix}GAH2" if period_number else "GAH2",
                            -hdp, away_odds, line_id, alt_line_id,
                            period_number, team="TEAM2", bet_type=bet_type)
            else:
                add_outcome(processed_data, f"{period_prefix}Asian Handicap",
                            f"{period_prefix}AH2", -hdp, away_odds, line_id,
                            alt_line_id, period_number, team="TEAM2",
                            bet_type=bet_type)


def handle_totals(processed_data, totals, period_prefix, period_number,
                  is_games, line_id):
    """Handles the totals (over/under) bet type."""
    bet_type = "TOTAL_POINTS"
    for total in totals:
        if total and total.get("over") and total.get("under"):
            points = total["points"]
            alt_line_id = total.get("altLineId")
            over_odds = total.get("over")
            under_odds = total.get("under")
            if is_games:
                add_outcome(processed_data, "Total Goals",
                            f"{period_prefix}GO", points, over_odds, line_id,
                            alt_line_id,
                            period_number, side="OVER", bet_type=bet_type)
                add_outcome(processed_data, "Total Goals",
                            f"{period_prefix}GU", points, under_odds, line_id,
                            alt_line_id,
                            period_number, side="UNDER", bet_type=bet_type)
            else:
                add_outcome(processed_data, f"{period_prefix}Total Goals",
                            f"{period_prefix}O", points, over_odds, line_id,
                            alt_line_id, period_number, side="OVER",
                            bet_type=bet_type)
                add_outcome(processed_data, f"{period_prefix}Total Goals",
                            f"{period_prefix}U", points, under_odds, line_id,
                            alt_line_id, period_number, side="UNDER",
                            bet_type=bet_type)


def handle_team_total(processed_data, team_totals, period_prefix,
                      period_number, is_games, line_id):
    """Handles the team total points bet type."""
    bet_type = "TEAM_TOTAL_POINTS"
    for team, total in team_totals.items():
        team_prefix = "H" if team == "home" else "A"
        if total and total.get("over") and total.get("under"):
            points = total["points"]
            alt_line_id = total.get("altLineId")
            over_odds = total.get("over")
            under_odds = total.get("under")
            if is_games:
                add_outcome(processed_data,
                            f"Team Total {team.capitalize()}",
                            f"{period_prefix}GT{team_prefix}O", points,
                            over_odds, line_id,
                            alt_line_id, period_number,
                            team="TEAM1" if team == "home" else "TEAM2",
                            side="OVER", bet_type=bet_type)
                add_outcome(processed_data,
                            f"Team Total {team.capitalize()}",
                            f"{period_prefix}GT{team_prefix}U", points,
                            under_odds, line_id,
                            alt_line_id, period_number,
                            team="TEAM1" if team == "home" else "TEAM2",
                            side="UNDER", bet_type=bet_type)
            else:
                add_outcome(processed_data,
                            f"{period_prefix}Team Total {team.capitalize()}",
                            f"{period_prefix}T{team_prefix}O", points,
                            over_odds, line_id, alt_line_id, period_number,
                            team="TEAM1" if team == "home" else "TEAM2",
                            side="OVER", bet_type=bet_type)
                add_outcome(processed_data,
                            f"{period_prefix}Team Total {team.capitalize()}",
                            f"{period_prefix}T{team_prefix}U", points,
                            under_odds, line_id, alt_line_id, period_number,
                            team="TEAM1" if team == "home" else "TEAM2",
                            side="UNDER", bet_type=bet_type)


def process_match_data(event_data, is_live=False):
    event_id = str(event_data["id"])
    home_team, away_team = get_team_names_by_event_id(event_id)
    event_info = None
    sport_ = None

    with matches_lock:
        for sport in matches_data:
            event_info = matches_data[sport]["events"].get(event_id)
            if event_info:
                sport_ = sport
                break

    if not event_info:
        with matches_lock_live:
            for sport in matches_data_live:
                event_info = matches_data_live[sport]["events"].get(event_id)
                if event_info:
                    sport_ = sport
                    break

    if event_info:
        league_id = event_info.get("league_id")
        league_name = event_info.get("league_name")
        country = event_info.get("country")
        starts = event_info.get("starts")
    else:
        logging.warning(
            f"Match with ID {event_id} not found in either matches_data or matches_data_live")
        league_id = league_name = country = starts = sport_ = None

    if not home_team or not away_team:
        return

    if "Bookings" in home_team or "Bookings" in away_team:
        return

    # Determine match type without changing the variable 'type'
    if "Sets" in home_team or "Sets" in away_team:
        match_type = "Sets"
    elif "Games" in home_team or "Games" in away_team:
        match_type = "Games"
    else:
        match_type = ""

    # Clean up team names
    home_team = home_team.replace(" (Sets)", "").replace(" (Games)", "")
    away_team = away_team.replace(" (Sets)", "").replace(" (Games)", "")

    # Convert start time to timestamp
    start_timestamp = datetime.fromisoformat(
        starts.replace('Z', '+00:00')
    ).timestamp() if starts else None

    if not start_timestamp:
        logging.warning(f"Invalid start time for match {event_id}")
        return

    processed_data = {
        "event_id": event_id,
        "match_name": f"{home_team} vs {away_team}".replace("/", ""),
        "start_time": start_timestamp,
        "home_team": home_team,
        "away_team": away_team,
        "league_id": league_id,
        "league": league_name,
        "country": country,
        "sport": sport_,
        "type": "PreMatch" if not is_live else "Live",
        "outcomes": [],
        "time": time.time(),
    }
    # print(event_data)

    for period in event_data.get("periods", []):
        # print("period:", period)
        if period.get("status") != 1:
            continue
        period_number = period.get("number", 0)
        if sport == "Ice Hockey" and period_number == 0:
            continue
        elif sport == "Ice Hockey" and period_number == 6:
            period_number = 0

        period_prefix = "" if period_number == 0 else f"{period_number}H"
        moneyline = period.get("moneyline")
        spreads = period.get("spreads", [])
        totals = period.get("totals", [])
        team_total = period.get("teamTotal", {})
        line_id = period.get("lineId")

        is_games = (match_type == "Games")

        if moneyline:
            handle_moneyline(processed_data, moneyline, period_prefix,
                             period_number, is_games, line_id)

        if spreads:
            handle_spreads(processed_data, spreads, period_prefix,
                           period_number, is_games, line_id)

        if totals:
            handle_totals(processed_data, totals, period_prefix,
                          period_number, is_games, line_id)

        if team_total:
            handle_team_total(processed_data, team_total, period_prefix,
                              period_number, is_games, line_id)

    if processed_data["outcomes"]:
        save_odds_to_jsonl(processed_data["match_name"], processed_data)
        return processed_data
    else:
        save_odds_to_jsonl(processed_data["match_name"], processed_data)
        # logging.warning(f"No valid outcomes for match {event_id}")
        return processed_data


def process_matches_data(matches, sport):
    leagues = {}
    events = {}

    if isinstance(matches, dict) and "league" in matches:
        for league in matches["league"]:
            league_id = league["id"]
            league_name = league["name"]
            leagues[league_id] = league_name

            for event in league.get("events", []):
                if event.get("resultingUnit") == "Corners":
                    continue
                if 'home' not in event or 'away' not in event or 'id' not in event:
                    continue
                event_id = str(event["id"])
                country = leagues_data[sport].get(league_id, {}).get(
                    "country")
                events[event_id] = {
                    "id": event_id,
                    "home": event["home"],
                    "away": event["away"],
                    "starts": event["starts"],
                    "league_id": league_id,
                    "league_name": league_name,
                    "country": country,
                    "sport": sport
                }

    return leagues, events


async def fetch_and_save_matches():
    global matches_data
    while True:
        try:
            for sport, sport_id in SPORT_IDs.items():
                res = await pinMarket.get_fixtures(sportid=sport_id, live=0,
                                                   since=str(0))
                with matches_lock_live:
                    live_matches = matches_data_live[sport]["events"]
                if res:
                    leagues, events = process_matches_data(res, sport)
                    events_filtered = {}
                    for event_id, event in events.items():
                        match_time = datetime.fromisoformat(
                            event['starts'].replace('Z', '+00:00'))
                        if datetime.now(
                                tz=timezone.utc) <= match_time <= datetime.now(
                            tz=timezone.utc) + timedelta(
                            hours=config.PRE_MATCH_HOURS_AHEAD):
                            if "Bookings" in event.get("home",
                                                       "") or "Bookings" in event.get(
                                "away", ""):
                                continue
                            if event_id not in live_matches:
                                events_filtered[event_id] = event

                    with matches_lock:
                        matches_data[sport]["leagues"] = leagues
                        matches_data[sport]["events"] = events_filtered
                        print(f"Updated {sport} matches data")
        except Exception as e:
            logging.error(f"Error fetching matches: {e}")
        await asyncio.sleep(60)


async def fetch_and_save_matches_live():
    global matches_data_live
    while True:
        try:
            for sport, sport_id in SPORT_IDs.items():
                res = await pinMarket.get_fixtures(sportid=sport_id, live=1,
                                                   since=str(0))
                if res:
                    leagues, events = process_matches_data(res, sport)
                    events_day = {}
                    for event_id, event in events.items():
                        match_time = datetime.fromisoformat(
                            event['starts'].replace('Z', '+00:00'))
                        if match_time <= datetime.now(
                                tz=timezone.utc) + timedelta(days=0.1):
                            if "Bookings" in event.get("home",
                                                       "") or "Bookings" in event.get(
                                "away", ""):
                                continue
                            events_day[event_id] = event

                    with matches_lock_live:
                        matches_data_live[sport]["leagues"] = leagues
                        matches_data_live[sport]["events"] = events_day
                        print(f"Updated {sport} matches data")
        except Exception as e:
            logging.error(f"Error fetching matches: {e}")
        await asyncio.sleep(60)


async def fetch_matches():
    all_leagues = {}
    all_events = {}
    try:
        for sport, sport_id in SPORT_IDs.items():
            res = await pinMarket.get_fixtures(sportid=sport_id, live=0)
            if res:
                leagues, events = process_matches_data(res, sport)
                events_day = {}
                for event_id, event in events.items():
                    match_time = datetime.fromisoformat(
                        event['starts'].replace('Z', '+00:00'))
                    if match_time <= datetime.now(timezone.utc) + timedelta(
                            days=1):
                        events_day[event_id] = event

                all_leagues[sport] = leagues
                all_events[sport] = events_day

        return all_leagues, all_events
    except Exception as e:
        logging.error(f"Error fetching matches: {e}")
