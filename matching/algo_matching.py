from mappings import mappings
from typing import Dict, Any, List, Tuple
from fuzzywuzzy import fuzz
from collections import defaultdict
from datetime import datetime, timedelta


class MatchPairer:
    def __init__(self, bookmaker: str, debug: bool = False):
        self.mappings = mappings
        self.bookmaker = bookmaker
        self.debug = debug
        self.high_fuzz_threshold = 85
        self.low_fuzz_threshold = 62

    def load_matched_events(self) -> Dict[Tuple[int, int], Dict[str, Any]]:
        events = self.mappings.load_matched_events(self.bookmaker)
        return {(event['pinnacle_id'], event['other_id']): event for event in
                events}

    def match_events(self, pinnacle_events: Dict[int, Dict[str, Any]],
                     other_bookmaker_events: Dict[int, Dict[str, Any]]) -> \
            Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]], Dict[
                int, Dict[str, Any]]]:
        self.matched_events = self.load_matched_events()
        matched_events = []
        unmatched_pinnacle = dict(pinnacle_events)
        unmatched_other = dict(other_bookmaker_events)
        # Удаляем уже сопоставленные события
        for (pin_id, other_id), event in self.matched_events.items():
            unmatched_pinnacle.pop(pin_id, None)
            unmatched_other.pop(other_id, None)

        # Первый круг: сопоставление с учетом лиги
        grouped_pinnacle = self.group_events(unmatched_pinnacle)
        grouped_other = self.group_events(unmatched_other)

        # New step: Match based on one known team
        single_team_matches = self.match_on_single_team(grouped_pinnacle,
                                                        grouped_other,
                                                        unmatched_pinnacle,
                                                        unmatched_other)
        matched_events.extend(single_team_matches)

        for match in single_team_matches:
            unmatched_pinnacle.pop(match['pinnacle_id'], None)
            unmatched_other.pop(match['other_id'], None)

        first_round_matches = self.first_round_matching(grouped_pinnacle,
                                                        grouped_other,
                                                        unmatched_pinnacle,
                                                        unmatched_other)
        matched_events.extend(first_round_matches)

        for match in first_round_matches:
            unmatched_pinnacle.pop(match['pinnacle_id'], None)
            unmatched_other.pop(match['other_id'], None)

        # Второй круг: сопоставление без учета лиги
        grouped_pinnacle_no_league = self.group_events_no_league(
            unmatched_pinnacle)
        grouped_other_no_league = self.group_events_no_league(unmatched_other)

        second_round_matches = self.second_round_matching(
            grouped_pinnacle_no_league, grouped_other_no_league,
            unmatched_pinnacle, unmatched_other)
        matched_events.extend(second_round_matches)

        for match in second_round_matches:
            unmatched_pinnacle.pop(match['pinnacle_id'], None)
            unmatched_other.pop(match['other_id'], None)

        # Третий круг: сопоставление с учетом времени, округленного до дня
        grouped_pinnacle_by_day = self.group_events_by_day(unmatched_pinnacle)
        grouped_other_by_day = self.group_events_by_day(unmatched_other)

        third_round_matches = self.third_round_matching(
            grouped_pinnacle_by_day, grouped_other_by_day, unmatched_pinnacle,
            unmatched_other)
        matched_events.extend(third_round_matches)

        for match in third_round_matches:
            unmatched_pinnacle.pop(match['pinnacle_id'], None)
            unmatched_other.pop(match['other_id'], None)
        # print(unmatched_pinnacle)
        # print(unmatched_other)
        # Сохраняем результаты через mappings
        self.mappings.save_matched_events(self.bookmaker, matched_events)
        self.mappings.save_unmatched_events(self.bookmaker,
                                            unmatched_pinnacle,
                                            unmatched_other)

        return matched_events, unmatched_pinnacle, unmatched_other

    def compare_tennis_names(self, pinnacle_name, other_name, bookmaker):
        def process_name(name, is_pinnacle=False):

            if is_pinnacle and bookmaker.lower() == 'sansabet':
                # Для Pinnacle: оставляем только первую букву фамилии
                words = name.split()
                if len(words) > 1:
                    return f"{' '.join(words[1:])} {words[0][0]}."

            if is_pinnacle and bookmaker.lower() == 'fonbet':
                # Для Pinnacle: оставляем только первую букву фамилии
                words = name.split()
                if len(words) > 1:
                    name = words[0]
                    if len(other_name.split()[-1]) == 1:
                        name = name[0]
                    return f"{' '.join(words[1:])} {name}"
            if not is_pinnacle and bookmaker.lower() == 'admiralbet_me':
                #     имя и фамилия записаны через запятую, меняем местами
                if ',' in name:
                    last_name, first_name = name.split(',', 1)
                    return f"{first_name.strip()} {last_name.strip()}"
            # elif any(bk in bookmaker.lower() for bk in
            #          ['unibet', 'bingoal', 'scooore']):
            #     # Для Unibet, Bingoal, Scooore: меняем местами части имени
            #     if ',' in name:
            #         last_name, first_name = name.split(',', 1)
            #         return f"{first_name.strip()} {last_name.strip()}"

            return name

        processed_pinnacle = process_name(pinnacle_name, is_pinnacle=True)
        processed_other = process_name(other_name)

        similarity = fuzz.ratio(processed_pinnacle.lower(),
                                processed_other.lower())
        # if similarity > 85:
        #     print(f"Сравнение имен: Сравнение имен{processed_pinnacle} vs {processed_other} -> {similarity}")

        return similarity

    def group_events(self, events: Dict[int, Dict[str, Any]]) -> Dict[
        Tuple[str, datetime, str, str], List[int]]:
        grouped = defaultdict(list)
        for event_id, event in events.items():
            start_time = datetime.fromtimestamp(event["start_time"])
            rounded_time = self.round_time(start_time, timedelta(minutes=10))
            country = event["country"]
            if not country:
                country = "Unknown"
            country = country.lower()
            # print(country)
            country = self.mappings.get_country(self.bookmaker, country)
            # print(country)
            league = event["league"]
            league = self.mappings.get_league(self.bookmaker, league)

            sport = event.get("sport", "Unknown")
            if sport == "Tennis":
                if "wta" in league.lower():
                    league = "WTA"
                elif "challenger" in league.lower():
                    league = "Challenger"
                elif "atp" in league.lower():
                    league = "ATP"
                elif "itf" in league.lower():
                    league = "ITF"

                translated_league = league
                league = translated_league
                country = "None"
            key = (country, rounded_time, league, sport)
            grouped[key].append(event_id)
        return grouped

    def group_events_no_league(self, events: Dict[int, Dict[str, Any]]) -> \
            Dict[
                Tuple[str, datetime, str], List[int]]:
        grouped = defaultdict(list)
        for event_id, event in events.items():
            start_time = datetime.fromtimestamp(
                event["start_time"])
            rounded_time = self.round_time(start_time, timedelta(minutes=10))
            country = event["country"]
            if not country:
                country = "Unknown"
            country = country.lower()
            country = self.mappings.get_country(self.bookmaker, country)
            sport = event.get("sport", "Unknown")
            if sport == "Tennis":
                country = "None"

            key = (country, rounded_time, sport)
            grouped[key].append(event_id)
        return grouped

    def group_events_by_day(self, events: Dict[int, Dict[str, Any]]) -> Dict[
        Tuple[str, datetime.date, str], List[int]]:
        grouped = defaultdict(list)
        for event_id, event in events.items():
            start_time = datetime.fromtimestamp(
                event["start_time"])
            day = start_time.date()
            country = event["country"]
            if not country:
                country = "Unknown"
            country = country.lower()
            country = self.mappings.get_country(self.bookmaker, country)
            league = event["league"]
            league = self.mappings.get_league(self.bookmaker, league)
            sport = event.get("sport", "Unknown")

            if sport == "Tennis":
                translated_league = league
                if "wta" in league.lower():
                    translated_league = "WTA"
                elif "challenger" in league.lower():
                    translated_league = "Challenger"
                elif "atp" in league.lower():
                    translated_league = "ATP"
                elif "itf" in league.lower():
                    translated_league = "ITF"
                league = translated_league
                country = "None"
            key = (country, day, sport, league)
            grouped[key].append(event_id)
        return grouped

    @staticmethod
    def round_time(dt: datetime, delta: timedelta) -> datetime:
        return dt - (dt - datetime.min) % delta

    def first_round_matching(self, grouped_pinnacle: Dict[
        Tuple[str, datetime, str, str], List[int]],
                             grouped_other: Dict[
                                 Tuple[str, datetime, str, str], List[int]],
                             pinnacle_events: Dict[int, Dict[str, Any]],
                             other_events: Dict[int, Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        matches = []
        for key in set(grouped_pinnacle.keys()) & set(grouped_other.keys()):
            pinnacle_group = grouped_pinnacle[key]
            other_group = grouped_other[key]

            country, _, league, sport = key

            if self.debug:
                print(f"Processing group: {country}, {league}, {sport}")

            for p_id in pinnacle_group:
                pinnacle_event = pinnacle_events[p_id]
                for o_id in other_group:
                    other_event = other_events[o_id]

                    mapped_home_team, mapped_away_team = self.get_mapped_teams(
                        country, league, other_event, sport)

                    if self.debug:
                        print(
                            f"Comparing: {pinnacle_event['home_team']} vs {mapped_home_team}, {pinnacle_event['away_team']} vs {mapped_away_team}")

                    pin_home = pinnacle_event['home_team']
                    pin_away = pinnacle_event['away_team']

                    similarity = fuzz.ratio(f"{pin_home} {pin_away}",
                                            f"{mapped_home_team} {mapped_away_team}")
                    if sport == "Tennis":
                        similarity = (self.compare_tennis_names(pin_home,
                                                                mapped_home_team,
                                                                self.bookmaker) + self.compare_tennis_names(
                            pin_away, mapped_away_team, self.bookmaker)) / 2

                    # Используем низкий порог, если коэффициенты подходят
                    if self.check_no_value_and_outcome_count(pinnacle_event,
                                                             other_event):
                        fuzz_threshold = self.low_fuzz_threshold
                    else:
                        fuzz_threshold = self.high_fuzz_threshold

                    if similarity > fuzz_threshold:
                        match = self.create_match(p_id, o_id, pinnacle_events,
                                                  other_events)
                        matches.append(match)
                        if self.debug:
                            print(f"Match found: {match}")
                        # Добавляем новый маппинг
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['home_team'],
                                                  pinnacle_event['home_team'],
                                                  country, league)
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['away_team'],
                                                  pinnacle_event['away_team'],
                                                  country, league)
                        # Добавляем маппинг для лиги
                        self.mappings.add_mapping(self.bookmaker, 'leagues',
                                                  other_event['league'],
                                                  pinnacle_event['league'],
                                                  country)
                        break  # Прерываем внутренний цикл после нахождения соответствия
                if p_id in [match['pinnacle_id'] for match in matches]:
                    break  # Прерываем внешний цикл, если нашли соответствие

        return matches

    def second_round_matching(self, grouped_pinnacle: Dict[
        Tuple[str, datetime, str], List[int]],
                              grouped_other: Dict[
                                  Tuple[str, datetime, str], List[int]],
                              pinnacle_events: Dict[int, Dict[str, Any]],
                              other_events: Dict[int, Dict[str, Any]]) -> \
            List[Dict[str, Any]]:
        matches = []
        for key in set(grouped_pinnacle.keys()) & set(grouped_other.keys()):
            pinnacle_group = grouped_pinnacle[key]
            other_group = grouped_other[key]

            country, _, sport = key

            if self.debug:
                print(f"Processing group (2nd round): {country}, {sport}")

            for p_id in pinnacle_group:
                pinnacle_event = pinnacle_events[p_id]
                for o_id in other_group:
                    other_event = other_events[o_id]

                    pin_home = pinnacle_event['home_team']
                    pin_away = pinnacle_event['away_team']
                    other_home = other_event['home_team']
                    other_away = other_event['away_team']

                    similarity = fuzz.ratio(f"{pin_home} {pin_away}",
                                            f"{other_home} {other_away}")
                    # Используем низкий порог, если коэффициенты подходят
                    if self.check_no_value_and_outcome_count(pinnacle_event,
                                                             other_event):
                        fuzz_threshold = 70
                    else:
                        fuzz_threshold = 90

                    if sport == "Tennis":
                        similarity = (self.compare_tennis_names(pin_home,
                                                                other_home,
                                                                self.bookmaker) + self.compare_tennis_names(
                            pin_away, other_away, self.bookmaker)) / 2
                    if similarity > fuzz_threshold:  # Повышенное требование к соответствию
                        match = self.create_match(p_id, o_id, pinnacle_events,
                                                  other_events)
                        matches.append(match)
                        if self.debug:
                            print(f"Second round match found: {match}")
                        # Добавляем новый маппинг с учетом лиги
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['home_team'],
                                                  pinnacle_event['home_team'],
                                                  country,
                                                  pinnacle_event['league'])
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['away_team'],
                                                  pinnacle_event['away_team'],
                                                  country,
                                                  pinnacle_event['league'])
                        # Добавляем маппинг для лиги
                        self.mappings.add_mapping(self.bookmaker, 'leagues',
                                                  other_event['league'],
                                                  pinnacle_event['league'],
                                                  country)
                        break  # Прерываем внутренний цикл после нахождения соответствия
                if p_id in [match['pinnacle_id'] for match in matches]:
                    break  # Прерываем внешний цикл, если нашли соответствие

        return matches

    def third_round_matching(self, grouped_pinnacle: Dict[
        Tuple[str, datetime.date, str], List[int]],
                             grouped_other: Dict[
                                 Tuple[str, datetime.date, str], List[int]],
                             pinnacle_events: Dict[int, Dict[str, Any]],
                             other_events: Dict[int, Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        matches = []
        for key in set(grouped_pinnacle.keys()) & set(grouped_other.keys()):
            pinnacle_group = grouped_pinnacle[key]
            other_group = grouped_other[key]

            country, _, sport, league = key

            if self.debug:
                print(f"Processing group (3rd round): {country}, {sport}")

            for p_id in pinnacle_group:
                pinnacle_event = pinnacle_events[p_id]
                for o_id in other_group:
                    other_event = other_events[o_id]

                    pin_home = pinnacle_event['home_team']
                    pin_away = pinnacle_event['away_team']
                    other_home = other_event['home_team']
                    other_away = other_event['away_team']

                    similarity = fuzz.ratio(f"{pin_home} {pin_away}",
                                            f"{other_home} {other_away}")
                    # Используем низкий порог, если коэффициенты подходят
                    if self.check_no_value_and_outcome_count(pinnacle_event,
                                                             other_event):
                        fuzz_threshold = 70
                    else:
                        fuzz_threshold = 85
                    if sport == "Tennis":
                        similarity = (self.compare_tennis_names(pin_home,
                                                                other_home,
                                                                self.bookmaker) + self.compare_tennis_names(
                            pin_away, other_away, self.bookmaker)) / 2
                    if similarity > fuzz_threshold:
                        match = self.create_match(p_id, o_id, pinnacle_events,
                                                  other_events)
                        matches.append(match)
                        if self.debug:
                            print(f"Third round match found: {match}")
                        # Добавляем новый маппинг с учетом лиги
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['home_team'],
                                                  pinnacle_event['home_team'],
                                                  country,
                                                  pinnacle_event['league'])
                        self.mappings.add_mapping(self.bookmaker, 'teams',
                                                  other_event['away_team'],
                                                  pinnacle_event['away_team'],
                                                  country,
                                                  pinnacle_event['league'])
                        # Добавляем маппинг для лиги
                        self.mappings.add_mapping(self.bookmaker, 'leagues',
                                                  other_event['league'],
                                                  pinnacle_event['league'],
                                                  country)
                        break  # Прерываем внутренний цикл после нахождения соответствия
                if p_id in [match['pinnacle_id'] for match in matches]:
                    break  # Прерываем внешний цикл, если нашли соответствие

        return matches

    def match_on_single_team(self, grouped_pinnacle: Dict[
        Tuple[str, datetime, str, str], List[int]],
                             grouped_other: Dict[
                                 Tuple[str, datetime, str, str], List[int]],
                             pinnacle_events: Dict[int, Dict[str, Any]],
                             other_events: Dict[int, Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        new_matches = []
        for key in set(grouped_pinnacle.keys()) & set(grouped_other.keys()):
            pinnacle_group = grouped_pinnacle[key]
            other_group = grouped_other[key]

            country, _, league, sport = key

            for p_id in list(pinnacle_group):
                pinnacle_event = pinnacle_events[p_id]
                pinnacle_home = pinnacle_event['home_team']
                pinnacle_away = pinnacle_event['away_team']

                matched_home = self.mappings.get_team(self.bookmaker, country,
                                                      league, pinnacle_home)
                matched_away = self.mappings.get_team(self.bookmaker, country,
                                                      league, pinnacle_away)

                if matched_home or matched_away:
                    for o_id in list(other_group):
                        other_event = other_events[o_id]
                        other_home = other_event['home_team']
                        other_away = other_event['away_team']

                        if (matched_home and other_home == matched_home) or \
                                (matched_away and other_away == matched_away):
                            match = self.create_match(p_id, o_id,
                                                      pinnacle_events,
                                                      other_events)
                            new_matches.append(match)
                            pinnacle_group.remove(p_id)
                            other_group.remove(o_id)
                            break

        return new_matches

    def get_mapped_teams(self, country: str, league: str,
                         event: Dict[str, Any], sport: str) -> Tuple[
        str, str]:
        home_team = event['home_team']
        away_team = event['away_team']

        if sport == "Tennis":
            home_team = home_team.replace(" (Sets)", "").replace(" (Games)",
                                                                 "")
            away_team = away_team.replace(" (Sets)", "").replace(" (Games)",
                                                                 "")

        mapped_home_team = self.mappings.get_team(self.bookmaker, country,
                                                  league, home_team)
        mapped_away_team = self.mappings.get_team(self.bookmaker, country,
                                                  league, away_team)

        # Добавляем логику для добавления лиги в переводы, если соответствие по Левенштейну >= 85%
        if fuzz.ratio(home_team, mapped_home_team) >= 85:
            self.mappings.add_mapping(self.bookmaker, 'teams', home_team,
                                      mapped_home_team, country, league)
        if fuzz.ratio(away_team, mapped_away_team) >= 85:
            self.mappings.add_mapping(self.bookmaker, 'teams', away_team,
                                      mapped_away_team, country, league)

        return mapped_home_team, mapped_away_team

    def create_match(self, pinnacle_id: int, other_id: int,
                     pinnacle_events: Dict[int, Dict[str, Any]],
                     other_events: Dict[int, Dict[str, Any]]) -> Dict[
        str, Any]:
        pinnacle_event = pinnacle_events[pinnacle_id]
        other_event = other_events[other_id]

        # Используем реальную страну из данных события
        country = pinnacle_event["country"]
        if not country:
            country = "Unknown"

        return {
            "pinnacle_id": pinnacle_id,
            "other_id": other_id,
            "pinnacle_match_name": f"{pinnacle_event['home_team']} vs {pinnacle_event['away_team']}",
            "other_match_name": f"{other_event['home_team']} vs {other_event['away_team']}",
            "pinnacle_home_team": pinnacle_event['home_team'],
            "pinnacle_away_team": pinnacle_event['away_team'],
            "other_home_team": other_event['home_team'],
            "other_away_team": other_event['away_team'],
            "start_time": pinnacle_event["start_time"],
            "pinnacle_league": pinnacle_event["league"],
            "other_league": other_event["league"],
            "country": country,
            "sport": pinnacle_event.get("sport", "Unknown"),
        }

    def check_no_value_and_outcome_count(self, event1: Dict[str, Any],
                                         event2: Dict[str, Any]) -> bool:
        outcomes1 = self.get_outcomes_dict(event1.get('outcomes', []))
        outcomes2 = self.get_outcomes_dict(event2.get('outcomes', []))

        common_outcomes = set(outcomes1.keys()) & set(outcomes2.keys())

        valid_outcomes_count = sum(1 for outcome in common_outcomes
                                   if outcomes1[outcome] < 4 and outcomes2[
                                       outcome] < 4)

        if valid_outcomes_count < 4:
            return False

        for outcome in common_outcomes:
            if outcomes1[outcome] < 4 and outcomes2[outcome] < 4:
                if self.calculate_value(outcomes1[outcome],
                                        outcomes2[outcome]):
                    return False

        return True

    @staticmethod
    def get_outcomes_dict(outcomes: List[Dict[str, Any]]) -> Dict[
        tuple, float]:
        return {(outcome.get('type'), outcome.get('line', 0)): outcome['odds']
                for outcome in outcomes}

    @staticmethod
    def calculate_value(odds1, odds2, margin=1.08):
        return odds1 * margin * 1.05 < odds2

    def remove_team_match(self, country: str, pinnacle_league: str,
                          pinnacle_home: str, pinnacle_away: str,
                          other_home: str, other_away: str):
        self.mappings.remove_mapping(self.bookmaker, 'teams', other_home,
                                     pinnacle_home, country, pinnacle_league)
        self.mappings.remove_mapping(self.bookmaker, 'teams', other_away,
                                     pinnacle_away, country, pinnacle_league)
