import csv
import math
import os
import sys
import time
import traceback
import copy

from datetime import datetime

import config

from modules.service import convert_us_to_dec


class Pinnacle_utils:
    def get_leagues(self, matches, only_id=0, only_name=0):
        result = {} if not only_id and not only_name else []

        for league in matches['league']:
            if only_id:
                result.append(league['id'])
            elif only_name:
                result.append(league['name'])
            else:
                result[league['id']] = league['name']
        return result

    def get_leagues_with_open_events(self, matches, only_id=0, only_name=0):
        result = {} if not only_id and not only_name else []

        for league in matches['league']:
            for i, event in enumerate(league['events']):
                if event[
                    'status'] == 'H':  # не принимает ставок = H, O = игра идет
                    continue
                if event['resultingUnit'] == 'Corners':
                    continue
                if only_id:
                    result.append(league['id'])
                elif only_name:
                    result.append(league['name'])
                else:
                    result[league['id']] = league['name']
        return result

    def get_events(self, matches, only_id=0, toStr=0):
        result = [] if only_id == 1 else {}
        sport_id = matches["sportId"]
        for league in matches['league']:
            league_id = int(league['id'])
            league_name = str(league['name'])
            for i, event in enumerate(league['events']):
                if event[
                    'status'] == 'H':  # не принимает ставок = H, O = игра идет
                    continue
                if event['resultingUnit'] == 'Corners':
                    continue

                event["start"] = datetime.strptime(event["starts"],
                                                   '%Y-%m-%dT%H:%M:%SZ')
                datediff = datetime.utcnow() - event["start"]
                event["from_start"] = datediff.total_seconds()

                # print(event["id"], league_name, event['status'])
                # print(datetime.utcnow(), event["start"], event["from_start"])

                if only_id == 1:
                    if toStr == 1:
                        event['id'] = str(event['id'])
                    result.append(event['id'])
                    continue
                event["sport_id"] = sport_id
                event["league_id"] = league_id
                event["league_name"] = league_name
                event["last"] = matches['last']
                fordelete = ['sameEventParlayPeriodsEnabled', 'version',
                             'rotNum']
                for el in fordelete:
                    if el in event:
                        del event[el]

                result[event['id']] = event
        return result, matches['last']


    def _us2dec_all_prices(self, prices):
        def apply(x):
            if "price" in x:
                x["price"] = float(convert_us_to_dec(int(x["price"])))
            return x

        prices = map(apply, prices)
        return prices

