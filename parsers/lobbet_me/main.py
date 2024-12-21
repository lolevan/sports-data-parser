# main.py
import asyncio
import logging
import time
import json
import os
from datetime import datetime
import aiohttp
import websockets
from parsers.lobbet_me.live import LiveOddsParser
from parsers.lobbet_me.prematch import PreMatchOddsParser
from parsers.utils import save_odds_to_jsonl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S'
)

BOOKIE = 'lobbet_me'


class LobbetClient:
    def __init__(self):
        self.live_parser = LiveOddsParser()
        self.prematch_parser = PreMatchOddsParser()
        self.ALL_MATCHES = {}
        self.parsed_matches = {}
        self.connected_clients = set()
        self.lock = asyncio.Lock()

    async def get_live_odds(self):
        live_matches = await self.live_parser.get_live_odds()
        async with self.lock:
            self.ALL_MATCHES = self.live_parser.ALL_MATCHES
            self.parsed_matches = self.live_parser.parsed_matches

        logging.info(
            f"Total live matches: {len(self.live_parser.ALL_MATCHES)}")
        return self.parsed_matches

    async def get_prematch_odds(self):
        prematch_matches = await self.prematch_parser.get_prematch_odds()
        async with self.lock:
            self.ALL_MATCHES = self.prematch_parser.ALL_MATCHES
            self.parsed_matches = self.prematch_parser.parsed_matches

        logging.info(
            f"Total prematch matches: {len(self.prematch_parser.ALL_MATCHES)}")
        return self.parsed_matches

    async def update_live_odds_periodically(self, interval=1.5):
        while True:
            start_time = time.time()
            try:
                live_odds = await self.get_live_odds()
                logging.info(
                    f"Live odds updated. Total live matches: {len(self.live_parser.ALL_MATCHES)}")
                odds_data = await self.normalize_odds(live_odds,
                                                      match_type='Live')
                if self.connected_clients:
                    data_to_send = json.dumps(odds_data)
                    coros = [self.send_data_to_client(client, data_to_send)
                             for client in self.connected_clients]
                    await asyncio.gather(*coros, return_exceptions=True)
            except Exception as e:
                logging.error(f"Error updating live odds: {e}")
            execution_time = time.time() - start_time
            sleep_time = max(0, interval - execution_time)

            logging.info(
                f"Live update time: {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

    async def update_prematch_odds_periodically(self, interval=16):
        while True:
            start_time = time.time()
            try:
                prematch_odds = await self.get_prematch_odds()
                logging.info(
                    f"Pre-match odds updated. Total prematch matches: {len(self.prematch_parser.ALL_MATCHES)}")
                odds_data = await self.normalize_odds(prematch_odds,
                                                      match_type='PreMatch')
                if self.connected_clients:
                    data_to_send = json.dumps(odds_data)
                    coros = [self.send_data_to_client(client, data_to_send)
                             for client in self.connected_clients]
                    await asyncio.gather(*coros, return_exceptions=True)
            except Exception as e:
                logging.error(f"Error updating pre-match odds: {e}")
            execution_time = time.time() - start_time
            sleep_time = max(0, interval - execution_time)

            logging.info(
                f"Pre-match update time: {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

    async def normalize_odds(self, matches, match_type):
        odds_data = {}
        async with self.lock:
            for match_id, match in matches.items():
                normalized_match = self.process_match_data(match, match_type)
                if normalized_match:
                    odds_data[match_id] = normalized_match
        return odds_data

    def process_match_data(self, match, match_type):
        outcomes = match.get('outcomes', [])
        if not outcomes:
            return None

        result = {
            "event_id": match['match_id'],
            "match_name": match['name'],
            "start_time": datetime.fromisoformat(
                match['start_time']).timestamp(),
            "home_team": match['home_team'],
            "away_team": match['away_team'],
            "league_id": None,  # If available
            "league": match['league'],
            "country": match['country'],
            "sport": match['sport'],
            "type": match_type,
            "outcomes": outcomes,
            "time": match['time'],
            "bookmaker": BOOKIE,
        }

        log_dir = 'odds_data'
        os.makedirs(log_dir, exist_ok=True)
        filename = f"{match['home_team']} vs {match['away_team']}"


        save_odds_to_jsonl(filename, result)
        return result

    async def send_data_to_client(self, client, data):
        try:
            await client.send(data)
        except websockets.exceptions.ConnectionClosed:
            logging.info(f"Client disconnected: {client.remote_address}")
            self.connected_clients.remove(client)

    async def websocket_handler(self, websocket, path):
        self.connected_clients.add(websocket)
        logging.info(f"Client connected: {websocket.remote_address}")
        try:
            await websocket.wait_closed()
        finally:
            self.connected_clients.remove(websocket)
            logging.info(f"Client disconnected: {websocket.remote_address}")

    async def run(self):
        server = await websockets.serve(self.websocket_handler, '0.0.0.0',
                                        6007)
        logging.info("WebSocket server started on ws://0.0.0.0:6007")
        # live_update_task = asyncio.create_task(
        #     self.update_live_odds_periodically(interval=2))
        prematch_update_task = asyncio.create_task(
            self.update_prematch_odds_periodically(interval=16))
        await asyncio.gather(prematch_update_task)

    async def reparse_match(self, match_id: int, match_type: str):
        """
        Reparse a single match by its ID.
        """

        async with aiohttp.ClientSession() as session:

            if match_type == 'Live':
                updated_matches = await self.get_live_odds()
                updated_match = updated_matches.get(match_id)

            else:
                updated_match = await self.prematch_parser.get_match_details2(
                    match_id, session=session)

        if updated_match:
            async with self.lock:
                self.ALL_MATCHES[match_id] = updated_match
                self.parsed_matches[match_id] = updated_match
            logging.info(f"Successfully reparsed match ID {match_id}")
            return updated_match
        else:
            logging.warning(f"Failed to reparse match ID {match_id}")
            return None


if __name__ == "__main__":
    lobbet_client = LobbetClient()
    try:
        asyncio.run(lobbet_client.run())
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
