import asyncio
import websockets
import json


async def connect_to_server():
    uri = "ws://localhost:8775"
    async with websockets.connect(uri, max_size=None) as websocket:
        print("Connected to the server.")
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                print_new_messages(data)
            except websockets.exceptions.ConnectionClosed:
                print("Connection to the server was closed.")
                break


def print_new_messages(data):
    print(f"Received data: {data}")


async def main():
    while True:
        try:
            await connect_to_server()
        except Exception as e:
            print(f"An error occurred: {e}")
        print("Attempting to reconnect in 5 seconds...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
