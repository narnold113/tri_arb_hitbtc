import websockets
import asyncio

async def pinger(websocket):


async def consumer():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri, ping_interval=None, ping_timeout=None) as websocket:
        while 1:
            res = await websocket.recv()
            print(res)
        while 1:
            pong_waiter = websocket.ping()
            await asyncio.sleep(5)
            if pong_waiter is None:
                print('Pong not received')
            else:
                print('pong received')


async def main():
    await asyncio.wait([consumer()])

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()
