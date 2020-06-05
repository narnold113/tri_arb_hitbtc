import asyncio
import websockets
import random

async def hello(websocket, path):
    while 1:
        await asyncio.sleep(2)
        pong_waiter = await websocket.ping()
        if pong_waiter is None:
            print('Pong has not yet been received')
        else:
            print('Pong has been received. Sending payload')
            print(pong_waiter)
            await websocket.send(str(random.randint(0, 100)))

start_server = websockets.serve(hello, "localhost", 8765, ping_interval=5, ping_timeout=5)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
