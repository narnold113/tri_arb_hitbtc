import json
import asyncio
import websockets
import numpy as np
import logging
import traceback as tb
import random
import time
from datetime import datetime


ARBS = [
    'ETH' # OK
    ,'LTC' # OK
    # 'XRP' # Not OK
    ,'BCH' # OK
    ,'EOS' # OK
    ,'XMR' # OK
    ,'ETC' # OK
    ,'BSV' # OK
    ,'ZRX' # OK
]
PAIRS = []
for arb in ARBS:
    if arb == 'XRP':
        PAIRS.append(arb + 'USDT')
    else:
        PAIRS.append(arb + 'USD')
    PAIRS.append(arb + 'BTC')
PAIRS.insert(0, 'BTCUSD')

PAIR = ['BTCUSD']

update_list = []
build_list = []

async def dataDirector(res):
    res = json.loads(res)
    if 'params' in res: # Filter initial status messages
        pair = res['params']['symbol']
        if res['method'] == 'snapshotOrderbook':
            build_list.append(pair)
        else:
            update_list.append(pair)
    else:
        pass

async def fullBookTimer():
    global update_list
    global build_list
    start_time = datetime.now()

    while 1:
        await asyncio.sleep(1)
        print(build_list)
        try:
            check = all(item in build_list for item in PAIRS)
            if check:
                print(datetime.now() - start_time)
                print('Build list contains all items in PAIRS')
                break
            else:
                continue
        except:
            print('There was an error')
        else:
            continue

async def subscribeToBook(pair, i=-1) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": "placeholder"}'''
    params = json.loads(strParams)
    params['params']['symbol'] = pair
    params['id'] = random.randrange(1000)
    try:
        async with websockets.client.connect(url) as websocket:
            # await websocket.send(str(params).replace('\'', '"'))
            while 1:
                res = await websocket.recv()
                await dataDirector(res)
    except websockets.exceptions.InvalidStatusCode as isc:
        print(isc)
        # i += 1
        # if i == 0:
        #     print('Waiting 30 seconds to retry the connection for', pair)
        #     await asyncio.sleep(30)
        #     await subscribeToBook(pair, i=i)
        # elif i in range(1,5):
        #     print('Waiting 60 seconds to retry the connection for', pair)
        #     await asyncio.sleep(60)
        #     await subscribeToBook(pair, i=i)
        # else:
        #     print('Waiting 120 seconds to retry the connection for', pair)
        #     await asyncio.sleep(120)
        #     await subscribeToBook(pair, i=i)

async def main() -> None:
    coroutines = [
        loop.create_task(subscribeToBook(pair))
        for pair in PAIRS
    ]
    coroutines.append(fullBookTimer())
    await asyncio.wait(coroutines)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()