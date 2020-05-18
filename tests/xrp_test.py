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
    # 'ETH' # OK
    # ,'LTC' # OK
    'XRP' # Not OK
    # ,'BCH' # OK
    # ,'EOS' # OK
    # ,'XMR' # OK
    # ,'ETC' # OK
    # ,'BSV' # OK
    # ,'ZRX' # OK
]
PAIRS = []
for arb in ARBS:
    if arb == 'XRP':
        PAIRS.append(arb + 'USDT')
    else:
        PAIRS.append(arb + 'USD')
    PAIRS.append(arb + 'BTC')
PAIRS.insert(0, 'BTCUSD')

SIDES = [
    'ask',
    'bid'
]

update_list = []
build_list = []

btc_book = {
    'orderbook': {
        'ask': [],
        'bid': []
    },
    'weighted_prices': {
        'regular': 0,
        'reverse': 0
    },
    'amount_if_bought': 0
}

arbitrage_book = {
    arb: {
        'orderbooks': {
            pair: {}
            for pair in PAIRS if pair[:3] == arb
        },
        'regular': { ### Regular arbitrage order: buy BTC/USD, buy ALT/BTC and sell ALT/USD. For buys, we calculate weighted price on the "ask" side ###
            'weighted_prices': {
                pair: 0
                for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
            },
            'triangle_values': []
        },
        'reverse': { ### Reverse arbitrage order: sell BTC/USD, sell ALT/BTC and buy ALT/USD. For sells, we consume the "bid" side of the orderbook ###
            'weighted_prices': {
                pair: 0
                for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
            },
            'triangle_values': [],
            'amount_if_bought': 0
        }
    }
    for arb in ARBS
}

async def dataDirector(res):
    res = json.loads(res)
    if 'params' in res: # Filter initial status messages
        pair = res['params']['symbol']
        if res['method'] == 'snapshotOrderbook':
            await buildBook(res, pair)
        else:
            await updateBook(res, pair)
    else:
        pass


async def buildBook(res, pair):
    global btc_book
    global arbitrage_book
    global build_list

    build_list.append(pair)

    for side in SIDES:
        res_length = len(res['params'][side])
        desired_length = int(res_length * 0.9)
        if pair == 'BTCUSD':
            btc_book['orderbook'][side] = np.zeros((res_length, 2))
            for i, item in enumerate(res['params'][side]):
                btc_book['orderbook'][side][i] = np.array([item['price'], item['size']])
        else:
            # pass
            arb1 = pair[:3]
            arbitrage_book[arb1]['orderbooks'][pair][side] = np.zeros((res_length, 2))
            for j, jtem in enumerate(res['params'][side]):
                arbitrage_book[arb1]['orderbooks'][pair][side][j] = np.array([jtem['price'], jtem['size']])

async def updateBook(res, pair):
    global btc_book
    global arbitrage_book
    global update_list
    updatebook = {
        side: {}
        for side in SIDES
    }
    update_list.append(pair)
    # print(pair)
    # print(res)
    try:

        for side in SIDES:
            updatebook[side] = np.zeros((len(res['params'][side]), 2))
            for i, item in enumerate(res['params'][side]):
                updatebook[side][i] = np.array([item['price'], item['size']])

        for side in SIDES:
            uos = updatebook[side]
            if pair == 'BTCUSD':
                # pass
                btc_ob = btc_book['orderbook'][side]

                notin_ind = np.in1d(uos[:,0], btc_ob[:,0], invert=True)
                btc_ob = np.append(btc_ob, uos[notin_ind], axis=0)

                inter, orders_ind, updateorders_ind = np.intersect1d(btc_ob[:,0], uos[:,0], return_indices=True)
                btc_ob[orders_ind] = uos[updateorders_ind]

                delete_ind = np.where(btc_ob == 0)[0]
                btc_ob = np.delete(btc_ob, delete_ind, axis=0)

                btc_book['orderbook'][side] = btc_ob
            else:
                # pass
                arb = pair[:3]
                arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]

                notin_ind = np.in1d(uos[:,0], arb_ob[:,0], invert=True)
                arb_ob = np.append(arb_ob, uos[notin_ind], axis=0)

                inter, orders_ind, updateorders_ind = np.intersect1d(arb_ob[:,0], uos[:,0], return_indices=True)
                arb_ob[orders_ind] = uos[updateorders_ind]

                delete_ind = np.where(arb_ob == 0)[0]
                arb_ob = np.delete(arb_ob, delete_ind, axis=0)

                arbitrage_book[arb]['orderbooks'][pair][side] = arb_ob
    except Exception:
        tb.print_exc

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
                await printBook()
                break
            else:
                continue
        except:
            print('There was an error')
        else:
            continue

async def printBook():
    global arbitrage_book
    global btc_book
    await asyncio.sleep(10)
    while 1:
        await asyncio.sleep(0.25)
        arbitrage_book['XRP']['orderbooks']['XRPUSDT']['ask'] = arbitrage_book['XRP']['orderbooks']['XRPUSDT']['ask'][arbitrage_book['XRP']['orderbooks']['XRPUSDT']['ask'][:,0].argsort()]
        print(arbitrage_book['XRP']['orderbooks']['XRPUSDT']['ask'][0:5])
        # if 'ask' in btc_book['orderbook']:
        #     btc_book['orderbook']['ask'] = btc_book['orderbook']['ask'][btc_book['orderbook']['ask'][:,0].argsort()]
        #     print(btc_book['orderbook']['ask'][0], '\n')
        # else:
        #     pass

async def subscribeToBook(pair, i=-1) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": "placeholder"}'''
    params = json.loads(strParams)
    params['params']['symbol'] = pair
    params['id'] = random.randrange(1000)
    try:
        async with websockets.client.connect(url) as websocket:
            await websocket.send(str(params).replace('\'', '"'))
            while 1:
                res = await websocket.recv()
                await dataDirector(res)
    except websockets.exceptions.InvalidStatusCode as isc:
        i += 1
        if i == 0:
            print('Waiting 30 seconds to retry the connection for', pair)
            await asyncio.sleep(30)
            await subscribeToBook(pair, i=i)
        elif i in range(1,5):
            print('Waiting 60 seconds to retry the connection for', pair)
            await asyncio.sleep(60)
            await subscribeToBook(pair, i=i)
        else:
            print('Waiting 120 seconds to retry the connection for', pair)
            await asyncio.sleep(120)
            await subscribeToBook(pair, i=i)

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