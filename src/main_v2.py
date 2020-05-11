import json
import asyncio
import websockets
import numpy as np
import logging
import traceback as tb
import helper as helper
import mysql.connector
from datetime import datetime
from mysql.connector import Error
from mysql.connector import errorcode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

balances = [1_000, 10_000, 25_000, 50_000]

ARBS = [
    'ETH'
    ,'LTC'
    ,'XRP'
    ,'BCH'
    ,'EOS'
    ,'XMR'
    ,'ETC'
    ,'BSV'
    ,'ZRX'
]
SIDES = [
    'ask',
    'bid'
]
PAIRS = []
for arb in ARBS:
    if arb == 'XRP':
        PAIRS.append(arb + 'USDT')
    else:
        PAIRS.append(arb + 'USD')
    PAIRS.append(arb + 'BTC')
PAIRS.insert(0, 'BTCUSD')
# print(PAIRS)
updatebooks = {
    pair: {}
    for pair in PAIRS
}

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
# print(arbitrage_book)
async def dataDirector(res):
    res = json.loads(res)
    pair = res['params']['symbol']
    if res['method'] == 'snapshotOrderbook':
        await buildBook(res, pair)
    else:
        await updateBook(res, pair)
        # pass


async def buildBook(res, pair):
    global btc_book
    global arbitrage_book

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
    updatebook = {
        side: {}
        for side in SIDES
    }
    # print(pair)
    # print(res)
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

async def populateArbValues():
    global arbitrage_book
    global btc_book
    global balances

    await asyncio.sleep(15)
    while 1:
        try:

            await asyncio.sleep(1)
            for side in SIDES:
                if side in btc_book['orderbook']:
                    if side == 'ask':
                        btc_book['orderbook'][side] = btc_book['orderbook'][side][btc_book['orderbook'][side][:,0].argsort()]
                        # print(btc_book['orderbook'][side][:10])
                        btc_book['weighted_prices']['regular'] = helper.getWeightedPrice(btc_book['orderbook'][side], balances, reverse=False)
                    else:
                        btc_book['orderbook'][side] = btc_book['orderbook'][side][btc_book['orderbook'][side][:,0].argsort()[::-1]]
                        btc_book['weighted_prices']['reverse'] = helper.getWeightedPrice(btc_book['orderbook'][side], balances, reverse=False)
                    btc_book['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,btc_book['weighted_prices']['regular'])]
                else:
                    pass
            # print(btc_book['weighted_prices']['regular'])
            
            for arb in ARBS:
                for pair in sorted(arbitrage_book[arb]['regular']['weighted_prices'], reverse=True): ### Pairs in regular and reverse are the same ###
                    for side in SIDES:
                        if side == 'ask':
                            arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                            arb_ob = arb_ob[arb_ob[:,0].argsort()]
                            # print(arb_ob[:10])

                            if pair[-3:] == 'USD':
                                arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                                arbitrage_book[arb]['reverse']['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,arbitrage_book[arb]['reverse']['weighted_prices'][pair])]
                                print(pair, arbitrage_book[arb]['reverse']['amount_if_bought'])
                            else:
                                arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, btc_book['amount_if_bought'], reverse=False)

                        else:
                            arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                            arb_ob = arb_ob[arb_ob[:,0].argsort()[::-1]]

                            if pair[-3:] == 'USD':
                                arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                            else:
                                arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, arbitrage_book[arb]['reverse']['amount_if_bought'], reverse=True)
            # print(arbitrage_book['ETH']['reverse'])

            for arb in ARBS:
                regular_arb_price = [x * y for x,y in zip(btc_book['weighted_prices']['regular'], arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC'])]
                reverse_arb_price = [x / y for x,y in zip(arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'USD'], arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'BTC'])]
                arbitrage_book[arb]['regular']['triangle_values'] = [(altusd - rap) / rap for altusd,rap in zip(arbitrage_book[arb]['regular']['weighted_prices'][arb + 'USD'], regular_arb_price)]
                arbitrage_book[arb]['reverse']['triangle_values'] = [(btcusd - rap) / rap for btcusd,rap in zip(btc_book['weighted_prices']['reverse'],reverse_arb_price)]
            print([100 * x for x in arbitrage_book['ETH']['regular']['triangle_values']])
            print([100 * y for y in arbitrage_book['ETH']['reverse']['triangle_values']])
        except Exception:
            tb.print_exc()
            break

# async def sqlHandler():
#     global arbitrage_book
#     global balances
#     conn = None
#     try:


            
async def printBook():
    global arbitrage_book
    global btc_book
    while 1:
        await asyncio.sleep(3)
        # index_array = btc_book['orderbook']['ask'][:,0].argsort()
        # btc_book['orderbook']['ask'] = btc_book['orderbook']['ask'][btc_book['orderbook']['ask'][:,0].argsort()]
        # print(index_array)
        # print(btc_book['orderbook']['ask'][0:10],'\n')
        # print('ask shape:', btc_book['orderbook']['ask'].shape)
        # print('bid shape:', btc_book['orderbook']['bid'].shape)
        # print('First Ask Item:', btc_book['orderbook']['ask'][0], 'Last Ask Item:', btc_book['orderbook']['ask'][-1])
        # print('First Bid Item:', btc_book['orderbook']['bid'][0], 'Last Bid Item:', btc_book['orderbook']['bid'][-1])
        if 'ask' in btc_book['orderbook']:
            btc_book['orderbook']['ask'] = btc_book['orderbook']['ask'][btc_book['orderbook']['ask'][:,0].argsort()]
            print(btc_book['orderbook']['ask'][:5], '\n')
        else:
            pass


async def subscribeToBook(pair) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": 123}'''
    params = json.loads(strParams)
    params['params']['symbol'] = pair
    async with websockets.client.connect(url) as websocket:
        await websocket.send(str(params).replace('\'', '"'))
        while 1:
            res = await websocket.recv()
            if 'params' in res:
                await dataDirector(res)

async def main() -> None:
    coroutines = [
        loop.create_task(subscribeToBook(pair))
        for pair in PAIRS
    ]
    # coroutines.append(printBook())
    coroutines.append(populateArbValues())
    await asyncio.wait(coroutines)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        # loop.run_forever(main())
    except:
        pass
    finally:
        loop.close()