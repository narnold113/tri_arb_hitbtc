import json
import asyncio
import websockets
import numpy as np
import logging
import traceback as tb
import helper
import mysql.connector
import random
import time
from datetime import datetime
from mysql.connector import Error
from mysql.connector import errorcode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

balances = [1_000, 10_000, 25_000, 50_000]

ARBS = [
    'ETH' # OK
    ,'LTC' # OK
    ,'XRP' # Not OK
    ,'BCH' # OK
    ,'EOS' # OK
    ,'XMR' # OK
    ,'ETC' # OK
    ,'BSV' # OK
    ,'ZRX' # OK
    ,'TRX'
]
SIDES = [
    'ask',
    'bid'
]
PAIRS = []
for arb in ARBS:
    PAIRS.append(arb + 'USD')
    PAIRS.append(arb + 'BTC')
PAIRS.insert(0, 'BTCUSD')
# print(PAIRS)

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
async def dataDirector(res, pair):
    res = json.loads(res)
    if 'params' in res: # Filter initial status messages
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
                btc_book['orderbook'][side][i] = np.array([item['price'], item['size']], np.float64)
        else:
            arb1 = pair[:3]
            arbitrage_book[arb1]['orderbooks'][pair][side] = np.zeros((res_length, 2))
            for j, jtem in enumerate(res['params'][side]):
                arbitrage_book[arb1]['orderbooks'][pair][side][j] = np.array([jtem['price'], jtem['size']], np.float64)

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
                updatebook[side][i] = np.array([item['price'], item['size']], np.float64)

        for side in SIDES:
            uos = updatebook[side]
            if pair == 'BTCUSD':
                btc_ob = btc_book['orderbook'][side]

                notin_ind = np.in1d(uos[:,0], btc_ob[:,0], invert=True)
                btc_ob = np.append(btc_ob, uos[notin_ind], axis=0)

                inter, orders_ind, updateorders_ind = np.intersect1d(btc_ob[:,0], uos[:,0], return_indices=True)
                btc_ob[orders_ind] = uos[updateorders_ind]

                delete_ind = np.where(btc_ob == 0)[0]
                btc_ob = np.delete(btc_ob, delete_ind, axis=0)

                btc_book['orderbook'][side] = btc_ob
            else:
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

async def populateArbValues():
    global arbitrage_book
    global btc_book
    global balances

    try:
        conn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='tri_arb_hitbtc')
        if conn.is_connected():
            print('Connection to Mariadb initiated in populateArbValues function')
        while 1:
            try:
                await asyncio.sleep(3)
                for side in SIDES:
                    if side in btc_book['orderbook']:
                        if side == 'ask':
                            btc_book['orderbook'][side] = btc_book['orderbook'][side][btc_book['orderbook'][side][:,0].argsort()]
                            btc_book['weighted_prices']['regular'] = helper.getWeightedPrice(btc_book['orderbook'][side], balances, reverse=False)
                        else:
                            btc_book['orderbook'][side] = btc_book['orderbook'][side][btc_book['orderbook'][side][:,0].argsort()[::-1]]
                            btc_book['weighted_prices']['reverse'] = helper.getWeightedPrice(btc_book['orderbook'][side], balances, reverse=False)
                        btc_book['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,btc_book['weighted_prices']['regular'])]
                    else:
                        pass
                
                for arb in ARBS:
                    for pair in sorted(arbitrage_book[arb]['regular']['weighted_prices'], reverse=True): ### Pairs in regular and reverse are the same ###
                        for side in SIDES:
                            if side == 'ask':
                                arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                                arb_ob = arb_ob[arb_ob[:,0].argsort()]

                                if pair[-3:] == 'USD' or pair[-3:] == 'USDT':
                                    arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                                    arbitrage_book[arb]['reverse']['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,arbitrage_book[arb]['reverse']['weighted_prices'][pair])]
                                else:
                                    arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, btc_book['amount_if_bought'], reverse=False)

                            else:
                                arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                                arb_ob = arb_ob[arb_ob[:,0].argsort()[::-1]]

                                if pair[-3:] == 'USD' or pair[-3:] == 'USDT':
                                    arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                                else:
                                    arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, arbitrage_book[arb]['reverse']['amount_if_bought'], reverse=True)
                    regular_arb_price = [x * y for x,y in zip(btc_book['weighted_prices']['regular'], arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC'])]
                    reverse_arb_price = [x / y for x,y in zip(arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'USD'], arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'BTC'])]
                    arbitrage_book[arb]['regular']['triangle_values'] = [(altusd - rap) / rap for altusd,rap in zip(arbitrage_book[arb]['regular']['weighted_prices'][arb + 'USD'], regular_arb_price)]
                    arbitrage_book[arb]['reverse']['triangle_values'] = [(btcusd - rap) / rap for btcusd,rap in zip(btc_book['weighted_prices']['reverse'],reverse_arb_price)]
                    # print(arb, [100 * x for x in arbitrage_book[arb]['regular']['triangle_values']])
                    # print(arb, [100 * y for y in arbitrage_book[arb]['reverse']['triangle_values']])
                    try:
                        arb_data = arbitrage_book[arb]['regular']['triangle_values']
                        for i, item in enumerate(arb_data):
                            arb_data[i] = item.item()

                        i = 1
                        for item in arbitrage_book[arb]['reverse']['triangle_values']:
                            arb_data.insert(i, item.item())
                            i+=2

                        arb_data.insert(0, datetime.now())
                        arb_data = tuple(arb_data)
                        add_arb = str(
                            "INSERT INTO {} "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(arb)
                        )
                        try:
                            cursor = conn.cursor()
                            cursor.execute(add_arb, arb_data)
                            conn.commit()
                            cursor.close()
                        except Error as err:
                            print('In the cursor.execute try')
                            print(err.msg)
                    except Exception:
                        tb.print_exc()
            except Exception:
                tb.print_exc()
                break
    except Error as err:
        print(err.msg)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            print('Disconnected from MariaDB')


async def createSqlTables():
    conn = None
    try:
        conn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='tri_arb_hitbtc')
        if conn.is_connected():
            print('Connected to Mariadb')
        cursor = conn.cursor()

        for arb in ARBS:
            table_creation = str(
                "CREATE TABLE {} ("
                "timestamp DATETIME(2) NOT NULL,"
                "regular1 DECIMAL(8,7) NULL,"
                "reverse1 DECIMAL(8,7) NULL,"
                "regular10 DECIMAL(8,7) NULL,"
                "reverse10 DECIMAL(8,7) NULL,"
                "regular25 DECIMAL(8,7) NULL,"
                "reverse25 DECIMAL(8,7) NULL,"
                "regular50 DECIMAL(8,7) NULL,"
                "reverse50 DECIMAL(8,7) NULL"
                ") ENGINE=InnoDB".format(arb)
            )
            try:
                cursor.execute(table_creation)
                print('Successfully created table for ', arb)
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
    except Error as e:
        print(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            print('Disconnected from MariaDB')
  
async def printBook():
    global arbitrage_book
    global btc_book
    await asyncio.sleep(10)
    while 1:
        await asyncio.sleep(0.25)
        if 'ask' in btc_book['orderbook']:
            btc_book['orderbook']['ask'] = btc_book['orderbook']['ask'][btc_book['orderbook']['ask'][:,0].argsort()]
            print(btc_book['orderbook']['ask'][0], '\n')
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
                print('Build list contains all items in PAIRS. It took', datetime.now() - start_time, 'seconds')
                print('Starting the Populate function...')
                await populateArbValues()
                break
            else:
                continue
        except Exception:
            tb.print_exc()
        else:
            continue

async def subscribeToBook(pair, i=-1) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": "placeholder"}'''
    params = json.loads(strParams)
    if pair == 'XRPUSD':
        params['params']['symbol'] = 'XRPUSDT'
    else:
        params['params']['symbol'] = pair
    params['id'] = random.randrange(1000)
    try:
        async with websockets.client.connect(url) as websocket:
            await websocket.send(str(params).replace('\'', '"'))
            while 1:
                res = await websocket.recv()
                await dataDirector(res, pair)
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
    except Exception:
        tb.print_exc

async def main() -> None:
    coroutines = [
        loop.create_task(subscribeToBook(pair))
        for pair in PAIRS
    ]
    coroutines.append(fullBookTimer())
    # coroutines = []
    coroutines.append(createSqlTables())
    # coroutines.append(sqlHandler())
    await asyncio.wait(coroutines)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()