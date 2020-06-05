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
import sys
from datetime import datetime
from statistics import mean as fmean
from mysql.connector import Error
from mysql.connector import errorcode

logger = logging.getLogger('tri_arb_hitbtc')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logHandler = logging.FileHandler('tri_arb_hitbtc.log', mode='a')
logHandler = logging.StreamHandler()
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


balances = [1_000, 10_000, 25_000, 50_000]
balances = [1_000]


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
threshold_keep = {
    arb: {
        'regular': [],
        'reverse': []
    }
    for arb in ARBS
}

async def streamDirector(res, pair):
    res = json.loads(res)
    if 'params' in res: # Filter initial status messages
        if res['method'] == 'snapshotOrderbook':
            await buildBook(res, pair)
        else:
            await updateBook(res, pair)
    else:
        pass

### These two functions handle the orderbooks ###
build_list = []
async def buildBook(res, pair): ### Build the orderbooks using the snapshot data sent by the ws ###
    global btc_book
    global arbitrage_book
    global build_list

    build_list.append(pair)
    for side in SIDES:
        try:
            res_length = len(res['params'][side])
            desired_length = int(res_length * 0.25)
            if pair == 'BTCUSD':
                btc_book['orderbook'][side] = np.zeros((desired_length, 2))
                for i, item in enumerate(res['params'][side][:desired_length]):
                    btc_book['orderbook'][side][i] = np.array([item['price'], item['size']], np.float64)
            else:
                arb1 = pair[:3]
                arbitrage_book[arb1]['orderbooks'][pair][side] = np.zeros((desired_length, 2))
                for j, jtem in enumerate(res['params'][side][:desired_length]):
                    arbitrage_book[arb1]['orderbooks'][pair][side][j] = np.array([jtem['price'], jtem['size']], np.float64)
        except Exception as err:
            logger.exception(err)
            sys.exit()

async def updateBook(res, pair):
    global btc_book
    global arbitrage_book
    updatebook = {
        side: {}
        for side in SIDES
    }
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
        logger.exception()

async def populateArbValues():
    global arbitrage_book
    global btc_book
    global balances
    global threshold_keep
    threshold_values = {
        arb: {
            type: list()
            for type in ['regular', 'reverse']
        }
        for arb in ARBS
    }
    threshold_dict = {
        arb: {
            type: dict()
            for type in ['regular', 'reverse']
        }
        for arb in ARBS
    }
    while 1:
        try:
            await asyncio.sleep(.005)
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
                regular_arb_price = np.multiply(btc_book['weighted_prices']['regular'], arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC'])
                reverse_arb_price = np.divide(arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'USD'], arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'BTC'])
                arbitrage_book[arb]['regular']['triangle_values'] = np.divide(np.subtract(arbitrage_book[arb]['regular']['weighted_prices'][arb + 'USD'], regular_arb_price), regular_arb_price)
                arbitrage_book[arb]['reverse']['triangle_values'] = np.divide(np.subtract(btc_book['weighted_prices']['reverse'], reverse_arb_price), reverse_arb_price)

                for type in ['regular', 'reverse']:
                    if arbitrage_book[arb][type]['triangle_values'][0] >= 0:
                        if 'timestamp' not in threshold_dict[arb][type].keys():
                            threshold_dict[arb][type]['timestamp'] = float(time.time())
                            threshold_values[arb][type].append(arbitrage_book[arb][type]['triangle_values'][0])
                        else:
                            threshold_values[arb][type].append(arbitrage_book[arb][type]['triangle_values'][0])
                    else:
                        if 'timestamp' not in threshold_dict[arb][type].keys():
                            continue
                        else:
                            threshold_dict[arb][type]['duration'] = float(time.time() - threshold_dict[arb][type]['timestamp'])
                            threshold_dict[arb][type]['low'] =  float(min(threshold_values[arb][type]))
                            threshold_dict[arb][type]['high'] = float(max(threshold_values[arb][type]))
                            threshold_dict[arb][type]['mean'] = float(fmean(threshold_values[arb][type]))

                            threshold_keep[arb][type].append(threshold_dict[arb][type])
                            threshold_values[arb][type] = list()
                            threshold_dict[arb][type] = dict()
        except Exception as err:
            # tb.print_exc()
            logger.exception(err)
            sys.exit()


async def createSqlTables():
    conn = None
    try:
        conn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='tri_arb_hitbtc')
        cursor = conn.cursor()

        for arb in ARBS:
            table_creation = str(
                "CREATE TABLE {} ("
                "timestamp DECIMAL(16,6) NOT NULL,"
                "duration DECIMAL(9,6) NULL,"
                "low DECIMAL(8,7) NULL,"
                "high DECIMAL(8,7) NULL,"
                "mean DECIMAL(8,7) NULL,"
                "type VARCHAR(50) NULL"
                ") ENGINE=InnoDB".format(arb)
            )
            try:
                cursor.execute(table_creation)
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    log_msg = arb + ' table already exists'
                    logger.info(log_msg)
                else:
                    logger.exception(err.msg)
    except Error as e:
        logger.exception(e.msg)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()

async def arbMonitor():
    global arbitrage_book
    global threshold_keep

    while 1:
        await asyncio.sleep(2)
        for arb in ARBS:
            insert_statement = str(
                "INSERT INTO {} "
                "VALUES (%s, %s, %s, %s, %s, %s)".format(arb)
            )
            for type in ['regular', 'reverse']:
                print(arb, type, len(threshold_keep[arb][type]))
                if len(threshold_keep[arb][type]) >= 3:
                    try:
                        conn = mysql.connector.connect(user='python', password='python', host='127.0.0.1', database='tri_arb_hitbtc')
                        log_msg = arb + ' for type ' + type + ' has over 3 length. Inserting records now.'
                        logger.info(log_msg)
                        for dct in threshold_keep[arb][type]:
                            insert_values = list(dct.values())
                            insert_values.append(type)
                            cursor = conn.cursor()
                            cursor.execute(insert_statement, tuple(insert_values))
                            conn.commit()
                            cursor.close()
                        logger.info('Inserted records successfully')
                    except Exception as err:
                        logger.exception(err)
                        conn.close()
                        sys.exit()
                    finally:
                        threshold_keep[arb][type][:] = list()
                        if conn is not None and conn.is_connected():
                            conn.close()
                else:
                    continue

async def fullBookTimer(): ### This function checks for initiated orderbooks. Once all have been initiated, start populateArbValues ###
    global build_list

    while 1:
        await asyncio.sleep(1)
        try:
            check = all(item in build_list for item in PAIRS)
            if check:
                await asyncio.wait([populateArbValues(), arbMonitor()])
                break
            else:
                continue
        except Exception:
            tb.print_exc()
        else:
            continue

async def printBook():
    global arbitrage_book
    global btc_book
    await asyncio.sleep(5)
    while 1:
        await asyncio.sleep(0.25)
        print(len(btc_book['orderbook']['ask']))
        # if 'ask' in btc_book['orderbook']:
        #     btc_book['orderbook']['ask'] = btc_book['orderbook']['ask'][btc_book['orderbook']['ask'][:,0].argsort()]
        #     print(btc_book['orderbook']['ask'][0], '\n')
        # else:
        #     pass

async def subscribeToBook(pair) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": "placeholder"}'''
    params = json.loads(strParams)
    if pair == 'XRPUSD':
        params['params']['symbol'] = 'XRPUSDT'
    else:
        params['params']['symbol'] = pair
    params['id'] = random.randrange(1000)
    try:
        async with websockets.client.connect(url, ping_interval=None, ping_timeout=None, max_queue=None) as websocket:
            await websocket.send(str(params).replace('\'', '"'))
            logger.info('Successfully connected to ws for %s', pair)
            while 1:
                try:
                    res = await websocket.recv()
                    await streamDirector(res, pair)
                except websockets.exceptions.ConnectionClosed as cc:
                    logger.exception(cc)
                    sys.exit()
    except websockets.exceptions.InvalidStatusCode as isc: ### Recursion. If the ws server receives too many requests, it throws a rate limit error ###
        logger.info('Waiting 90 seconds to retry the connection for %s', pair)
        await asyncio.sleep(90)
        await subscribeToBook(pair)
    except Exception:
        tb.print_exc


async def main() -> None:
    coroutines = [
        loop.create_task(subscribeToBook(pair))
        for pair in PAIRS
    ]
    coroutines.append(fullBookTimer())
    # coroutines.append(printBook())
    coroutines.append(createSqlTables())
    await asyncio.wait(coroutines)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()
