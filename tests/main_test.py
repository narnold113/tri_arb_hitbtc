import json
import asyncio
import numpy as np
import logging
import helper_test as helper
import mysql.connector
from datetime import datetime
from mysql.connector import Error
from mysql.connector import errorcode

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SIDES = [
    'ask',
    'bid'
]

ARBS = [
    'ETH'
]

PAIRS = []
for arb in ARBS:
    PAIRS.append(arb + 'BTC')
    PAIRS.append(arb + 'USD')
PAIRS.sort(reverse=True)
PAIRS.insert(0, 'BTCUSD')

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
        'regular': [],
        'reverse': []
    },
    'amount_if_bought': []
}

arbitrage_book = {
    arb: {
        'orderbooks': {
            pair: {}
            for pair in PAIRS if pair[:3] == arb
        },
        'regular': { ### Regular arbitrage order: buy BTC/USD, buy ALT/BTC and sell ALT/USD. For buys, we calculate weighted price on the "ask" side ###
            'weighted_prices': {
                pair: []
                for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
            },
            'triangle_values': []
        },
        'reverse': { ### Reverse arbitrage order: buy ALT/USD, sell ALT/BTC and sell BTC/USD. For sells, we consume the "bid" side of the orderbook ###
            'weighted_prices': {
                pair: []
                for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
            },
            'triangle_values': [],
            'amount_if_bought': []
        }
    }
    for arb in ARBS
}
METHODS = [
    'snapshot',
    'update'
]

balances = [10, 11, 12]

async def dataDirector(res):
    # res = json.loads(res)
    pair = res['params']['symbol']
    # print(pair)
    if res['method'] == 'snapshotOrderbook':
        await buildBook(res, pair)
        # pass
    else:
        await updateBook(res, pair)
        # pass

async def buildBook(res, pair):
    global btc_book
    global arbitrage_book
    arb = pair[:3]
    logger.debug('Building %s book for %s arb',pair, arb)

    for side in SIDES:
        if res['method'] == 'snapshotOrderbook':
            if pair == 'BTCUSD':
                btc_book['orderbook'][side] = np.zeros((len(res['params'][side]), 2))
                for i, item in enumerate(res['params'][side]):
                    btc_book['orderbook'][side][i] = [item['price'], item['size']]
            else:
                arbitrage_book[pair[:3]]['orderbooks'][pair][side] = np.zeros((len(res['params'][side]), 2))
                for j, jtem in enumerate(res['params'][side]):
                    arbitrage_book[arb]['orderbooks'][pair][side][j] = [jtem['price'], jtem['size']]
        else:
            continue
            

async def updateBook(res, pair):
    global btc_book
    global arbitrage_book
    global updatebooks

    for side in SIDES:
        updatebooks[pair][side] = np.zeros((len(res['params'][side]), 2))
        for i, item in enumerate(res['params'][side]):
            updatebooks[pair][side][i] = [item['price'], item['size']]

    # print(updatebooks)
    for side in SIDES:
        update_orders = updatebooks[pair][side]
        if pair =='BTCUSD':
            btc_orderbook = btc_book['orderbook'][side]

            notin_ind = np.in1d(update_orders[:,0], btc_orderbook[:,0], invert=True)
            btc_orderbook = np.append(btc_orderbook, update_orders[notin_ind], axis=0)

            inter, orders_ind, updateorders_ind = np.intersect1d(btc_orderbook[:,0], update_orders[:,0], return_indices=True)
            btc_orderbook[orders_ind] = update_orders[updateorders_ind]

            delete_ind = np.where(btc_orderbook == 0)[0]
            btc_orderbook = np.delete(btc_orderbook, delete_ind, axis=0)

            btc_book['orderbook'][side] = btc_orderbook
        else:
            arb = pair[:3]
            arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]

            notin_ind = np.in1d(update_orders[:,0], arb_ob[:,0], invert=True)
            arb_ob = np.append(arb_ob, update_orders[notin_ind], axis=0)

            inter, orders_ind, updateorders_ind = np.intersect1d(arb_ob[:,0], update_orders[:,0], return_indices=True)
            arb_ob[orders_ind] = update_orders[updateorders_ind]

            delete_ind = np.where(arb_ob == 0)[0]
            arb_ob = np.delete(arb_ob, delete_ind, axis=0)

            arbitrage_book[arb]['orderbooks'][pair][side] = arb_ob

            # pass






            # for ui, updateItem in enumerate(updatebooks[pair][side]):
            #     if pair == 'BTCUSD':
            #         for bi, btcItem in enumerate(btc_book['orderbook'][side]):
            #             if updateItem[0] == btcItem[0] and updateItem[1] != 0:
            #                 btc_book['orderbook'][side][bi][1] = updateItem[1]
            #                 break
            #             elif updateItem[0] == btcItem[0] and updateItem[1] == 0:
            #                 btc_book['orderbook'][side] = np.delete(btc_book['orderbook'][side], bi, axis=0)
            #                 break
            #             else:
            #                 pass
            #     else:
            #         for oi, orderItem in enumerate(arbitrage_book[arb]['orderbooks'][pair][side]):
            #             if updateItem[0] == orderItem[0] and updateItem[1] != 0:
            #                 arbitrage_book[arb]['orderbooks'][pair][side][oi][1] = updateItem[1]
            #                 break
            #             elif updateItem[0] == orderItem[0] and updateItem[1] == 0:
            #                 arbitrage_book[arb]['orderbooks'][pair][side] = np.delete(arbitrage_book[arb]['orderbooks'][pair][side], oi, axis=0)
            #                 break
            #             else:
            #                 pass
            #     if pair == 'BTCUSD':
            #         if updateItem[0] not in btc_book['orderbook'][side] and updateItem[1] != 0:
            #             btc_book['orderbook'][side] = np.append(btc_book['orderbook'][side], [updateItem], axis=0)
            #         else:
            #             continue
            #     else:
            #         if updateItem[0] not in arbitrage_book[arb]['orderbooks'][pair][side] and updateItem[1] != 0:
            #             arbitrage_book[arb]['orderbooks'][pair][side] = np.append(arbitrage_book[arb]['orderbooks'][pair][side], [updateItem], axis=0)
            #         else:
            #             continue


async def getBook(pair, method):
    path = 'data/' + pair + '_' + method + '.json'
    # print(path)
    with open(path) as f:
        data = json.load(f)
    # print(data)
    await dataDirector(data)

async def printBook():
    global arbitrage_book
    global btc_book
    # global updatebooks
    while 1:
        await asyncio.sleep(5)
        print(arbitrage_book, '\n')
        # print(btc_book, '\n')
        # print(updatebooks)

async def fillArbitrageBook():
    global arbitrage_book
    global btc_book
    global balances

    await asyncio.sleep(5)
    while 1:
        await asyncio.sleep(1)
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
        # print(btc_book['weighted_prices'])
        
        for arb in ARBS:
            for pair in sorted(arbitrage_book[arb]['regular']['weighted_prices'], reverse=True): ### Pairs in regular and reverse are the same ###
                for side in SIDES:
                    if side == 'ask':
                        arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                        arb_ob = arb_ob[arb_ob[:,0].argsort()]

                        if pair[-3:] == 'USD':
                            arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                            arbitrage_book[arb]['reverse']['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,arbitrage_book[arb]['reverse']['weighted_prices'][pair])]
                        else:
                            arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, btc_book['amount_if_bought'], reverse=False)

                    else:
                        arb_ob = arbitrage_book[arb]['orderbooks'][pair][side]
                        arb_ob = arb_ob[arb_ob[:,0].argsort()[::-1]]

                        if pair[-3:] == 'USD':
                            arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, balances, reverse=False)
                        else:
                            arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arb_ob, arbitrage_book[arb]['reverse']['amount_if_bought'], reverse=True)
        # print(arbitrage_book['ETH']['regular']['weighted_prices'])














        # btc_book['weighted_prices']['regular'] = helper.getWeightedPrice(btc_book['orderbook']['ask'], balances, reverse=False)
        # btc_book['weighted_prices']['reverse'] = helper.getWeightedPrice(btc_book['orderbook']['bid'], balances, reverse=False)
        # btc_book['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,btc_book['weighted_prices']['regular'])]

        # for arb in ARBS:
        #     for pair in arbitrage_book[arb]['regular']['weighted_prices']: ### Pairs in regular and reverse are the same ###
        #         if pair[-3:] != 'BTC':
        #             arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['bid'], balances, reverse=False)
        #             arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['ask'], balances, reverse=False)
        #             arbitrage_book[arb]['reverse']['amount_if_bought'] = [bal / wp for bal,wp in zip(balances,arbitrage_book[arb]['reverse']['weighted_prices'][pair])]
        #         else:
        #             arbitrage_book[arb]['regular']['weighted_prices'][pair] = helper.getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['ask'], btc_book['amount_if_bought'], reverse=False)
        #             arbitrage_book[arb]['reverse']['weighted_prices'][pair] = helper.getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['bid'], arbitrage_book[arb]['reverse']['amount_if_bought'], reverse=True)




        for arb in ARBS:
            regular_arb_price = [x * y for x,y in zip(btc_book['weighted_prices']['regular'], arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC'])]
            reverse_arb_price = [x / y for x,y in zip(arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'USD'], arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'BTC'])]
            arbitrage_book[arb]['regular']['triangle_values'] = [(altusd - rap) / rap for altusd,rap in zip(arbitrage_book[arb]['regular']['weighted_prices'][arb + 'USD'], regular_arb_price)]
            arbitrage_book[arb]['reverse']['triangle_values'] = [(btcusd - rap) / rap for btcusd,rap in zip(btc_book['weighted_prices']['reverse'],reverse_arb_price)]
        # print(btc_book['weighted_prices'])
        print(arbitrage_book['ETH']['regular'])
        print(arbitrage_book['ETH']['reverse'], '\n')
        # print('regular: ', arbitrage_book['ETH']['regular']['weighted_prices'], '\n', 'reverse: ', arbitrage_book['ETH']['reverse']['weighted_prices'], '\n\n')
        # await insertTriArbValues()

async def createSqlTables():
    global arbitrage_book
    try:
        conn = mysql.connector.connect(**helper.read_db_config())
        cursor = conn.cursor()
        if conn.is_connected():
            logger.info('Connected to MariaDB in createSqlTables Function')
        
        for arb in ARBS:
            query = str(
                "CREATE TABLE {} ("
                "timestamp DATETIME(3) NOT NULL,"
                "regular1 DECIMAL(11,10) NULL,"
                "reverse1 DECIMAL(11,10) NULL,"
                "regular10 DECIMAL(11,10) NULL,"
                "reverse10 DECIMAL(11,10) NULL,"
                "regular25 DECIMAL(11,10) NULL,"
                "reverse25 DECIMAL(11,10) NULL,"
                "regular50 DECIMAL(11,10) NULL,"
                "reverse50 DECIMAL(11,10) NULL"
                ") ENGINE=InnoDB".format(arb)
            )
            try:
                cursor.execute(query)
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logger.error('Table already exists for %s', arb)
                else:
                    logger.error(err.msg)
            else:
                logger.info('Succesfully created table for %s', arb)
    except Error as e:
        logger.error(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            logger.info('Disconnected from MariaDB in createSqlTables Function')

async def insertTriArbValues():
    global arbitrage_book

    try:
        conn = mysql.connector.connect(**helper.read_db_config())
        cursor = conn.cursor()
        if conn.is_connected():
            logger.info('Connected to MariaDB in insertTriArbValues Function')
        
        # while 1:
        await asyncio.sleep(2)
        time = datetime.now()

        for arb in ARBS:
            data = (arb, time, 0.02, -0.02, .015, -0.015, 0.01, -0.01, 0.005, -0.005)
            query = str(
                "INSERT INTO {0} "
                "(timestamp, regular1, reverse1, regular10, reverse10, regular25, reverse25, regular50, reverse50) "
                "VALUES ('{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})".format(*data)
            )
            logger.debug(query)
            try:
                cursor.execute(query)
            except Error as err:
                logger.error(err)
            else:
                logger.info('Values inserted into {} table'.format(arb))
    except Error as e:
        logger.error(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.commit()
            cursor.close()
            conn.close()
            logger.info('Disconnected from MariaDB in insertTriArbValues Function')


async def main() -> None:
    coroutines = [
        loop.create_task(getBook(pair, method))
        for method in METHODS
            for pair in PAIRS
    ]
    # print(coroutines)
    # print(PAIRS)
    # coroutines.append(printBook())
    # coroutines.append(createSqlTables())
    coroutines.append(fillArbitrageBook())
    # coroutines.append(insertTriArbValues())
    
    logger.info('Running main function')
    await asyncio.wait(coroutines)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()