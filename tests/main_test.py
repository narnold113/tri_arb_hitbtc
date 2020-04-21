import json
import asyncio
import numpy as np
import logging
from orderbook_test import getWeightedPrice

logging.basicConfig(level=logging.INFO)
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
            'triangle_value': 0
        },
        'reverse': { ### Reverse arbitrage order: buy ALT/USD, sell ALT/BTC and sell BTC/USD. For sells, we consume the "bid" side of the orderbook ###
            'weighted_prices': {
                pair: 0
                for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
            },
            'triangle_value': 0,
            'amount_if_bought': 0
        }
    }
    for arb in ARBS
}
METHODS = [
    'snapshot',
    'update'
]

balance = 100

async def buildBook(res, pair):
    global updatebooks
    global btc_book
    global arbitrage_book
    arb = pair[:3]
    # logger.info('Building book for %s arb', arb)

    for side in SIDES:
        if res['method'] == 'snapshotOrderbook':
            if pair == 'BTCUSD':
                btc_book['orderbook'][side] = np.zeros((len(res['params'][side]), 2))
                for i, item in enumerate(res['params'][side]):
                    btc_book['orderbook'][side][i][0] = item['price']
                    btc_book['orderbook'][side][i][1] = item['size']
            else:
                arbitrage_book[pair[:3]]['orderbooks'][pair][side] = np.zeros((len(res['params'][side]), 2))
                for j, jtem in enumerate(res['params'][side]):
                    arbitrage_book[arb]['orderbooks'][pair][side][j][0] = jtem['price']
                    arbitrage_book[arb]['orderbooks'][pair][side][j][1] = jtem['size']
        else:
            updatebooks[pair][side] = np.zeros((len(res['params'][side]), 2))
            for i, item in enumerate(res['params'][side]):
                updatebooks[pair][side][i][0] = item['price']
                updatebooks[pair][side][i][1] = item['size']

    # for side in SIDES:
        if 'ask' in updatebooks[pair]:
            for ui, updateItem in enumerate(updatebooks[pair][side]):
                if pair == 'BTCUSD':
                    for bi, btcItem in enumerate(btc_book['orderbook'][side]):
                        if updateItem[0] == btcItem[0] and updateItem[1] != 0:
                            btc_book['orderbook'][side][bi][1] = updateItem[1]
                            break
                        elif updateItem[0] == btcItem[0] and updateItem[1] == 0:
                            btc_book['orderbook'][side] = np.delete(btc_book['orderbook'][side], bi, axis=0)
                            break
                        else:
                            pass
                else:
                    for oi, orderItem in enumerate(arbitrage_book[arb]['orderbooks'][pair][side]):
                        if updateItem[0] == orderItem[0] and updateItem[1] != 0:
                            arbitrage_book[arb]['orderbooks'][pair][side][oi][1] = updateItem[1]
                            break
                        elif updateItem[0] == orderItem[0] and updateItem[1] == 0:
                            arbitrage_book[arb]['orderbooks'][pair][side] = np.delete(arbitrage_book[arb]['orderbooks'][pair][side], oi, axis=0)
                            break
                        else:
                            pass
                if pair == 'BTCUSD':
                    if updateItem[0] not in btc_book['orderbook'][side] and updateItem[1] != 0:
                        btc_book['orderbook'][side] = np.append(btc_book['orderbook'][side], [updateItem], axis=0)
                    else:
                        pass
                else:
                    if updateItem[0] not in arbitrage_book[arb]['orderbooks'][pair][side] and updateItem[1] != 0:
                        arbitrage_book[arb]['orderbooks'][pair][side] = np.append(arbitrage_book[arb]['orderbooks'][pair][side], [updateItem], axis=0)
                    else:
                        pass


async def getBook(pair, method):
    path = 'data/' + pair + '_' + method + '.json'
    with open(path) as f:
        data = json.load(f)
    await buildBook(data, pair)

async def printBook():
    global arbitrage_book
    global btc_book
    # global updatebooks
    while 1:
        await asyncio.sleep(5)
        print(arbitrage_book, '\n')
        print(btc_book, '\n')
        # print(updatebooks)

async def fillArbitrageBook():
    global arbitrage_book
    global btc_book
    global balance

    while 1:
        await asyncio.sleep(5)
        btc_book['weighted_prices']['regular'] = getWeightedPrice(btc_book['orderbook']['ask'], balance, reverse=False)
        btc_book['weighted_prices']['reverse'] = getWeightedPrice(btc_book['orderbook']['bid'], balance, reverse=False)
        btc_book['amount_if_bought'] = balance / btc_book['weighted_prices']['regular']
        # print(btc_book)

        for arb in ARBS:
            for pair in arbitrage_book[arb]['regular']['weighted_prices']: ### Pairs in regular and reverse are the same ###
                if pair[-3:] != 'BTC':
                    arbitrage_book[arb]['regular']['weighted_prices'][pair] = getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['bid'], balance, reverse=False)
                    arbitrage_book[arb]['reverse']['weighted_prices'][pair] = getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['ask'], balance, reverse=False)
                    arbitrage_book[arb]['reverse']['amount_if_bought'] = balance / arbitrage_book[arb]['reverse']['weighted_prices'][pair]

                else:
                    arbitrage_book[arb]['regular']['weighted_prices'][pair] = getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['ask'], btc_book['amount_if_bought'], reverse=False)
                    arbitrage_book[arb]['reverse']['weighted_prices'][pair] = getWeightedPrice(arbitrage_book[arb]['orderbooks'][pair]['bid'], arbitrage_book[arb]['reverse']['amount_if_bought'], reverse=True)
            regular_arb_price = btc_book['weighted_prices']['regular'] * arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC']
            reverse_arb_price = arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'USD'] / arbitrage_book[arb]['reverse']['weighted_prices'][arb + 'BTC']
            arbitrage_book[arb]['regular']['triangle_value'] = (arbitrage_book[arb]['regular']['weighted_prices'][arb + 'USD'] - regular_arb_price) / regular_arb_price
            arbitrage_book[arb]['reverse']['triangle_value'] = (btc_book['weighted_prices']['reverse'] - reverse_arb_price) / reverse_arb_price
        print(btc_book['weighted_prices'])
        print('regular: ', arbitrage_book['ETH']['regular'], '\n', 'reverse: ', arbitrage_book['ETH']['reverse'])


async def main() -> None:
    coroutines = [
        loop.create_task(getBook(pair, method))
        for method in METHODS
            for pair in PAIRS
    ]
    # coroutines.append(printBook())
    coroutines.append(fillArbitrageBook())

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