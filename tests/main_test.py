import json
import asyncio
import numpy as np

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
PAIRS.insert(0, 'BTCUSD')

orderbooks = {
    pair: {}
    for pair in PAIRS
}

updatebooks = {
    pair: {}
    for pair in PAIRS
}

arbitrageBook = {
    arb: {
        'forward': {
            'weightedPrices': {
                pair: {} 
                for pair in PAIRS if pair[:3] == arb or pair == 'BTCUSD'
            },
            'value': 0
        },
        'reverse': {
            'weightedPrices': {
                pair: {} 
                for pair in PAIRS if pair[:3] == arb or pair == 'BTCUSD'
            },
            'value': 0
        }
    }
    for arb in ARBS
}
METHODS = [
    'snapshot',
    'update'
]

async def buildBook(res, pair):
    global orderbooks
    global updatebooks
    # print(res)
    
    for side in SIDES:
        if res['method'] == 'snapshotOrderbook':
            orderbooks[pair][side] = np.zeros((len(res['params'][side]), 2))
            for i, item in enumerate(res['params'][side]):
                orderbooks[pair][side][i][0] = item['price']
                orderbooks[pair][side][i][1] = item['size']
        else:
            updatebooks[pair][side] = np.zeros((len(res['params'][side]), 2))
            for i, item in enumerate(res['params'][side]):
                updatebooks[pair][side][i][0] = item['price']
                updatebooks[pair][side][i][1] = item['size']
    

    for side in SIDES:
        if 'ask' in updatebooks[pair]: ### Update orderbook once update book has been populated ###
            # pass
            for ui, updateItem in enumerate(updatebooks[pair][side]):
                for oi, orderItem in enumerate(orderbooks[pair][side]):
                    if updateItem[0] == orderItem[0] and updateItem[1] != 0:
                        orderbooks[pair][side][oi][1] = updateItem[1]
                    elif updateItem[0] == orderItem[0] and updateItem[1] == 0:
                        orderbooks[pair][side] = np.delete(orderbooks[pair][side], oi, axis=0)
                    else:
                        pass
                if updateItem[0] not in orderbooks[pair][side] and updateItem[1] != 0:
                    orderbooks[pair][side] = np.append(orderbooks[pair][side], [updateItem], axis=0)
                else:
                    pass
        else:
            pass
    # print(orderbooks)

    # if 'ask' in orderbooks[pair]:
    #     for side in SIDES:
    #         for oi, orderItem in enumerate(orderbooks[pair][side]):
    #             print(orderItem, oi)

    # if 'ask' in updatebooks[pair]:
    #     for side in SIDES:
    #         for ui, updateItem in enumerate(updatebooks[pair][side]):
    #             print(updateItem)
    #             updateItem[0] = 111
    #             print(updateItem, '\n')
    
    # for side in SIDES:
    #     if side == 'bid':
    #         orderbooks[pair][side].sort(reverse=True)
    #     else:
    #         orderbooks[pair][side].sort()

    # print(orderbooks)
    # print('\n')



async def getBook(pair, method):
    path = 'data/' + pair + '_' + method + '.json'
    with open(path) as f:
        data = json.load(f)
        # data = f
    await buildBook(data, pair)

async def printBook():
    global orderbooks
    # global updatebooks
    while 1:
        await asyncio.sleep(3)
        print(orderbooks)
        # print(updatebooks)


async def main() -> None:
    coroutines = [
        loop.create_task(getBook(pair, method))
        for method in METHODS
            for pair in PAIRS
    ]
    coroutines.append(printBook())
    await asyncio.wait(coroutines)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()