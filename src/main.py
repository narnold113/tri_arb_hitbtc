import asyncio
import websockets
import json
import pandas as pd
import numpy as np
import weightedPrice as wp

# from weightedPrice import Orderbook

### This allows the entire np array to be printed. Set to 1000 for default ###
np.set_printoptions(threshold=1000)

# PAIRS = [
#     'BTCUSD',
#     'ETHUSD',
#     'ETHBTC'
# ]
# PAIRS2 = []
# for pair in PAIRS:
#     PAIRS2.append(pair[:3])

# ARBS = [item for item, count in Counter(PAIRS2).items() if count > 1]

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

# weightedPrices = {
#     pair: {}
#     for pair in PAIRS
# }

balance = 10_000


async def buildBook(res, pair) -> None:
    global orderbooks
    global updatebooks
    res = json.loads(res)

    ### Filter out status message ###
    if 'params' in res:
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
            if 'ask' in updatebooks[pair]: ### Update orderbook once updatebook has been populated ###
                for ui, updateItem in enumerate(updatebooks[pair][side]):
                    for oi, orderItem in enumerate(orderbooks[pair][side]):
                        if updateItem[0] == orderItem[0] and updateItem[1] != 0:
                            orderbooks[pair][side][oi][1] = updateItem[1]
                            break
                        elif updateItem[0] == orderItem[0] and updateItem[1] == 0:
                            orderbooks[pair][side] = np.delete(orderbooks[pair][side], oi, axis=0)
                            break
                        else:
                            pass
                    if updateItem[0] not in orderbooks[pair][side] and updateItem[1] != 0:
                        orderbooks[pair][side] = np.append(orderbooks[pair][side], [updateItem], axis=0)
                    else:
                        pass
            else:
                pass
        for side in SIDES:
            if side == 'bid':
                orderbooks[pair][side] = orderbooks[pair][side][orderbooks[pair][side][:,0].argsort()[::-1]]
            else:
                orderbooks[pair][side] = orderbooks[pair][side][orderbooks[pair][side][:,0].argsort()]
    else:
        pass
    
    # if 'ask' in orderbooks['ETHUSD']:
    #     orderbooks['ETHUSD']['ask'] = orderbooks['ETHUSD']['ask'][orderbooks['ETHUSD']['ask'][:,0].argsort()]
    #     print('This is the orderbook book:')
    #     print(orderbooks['ETHUSD']['ask'][0:25])

    
    # if 'ask' in updatebooks['ETHUSD']:
    #     print('This is the update book:')
    #     print(updatebooks['ETHUSD']['ask'])
    #     print('\n')
    

async def subscribeToBook(pair) -> None:
    url='wss://api.hitbtc.com/api/2/ws'
    strParams = '''{"method": "subscribeOrderbook","params": {"symbol": "placeholder"},"id": 123}'''
    params = json.loads(strParams)
    params['params']['symbol'] = pair
    async with websockets.client.connect(url) as websocket:
        await websocket.send(str(params).replace('\'', '"'))
        while 1:
            res = await websocket.recv()
            await buildBook(res, pair)

async def printBook():
    global orderbooks
    while 1:
        await asyncio.sleep(6)
        # orderbooks['ETHUSD']['ask'] = orderbooks['ETHUSD']['ask'][orderbooks['ETHUSD']['ask'][:,0].argsort()]
        # print(orderbooks['ETHUSD']['ask'][0:25])
        # print('ask size', orderbooks['ETHUSD']['ask'].shape)
        # print('bid size', orderbooks['ETHUSD']['bid'].shape)
        print(orderbooks['ETHUSD'])
        # print(orderbooks['ETHUSD']['bid'][0:25])

async def calculateWeightedPriceBook():
    global orderbooks
    global balance
    global weightedPrices
    global arbitrageBook

    while 1:
        await asyncio.sleep(5)

        for arb in ARBS:
            arb['forward']

        # for pair in PAIRS:
        #     for side in SIDES:
        #         if pair[-3:] != 'BTC':
        #             weightedPrices[pair][side] = wp.getWeightedPrice(orderbooks[pair][side], balance)
        #             weightedPrices[pair]['amountIfBought'] = balance / weightedPrices[pair][side] 
        #         else:
        #             pass
        
        # for arb in ARBS:
        #     for p


        # for pair in PAIRS:
        #     for side in SIDES:
        #         if pair[-3:] == 'BTC':
        #             weightedPrices[pair][side] = wp.getWeightedPrice(orderbooks[pair][side], weightedPrices['BTCUSD']['amountIfBought'])
        #             # weightedPrices[pair]['amountIfBought'] = balance / weightedPrices[pair][side] 
        #         else:
        #             pass
        # print(weightedPrices)

async def main() -> None:
    coroutines = [
        loop.create_task(subscribeToBook(pair))
        for pair in PAIRS
    ]
    coroutines.append(printBook())
    # coroutines.append(calculateWeightedPriceBook())
    await asyncio.wait(coroutines)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except:
        pass
    finally:
        loop.close()