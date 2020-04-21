# import numpy as np
# from collections import Counter
# import random

# r = [random.randrange(100) for _ in range(10_000)]
# nr = np.array(r)
# rr = [random.randrange(1000) for _ in range(10_000)]
# nrr = np.array(rr)
# # print(type(nr))
# # print(nr)

# # %%timeit
# for x in nr:
#     for y in nrr:
#         if y == x:
#             print('Found a match')
#             break
#             # pass
#         else:
#             pass











# PAIRS = [
#     'BTCUSD',
#     'ETHUSD',
#     'ETHBTC',
#     'LTCUSD',
#     'LTCBTC',
#     'XRPUSD',
#     'XRPBTC'
# ]
# PAIRS1 = []
# for pair in PAIRS:
#     PAIRS1.append(pair[:3])
# arbitrageBook = {
#     arb: {
#         'forward': {},
#         'reverse': {}
#     }
#     for arb, count in Counter(PAIRS1).items() if count >1
# }
# print(arbitrageBook)
# u, c = np.unique(PAIRS, return_counts=True)
# dup = [item for item, count in Counter(PAIRS1).items() if count > 1]
# print(dup)

# for i in PAIRS:
#     print(i[:3])

# print(list(np.array(PAIRS).T[1]))

# PAIRS2 = [
#     pair for pair[:3] in PAIRS
# ]
# print(PAIRS2)























ARBS = [
    'ETH'
    ,'LTC'
    # ,'XRP'
    # ,'EOS'
]

PAIRS = []
for arb in ARBS:
    PAIRS.append(arb + 'USD')
    PAIRS.append(arb + 'BTC')
PAIRS.insert(0, 'BTCUSD')
PAIRS.sort(reverse=True)
print(PAIRS)


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
            'triangle_value': 0
        }
    }
    for arb in ARBS
}
# print(arbitrage_book)

for arb in ARBS:
    pair = arb + 'BTC'
    print(arbitrage_book[arb]['regular']['weighted_prices'][arb + 'BTC'])
