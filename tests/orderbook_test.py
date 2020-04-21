# btc_book = {
#     'orderbook': {
#         'ask': [],
#         'bid': []
#     },
#     'weighted_prices': {
#         'regular': 0,
#         'reverse': 0
#     },
#     'amount_if_bought': 0
# }

# ARBS = [
#     'ETH',
#     'LTC'
# ]
# PAIRS = []
# for arb in ARBS:
#     PAIRS.append(arb + 'BTC')
#     PAIRS.append(arb + 'USD')
# PAIRS.insert(0, 'BTCUSD')
# # print(PAIRS)
# ARB_TYPE = [
#     'regular',
#     'reverse'
# ]


# arbitrage_book = {
#     arb: {
#         'orderbooks': {
#             pair: []
#             for pair in PAIRS if pair[:3] == arb
#         },
#         'regular': { ### Regular arbitrage order: buy BTC/USD, buy ALT/BTC and sell ALT/USD. For buys, we calculate weighted price on the "ask" side ###
#             'weighted_prices': {
#                 pair: 0
#                 for pair in PAIRS if pair[:3] == arb or pair == 'BTCUSD'
#             },
#             'triangle_value': 0
#         },
#         'reverse': { ### Reverse arbitrage order: buy ALT/USD, sell ALT/BTC and sell BTC/USD. For sells, we consume the "bid" side of the orderbook ###
#             'weighted_prices': {
#                 pair: 0
#                 for pair in PAIRS if pair[:3] == arb or pair == 'BTCUSD'
#             },
#             'triangle_value': 0
#         }
#     }
#     for arb in ARBS
# }
# # print(arbitrage_book)

# btc_book['weighted_prices']['regular'] = 7000
# btc_book['weighted_prices']['reverse'] = 6800


# # for arb in ARBS:
# #     for arb_type in ARB_TYPE:
# #         if arb_type == 'regular':
# #             arbitrage_book[arb][arb_type]['weighted_prices']['BTCUSD'] = btc_book['weighted_prices']['regular']
# #         else:
# #             arbitrage_book[arb][arb_type]['weighted_prices']['BTCUSD'] = btc_book['weighted_prices']['reverse']

# for arb in ARBS:
#     arbitrage_book[arb]['regular']['weighted_prices']['BTCUSD'] = btc_book['weighted_prices']['regular']
#     arbitrage_book[arb]['reverse']['weighted_prices']['BTCUSD'] = btc_book['weighted_prices']['reverse']



# print(arbitrage_book)









def getWeightedPrice(orders, balance, reverse=False):
    volume = 0
    price = 0
    wp = 0
    remainder = 0
    if reverse:
        for order in orders:
            volume += order[1]
            wp += order[0] * (order[1] / balance)
            if volume >= balance:
                remainder = volume - balance
                wp -= order[0] * (remainder / balance)
                return wp
    else:
        for order in orders:
            volume += order[0] * order[1]
            wp += order[0] * ((order[0] * order[1]) / balance)
            if volume >= balance:
                remainder = volume - balance
                wp -= order[0] * (remainder / balance)
                return wp