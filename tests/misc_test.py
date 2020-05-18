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























# ARBS = [
#     'ETH'
#     ,'LTC'
#     # ,'XRP'
#     # ,'EOS'
# ]

# PAIRS = []
# for arb in ARBS:
#     PAIRS.append(arb + 'USD')
#     PAIRS.append(arb + 'BTC')
# PAIRS.insert(0, 'BTCUSD')
# PAIRS.sort(reverse=True)
# print(PAIRS)


# arbitrage_book = {
#     arb: {
#         'orderbooks': {
#             pair: {}
#             for pair in PAIRS if pair[:3] == arb
#         },
#         'regular': { ### Regular arbitrage order: buy BTC/USD, buy ALT/BTC and sell ALT/USD. For buys, we calculate weighted price on the "ask" side ###
#             'weighted_prices': {
#                 pair: 0
#                 for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
#             },
#             'triangle_value': 0
#         },
#         'reverse': { ### Reverse arbitrage order: buy ALT/USD, sell ALT/BTC and sell BTC/USD. For sells, we consume the "bid" side of the orderbook ###
#             'weighted_prices': {
#                 pair: 0
#                 for pair in PAIRS if pair[:3] == arb # or pair == 'BTCUSD'
#             },
#             'triangle_value': 0
#         }
#     }
#     for arb in ARBS
# }
# # print(arbitrage_book)

# for arb in ARBS:
#     for pair in sorted(arbitrage_book[arb]['orderbooks'], reverse=True):
#         print(pair)















# ARBS = [
#     'ETH'
#     ,'LTC'
#     ,'XRP'
#     ,'EOS'
# ]
# # tables = {}

# # for arb in ARBS:
# #     tables[arb] = str(
# #         "CREATE TABLE {} "
# #         "TEST"
# #     )

# # print(tables)

# time = '2019-01-01'

# for arb in ARBS:
#     data = [arb, time, 0.02, -0.02, .015, -0.015, 0.01, -0.01]
#     # print(data)
#     query = str(
#         "INSERT INTO {0} "
#         "VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7})".format(*data)
#     )
#     print(query)






































# from datetime import datetime
# import numpy as np
# startTime = datetime.now()
# osize=60001
# usize=1000
# dtype=[('price', 'f4'), ('size', 'f4')]

# # xx = np.array([[100, 5],[200, 1],[300, 2],[400, 2],[500, 3]])
# # yy = np.array([[200, 0],[300, 0],[400, 1],[600,5]])

# oprice = np.random.randint(7001, size=osize)
# oquantity = np.random.randint(51, size=osize)
# btc_orderbook = np.column_stack((oprice,oquantity))
# # btc_orderbook = np.array(btc_orderbook, dtype=dtype)
# print(btc_orderbook[0])
# uprice = np.random.randint(7001, size=usize)
# uquantity = np.random.randint(60, size=usize)
# update_orders = np.column_stack((uprice,uquantity))
# # print(updateOrders)



# # btc_orderbook = np.array([(101, 1), (102, 2), (103, 3), (104, 4), (105, 5), (106, 6)], dtype=[('price', 'f4'), ('size', 'f4')])
# # update_orders = np.array([(101, 2), (102, 0), (103, 1), (106, 10), (107, 7), (108,8)], dtype=[('price', 'f4'), ('size', 'f4')])

# # inter, orders_ind, updateorders_ind = np.intersect1d(btc_orderbook['price'], update_orders['price'], return_indices=True)
# # btc_orderbook[orders_ind] = update_orders[updateorders_ind]

# # notin_ind = np.in1d(update_orders['price'], btc_orderbook['price'],invert=True)
# # btc_orderbook = np.append(btc_orderbook, update_orders[notin_ind], axis=0)

# # delete_ind = np.where(btc_orderbook['size'] == 0)[0]
# # btc_orderbook = np.delete(btc_orderbook, delete_ind, axis=0)

# # print(btc_orderbook)

# # btc_orderbook = np.array([[101, 1], [102, 2], [103, 3], [104, 4], [105, 5], [106, 6]])
# # update_orders = np.array([[101, 2], [102, 0], [103, 1], [106, 10], [107, 7], [108,8], [99,1]])

# inter, orders_ind, updateorders_ind = np.intersect1d(btc_orderbook[:,0], update_orders[:,0], return_indices=True)
# btc_orderbook[orders_ind] = update_orders[updateorders_ind]

# notin_ind = np.in1d(update_orders[:,0], btc_orderbook[:,0], invert=True)
# btc_orderbook = np.append(btc_orderbook, update_orders[notin_ind], axis=0)

# delete_ind = np.where(btc_orderbook == 0)[0]
# btc_orderbook = np.delete(btc_orderbook, delete_ind, axis=0)

# btc_orderbook = btc_orderbook[btc_orderbook[:,0].argsort()[::-1]]

# print(btc_orderbook)






# # notind = np.in1d(updateOrders[:,0], orders[:,0], invert=True)
# # orders = np.append(orders, updateOrders[notind], axis=0)

# # inter, orders_ind, updateOrders_ind = np.intersect1d(orders[:,0], updateOrders[:,0], return_indices=True)
# # orders[orders_ind] = updateOrders[updateOrders_ind]

# # delind = np.where(orders == 0)[0]
# # orders = np.delete(orders, delind, axis = 0)

# # print(orders)




# # for ui, updateItem in enumerate(updateOrders):
# #     for oi, orderItem in enumerate(orders):
# #         if updateItem[0] == orderItem[0]:
# #             if updateItem[1] != 0:
# #                 orders[oi][1] = updateItem[1]
# #                 break
# #             else:
# #                 orders = np.delete(orders, oi, axis=0)
# #                 break            
# #         else:
# #             continue
# #     if updateItem[0] not in orders[:,0] and updateItem[1] != 0:
# #         orders = np.append(orders, [updateItem], axis=0)
# #     else:
# #         continue

# # print(orders)

# print(datetime.now() - startTime)























# import numpy as np




# ### regular arb (Buy btcusd, buy ethbtc, sell ethusd) ###
# btcusd = [7000, 7050, 7100] # x
# ethusd = [210, 209, 208] # y
# ethbtc = [0.032, 0.0323, 0.0325] # z
# reg_arb_price = [x * z for x,z in zip(btcusd, ethbtc)]
# reg_tri_value = [(y - rap) / rap for y,rap in zip(ethusd,reg_arb_price)]

# # print(reg_arb_price)
# # print(reg_tri_value)


# ### reverse arb (Sell btcusd, sell ethbtc, buy ethusd) ###
# btcusd = np.array([6990, 6950, 6900]) # x
# ethusd = np.array([211, 212, 213]) # y
# ethbtc = np.array([0.032, 0.0317, 0.0315]) # z
# rev_arb_price = np.array([x / z for x,z in zip(ethusd, ethbtc)])
# rev_tri_value = np.array([(y - rap) / rap for y,rap in zip(btcusd,rev_arb_price)])

# xx = rev_tri_value * 100
# # xx = [100 * yy for yy in rev_tri_value]
# # print(reg_arb_price)
# print(xx)



























# from datetime import datetime
# startTime = datetime.now()

# pairs = ['btc', 'eth', 'bch']
# update_list = ['btc', 'btc', 'btc', 'eth', 'ltc', 'bch']


# not_full = True
# while not_full:
#     try:
#         check = all(item in update_list for item in pairs)
#         if check:
#             print(datetime.now() - startTime)
#             print('Update list contains all items in PAIRS')
#             # not_full = False
#             break
#         else:
#             pass
#     except:
#         print('There was an error')
#     else:
#         # print('Not yet')
#         continue
# # check = all(item in update_list for item in pairs)
# # print(check)




















# regular = [1,3,5]
# arb_data = regular
# reverse = [2,4,6]

# # for i, item in enumerate(y):
# #     x.insert(i+1, item)
# # print(x)

# i=1
# for item in reverse:
#     arb_data.insert(i, item)
#     i+=2
# print(tuple(arb_data))


# # z = x+y
# # print(z)



























import numpy as np

x = np.array([8001.5748393384, 8002.05182], np.float64)
print(x)
# print(type(x[0].item()))