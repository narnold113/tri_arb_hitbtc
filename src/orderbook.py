# import requests
import json
# btcusd = requests.get('https://api.hitbtc.com/api/2/public/orderbook/BTCUSD?limit=10').json()
# ethusd = requests.get('https://api.hitbtc.com/api/2/public/orderbook/ETHUSD?limit=10').json()
# ethbtc = requests.get('https://api.hitbtc.com/api/2/public/orderbook/ETHBTC?limit=10').json()


# def readfile(filepath):
#     with open(filepath, 'r') as myFile:
#         data = myFile.read()
#         obj = json.loads(data)
#         return obj


### returns a list of dicts ###
# btcusdRaw = readfile('dev/OrderBooks/BTCUSD_snapshot.json')

# class Orderbook:
#     def __init__(self, pair, orderbook, balance):
#         self.pair = pair
#         self.pair.ask = orderbook['ask']
#         self.pair.bid = orderbook['bid']
#         self.balance = balance

#     def getWeightedPrice(self, orders, reverse=False):
#         volume = 0
#         price = 0
#         wp = 0
#         remainder = 0
#         if reverse:
#             for order in orders:
#                 volume += order[1]
#                 wp += order[0] * (order[1] / balance)
#                 if volume >= balance:
#                     remainder = volume - balance
#                     wp -= order[0] * (remainder / balance)
#                     return wp
#         else:
#             for order in orders:
#                 volume += order[0] * order[1]
#                 wp += order[0] * ((order[0] * order[1]) / balance)
#                 if volume >= balance:
#                     remainder = volume - balance
#                     wp -= order[0] * (remainder / balance)
#                     return wp
    
#     def getBidWp(self, reverse=False):
#         return self.getWeightedPrice(self.bids, reverse)
    
#     def getAskWp(self, reverse=False):
#         return self.getWeightedPrice(self.ask, reverse)


# class Orderbook:
#     def __init__(self, orderbook, balance):
#         self.asks = orderbook['params']['ask']
#         self.bids = orderbook['params']['bid']
#         self.balance = balance

    # def getWeightedPrice(self, orders):
    #     volume = 0
    #     price = 0
    #     wp = 0
    #     remainder = 0
    #     for order in orders:
    #         volume += float(order['price']) * float(order['size'])
    #         wp += float(order['price']) * ((float(order['price']) * float(order['size'])) / self.balance)
    #         if volume > self.balance:
    #             remainder = volume - self.balance
    #             wp -= float(order['price']) * (remainder / self.balance)
    #             return wp
    
#     def getBidsWp(self):
#         return self.getWeightedPrice(self.bids)
    
#     def getAsksWp(self):
#         return self.getWeightedPrice(self.asks)



# btcusd = Orderbook(btcusdRaw, 10000)
# print(type(btcusd))
# print(btcusd.getAsksWp())


ethbtcBids = [[0.03, 10], [.025, 10], [.02, 10]]

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

print(getWeightedPrice(ethbtcBids, 0.980392156862745, reverse=True))






















# content2 = requests.get('https://api.bitforex.com/api/v1/market/depth?symbol=coin-usdt-btc&size=10')


# content = requests.get('https://api.hitbtc.com/api/2/public/ticker/BTCUSD')

# print(content2.json()['data']['asks'])