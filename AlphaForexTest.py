from siliconmetatrader5 import MetaTrader5

mt5 = MetaTrader5(host="localhost", port=8001, keepalive=True)

if not mt5.initialize():
    raise RuntimeError("🚨 MT5 initialization failed!")

print("Connected!")

symbols = ["USDJPYrfd", "AUDUSDrfd", "NZDUSDrfd"]
for symbol in symbols:
    print("========================================")
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        print(symbol, "not found, can not call order_check()")
        mt5.shutdown()
        quit()
    print(symbol_info.name)

symbol = "USDJPYrfd"

# lot = 0.1
# point = mt5.symbol_info(symbol).point
price = mt5.symbol_info_tick(symbol).ask
# deviation = 20
# request = {
#     "action": mt5.TRADE_ACTION_DEAL,
#     "symbol": symbol,
#     "volume": lot,
#     "type": mt5.ORDER_TYPE_SELL,
#     "price": price,
#     "sl": price - (symbol_info.trade_stops_level + 20) * point,
#     "tp": price + (symbol_info.trade_stops_level + 20) * point,
#     "deviation": deviation,
#     "magic": 234000,
#     "comment": "python script open",
#     "type_time": mt5.ORDER_TIME_GTC,
#     "type_filling": mt5.ORDER_FILLING_FOK,
# }

point = symbol_info.point

# Получаем оба тика
tick = mt5.symbol_info_tick(symbol)
ask = tick.ask
bid = tick.bid

lot = 0.1
deviation = 20

min_distance = symbol_info.trade_stops_level
# Для JPY-пар trade_stops_level может быть в "пунктах" (второй знак), 
# поэтому умножаем на 10, чтобы получить пипсы
safe_distance = (min_distance * 10) + 10  # запас 10 пипсов

request = {
    "action": mt5.TRADE_ACTION_DEAL,
    "symbol": symbol,
    "volume": lot,
    "type": mt5.ORDER_TYPE_SELL,  # ← SELL, а не BUY
    "price": price,                  # ← для SELL цена закрытия = Bid
    "sl": 0.0,
    "tp": 0.0,
    "deviation": deviation,
    "magic": 234000,
    "comment": "python script sell",
    "type_time": mt5.ORDER_TIME_GTC,
    "type_filling": mt5.ORDER_FILLING_FOK,  # ваш определённый ранее режим
}

# send a trading request
result = mt5.order_send(request)
# check the execution result
print("1. order_send(): by {} {} lots at {} with deviation={} points".format(symbol,lot,price,deviation));
if result.retcode != mt5.TRADE_RETCODE_DONE:
    print("2. order_send failed, retcode={}".format(result.retcode))
    # request the result as a dictionary and display it element by element
    result_dict=result._asdict()
    for field in result_dict.keys():
        print("   {}={}".format(field,result_dict[field]))
        # if this is a trading request structure, display it element by element as well
        if field=="request":
            traderequest_dict=result_dict[field]._asdict()
            for tradereq_filed in traderequest_dict:
                print("       traderequest: {}={}".format(tradereq_filed,traderequest_dict[tradereq_filed]))
    print("shutdown() and quit")
    mt5.shutdown()
    quit()
 
print("2. order_send done, ", result)
print("   opened position with POSITION_TICKET={}".format(result.order))
print("   sleep 2 seconds before closing position #{}".format(result.order))

mt5.close()