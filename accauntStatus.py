from siliconmetatrader5 import MetaTrader5

mt5 = MetaTrader5(host="localhost", port=8001, keepalive=True)

if not mt5.initialize():
    raise RuntimeError("🚨 MT5 initialization failed!")

# Получаем все открытые позиции
positions = mt5.positions_get()

if positions is None:
    print("Ошибка при получении позиций:", mt5.last_error())
elif len(positions) == 0:
    print("У вас нет открытых позиций.")
else:
    print(f"Всего открытых позиций: {len(positions)}")
    for pos in positions:
        print("----------------------------------------")
        print(f"Символ:      {pos.symbol}")
        print(f"Тикет:       {pos.ticket}")
        print(f"Тип:         {'BUY' if pos.type == 0 else 'SELL'}")
        print(f"Объём:       {pos.volume}")
        print(f"Цена входа:  {pos.price_open}")
        print(f"Текущая цена: {pos.price_current}")
        print(f"Прибыль:     {pos.profit}")

positions = mt5.positions_get()
for pos in positions:
    # Определяем тип закрывающего ордера: для BUY — SELL, для SELL — BUY
    close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    
    # Получаем актуальную цену
    tick = mt5.symbol_info_tick(pos.symbol)
    price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask
    
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pos.symbol,
        "volume": pos.volume,
        "type": close_type,
        "position": pos.ticket,
        "price": price,
        "deviation": 20,
        "magic": 234000,
        "comment": "close by python",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,  # ← Явно указываем IOC
    }
    
    result = mt5.order_send(request)
    print(f"Закрытие {pos.ticket}: retcode={result.retcode}")

mt5.close()