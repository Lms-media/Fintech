import time

from QUIK.QuikPy import QuikPy

provider = QuikPy()

classes = provider.get_classes_list()['data']
print("Доступные классы:", classes)  # Ищем CETS в выводе

info = provider.get_security_info("CETS", "USD000000TOD")
print(info)

trading_status = provider.get_param_ex("CETS", "USD000000TOD", "STATUS")['data']
print(f"Статус торгов: {trading_status}")  # Должен быть "T" (торгуется)

current_close = provider.get_param_ex("CETS", "USD000000TOD", 'last')
print(f"Текущая цена закрытия: {current_close}")

inf = provider.get_symbol_info("CETS", "USD000000TOD")
print(inf)

security_info = provider.get_param_ex("CETS", "USD000000TOD", "PREVPRICE")  # "LAST" — цена последней сделки
close_price = security_info['data']["param_value"]  # Для цены закрытия может потребоваться другой параметр, например "PREVPRICE"
print(f"Цена закрытия для USD000000TOD: {close_price}")
