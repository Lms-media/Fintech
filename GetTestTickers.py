from QUIK.QuikPy import QuikPy

qp_provider = QuikPy()
assets = ('OZON', 'HEAD', 'DELI', 'ROSN')
# для ROSN нули будут
print(qp_provider.get_trade_accounts())

for asset in assets:
    class_code, sec_code = qp_provider.dataname_to_class_sec_codes(asset)
    print("Режим торгов и название актива: ", class_code, sec_code)
    si = qp_provider.get_symbol_info(class_code, sec_code)
    lastPrice = qp_provider.get_param_ex(class_code, sec_code, 'LAST')
    openPrice = qp_provider.get_param_ex(class_code, sec_code, 'OPEN')
    lowPrice = qp_provider.get_param_ex(class_code, sec_code, 'LOW')
    highPrice = qp_provider.get_param_ex(class_code, sec_code, 'HIGH')
    closePrice = qp_provider.get_param_ex(class_code, sec_code, 'PREVPRICE') # как правило ноль, думаю на бектестах будет что-то другое

    print(lastPrice)
    print(openPrice)
    print(lowPrice)
    print(highPrice)
    print(closePrice)

qp_provider.close_connection_and_thread()
