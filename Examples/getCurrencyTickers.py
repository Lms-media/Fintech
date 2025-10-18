import requests

url = "https://iss.moex.com/iss/engines/currency/markets/selt/securities.json"
response = requests.get(url)
data = response.json()

for sec in data['securities']['data']:
    print(f"Тикер: {sec[0]}, Название: {sec[10]}")
