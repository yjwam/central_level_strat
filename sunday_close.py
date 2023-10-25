import yfinance as yf
from ib_insync import *

def get_historical_data(ib,contract,barsize,trading_hours,which,debugging = False):
    # function to get historical data to calculate EMA used Yahoo finacne in debugging
    if debugging:
        sym = contract.symbol
        data = yf.download(tickers=sym,interval='1m')['Adj Close']
    else:
        try:
            RTH = True if trading_hours == "RTH" else False
            bars = ib.reqHistoricalData(
                contract=contract,
                endDateTime='',
                durationStr='1 W',
                barSizeSetting=barsize,
                whatToShow='TRADES',
                useRTH=RTH)
            data = util.df(bars)[which]
        except Exception as e:
            print("Error:",e)
            ib.sleep(1)
            print("Retrying to get historical data")
            data = get_historical_data(ib,contract,barsize,trading_hours,which,debugging = False)
    return data

def get_sunday_open(data,debugging):
    if debugging:
        return float(input("Enter sunday close: "))
    data['weekday'] = [date.weekday() for date in data['date']]
    data = data.sort_values(by=['date'], ascending=False)
    sunday_open = data.loc[0,'open']
    return sunday_open

def create_ib_contract(contract,ib):
    contract = Contract(symbol=contract['symbol'], secType=contract['secType'], exchange=contract['exchange'], currency=contract['currency'], includeExpired=True)
    contract = ib.qualifyContracts(contract)[0]
    return contract

contract_info = {"symbol" : "AAPL",
                "secType" : "STK",
                "exchange" : "SMART",
                "currency" : "USD"}

ib = IB()
for i in range(1,51):
    try:
        ib.connect('127.0.0.1', 7497, clientId=i)
        print(f"Connected with client ID {i}")
        ib_open = True
        break
    except ConnectionRefusedError:
        ib_open = False
        print("Please open TWS and run code again")
        break
    except:
        ib_open = False
        print(f"Client ID {i} in use")
        continue

contract = create_ib_contract(contract_info,ib)
hist_data = get_historical_data(ib,contract,'1 day','ETH',['date','open'])
sunday_open = get_sunday_open(hist_data)

print("Sunday Open:" ,sunday_open)