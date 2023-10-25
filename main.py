import datetime
import yfinance as yf
from ib_insync import *
import json
import os
import schedule
import pytz

def create_ib_contract(contract,ib):
    contract = Contract(symbol=contract['symbol'], secType=contract['secType'], exchange=contract['exchange'], currency=contract['currency'], includeExpired=True)
    contract = ib.qualifyContracts(contract)[0]
    return contract

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

def live_data(contract,ib,debugging=False):
    # function to fecth live data from ib tws using input for debugging
    if not debugging:
        ib.reqMktData(contract)
        ib.sleep(2)
        bar = ib.ticker(contract)
        price = bar.marketPrice()
        return price
    else:
        return float(input("Enter current price: "))
    
def place_order(contract,ib,action,quantity):
    order = MarketOrder(action, quantity)
    trade = ib.placeOrder(contract, order)
    while not trade.isDone():
        ib.waitOnUpdate()
    return trade

def update_results(path,contract,trade,sunday_open=0,first_target=0,second_target=0,reverse=False,first = 0):
    # function to write order placed and update order 
    trade = trade.fills[0]
    st = "long" if trade.execution.side == 'BOT' else "short"
    cId = str(contract.conId)+".json"
    path = os.path.join(path,cId)
    if reverse:
        with open(path) as f:
            temp = json.load(f)
        del temp['contract']
        temp = {"contract":str(trade.contract),
                "Reversed" : reverse,
                "old_position": temp,
                "entry_timestamp":str(datetime.datetime.now()),
                "entry_price":trade.execution.price,
                'long/short':st,
                'quantity':trade.execution.shares,
                'central_level':sunday_open,
                'first_target':first_target,
                'second_target':second_target,
                'check_first_target' : True,
                'check_second_target' : True,
                'current_quantity':trade.execution.shares}
        with open(path, 'w') as f:
            json.dump(temp, f, indent=4)
        return None
    if first==0:
        temp = {"contract":str(trade.contract),
                "Reversed" : reverse,
                "entry_timestamp":str(datetime.datetime.now()),
                "entry_price":trade.execution.price,
                'long/short':st,
                'quantity':trade.execution.shares,
                'central_level':sunday_open,
                'first_target':first_target,
                'second_target':second_target,
                'check_first_target' : True,
                'check_second_target' : True,
                'current_quantity':trade.execution.shares}
        with open(path, 'w') as f:
            json.dump(temp, f, indent=4)

    elif first == 1:
        with open(path) as f:
            temp = json.load(f)
        temp["first_exit_timestamp"] = str(datetime.datetime.now())
        temp['first_exit_price'] = trade.execution.price
        temp['first_quantity'] = trade.execution.shares
        temp['current_quantity'] = temp['current_quantity'] - trade.execution.shares
        temp['check_first_target'] = False
        with open(path, 'w') as f:
            json.dump(temp, f, indent=4)

    elif first == 2:
        with open(path) as f:
            temp = json.load(f)
        temp["second_exit_timestamp"] = str(datetime.datetime.now())
        temp['second_exit_price'] = trade.execution.price
        temp['second_quantity'] = trade.execution.shares
        temp['check_second_target'] = False
        temp['current_quantity'] = temp['current_quantity'] - trade.execution.shares
        with open(path, 'w') as f:
            json.dump(temp, f, indent=4)
    
    else:
        with open(path) as f:
            temp = json.load(f)
        temp["exit_timestamp"] = str(datetime.datetime.now())
        temp['exit_price'] = trade.execution.price
        temp['quantity'] = trade.execution.shares
        temp['current_quantity'] = temp['current_quantity'] - trade.execution.shares
        with open(path, 'w') as f:
            json.dump(temp, f, indent=4)
    return None

def check_open_orders(path,contract):
    cId = str(contract.conId)+".json"
    path = os.path.join(path,cId)
    try:
        with open(path) as f:
            temp = json.load(f)
            if "second_quantity" in list(temp.keys()) or "exit_price" in list(temp.keys()):
                return False, {}
            else:
                return True, temp
    except:
        return False,{}

def get_sunday_open(data,debugging):
    if debugging:
        return float(input("Enter sunday close: "))
    data['weekday'] = [date.weekday() for date in data['date']]
    data = data.sort_values(by=['date'], ascending=False)
    sunday_open = data.loc[0,'open']
    return sunday_open    

def trader(contract_info,ib,debugging=False):
    global exit_code
    delay = contract_info['time_frame']
    path = "orders"
    contract = create_ib_contract(contract_info['contract'],ib)
    # check any open positions
    print("Checking Open Position")
    open_pos,open_pos_dict = check_open_orders(path,contract) #istead of this right function to store sunday open or read file for sunday open
    if not open_pos:
        print("No Open Position Found")
    long_level = contract_info['long_level']
    short_level = contract_info['short_level']
    quantity = contract_info['no_contract']
    first_target_point = contract_info['first_target_point']
    second_target_point = contract_info['second_target_point']
    if not open_pos:
        cft = True
        cst = True
        hist_data = get_historical_data(ib,contract,'1 day',contract_info["trading_hours"],['date','open'],debugging)
        sunday_open = get_sunday_open(hist_data,debugging)
        while True: # find way to run it for time invertal
            current_price = live_data(contract,ib,debugging) 
            print(datetime.datetime.now()," Current Price :",current_price)
            if current_price > sunday_open + long_level:
                position = 1
                trade = place_order(contract,ib,"BUY",quantity)
                traded_price = trade.fills[0].execution.price
                first_target = traded_price + first_target_point
                second_target = traded_price + second_target_point
                update_results(path,contract,trade,sunday_open,first_target,second_target,0)
                print("Taking Long Position")
                break
            elif current_price < sunday_open - short_level:
                position = -1
                trade = place_order(contract,ib,"SELL",quantity)
                traded_price = trade.fills[0].execution.price
                first_target = traded_price - first_target_point
                second_target = traded_price - second_target_point
                update_results(path,contract,trade,sunday_open,first_target,second_target,0)
                print("Taking Short Position")
                break
            else:
                position = 0
                print("No position taken")
    else:
        print("Open Position Found")
        traded_price = open_pos_dict['entry_price']
        first_target = open_pos_dict['first_target']
        second_target = open_pos_dict['second_target']
        sunday_open = open_pos_dict['central_level']
        position = 1 if open_pos_dict["long/short"] == "long" else -1
        quantity = open_pos_dict["current_quantity"]
        cft = open_pos_dict['check_first_target']
        cst = open_pos_dict['check_second_target']
        now = datetime.datetime.now()
        if now.weekday() == 5 and now.time() > datetime.datetime(2000,1,1,15,59,59).time():
            print("Friday 4 P.M. : Closing All Position")
            action = "BUY" if position == -1 else "SELL"
            trade = place_order(contract,ib,action,quantity)
            update_results(path,contract,trade,first=3)
            exit_code = True

    while position != 0:
        ib.sleep(delay)
        current_price = live_data(contract,ib,debugging)
        print(datetime.datetime.now()," Current Price :",current_price)
        if position*(current_price-first_target) > 0 and cft:
            cft = False
            if position > 0:
                position = 0
                print("Taking Profit")
                trade = place_order(contract,ib,"SELL",quantity//2)
                update_results(path,contract,trade,first=1)
                continue
            else:
                position = 0
                print("Taking Profit")
                trade = place_order(contract,ib,"BUY",quantity//2)
                update_results(path,contract,trade,first=1)
                continue

        if position*(current_price-second_target) > 0 and cst:
            cst = False
            if position > 0:
                position = 0
                print("Taking Profit")
                trade = place_order(contract,ib,"SELL",quantity)
                update_results(path,contract,trade,first=2)
                continue
            else:
                position = 0
                print("Taking Profit")
                trade = place_order(contract,ib,"BUY",quantity)
                update_results(path,contract,trade,first=2)
                continue
        
        if position*(current_price - traded_price) < 0 and cst:
            print("Reverse Current Position")
            position = -1*position
            action = "BUY" if position == 1 else "SELL"
            trade = place_order(contract,ib,action,2*quantity)
            traded_price = trade.fills[0].execution.price
            first_target = traded_price + position*first_target_point
            second_target = traded_price + position*second_target_point
            update_results(path,contract,trade,sunday_open,first_target,second_target,reverse=True,first=0)
            if cst and not cft:
                second_target = first_target
            continue

    if not cst:
        exit_code = True
    return 0

def main(ib,i):
    exit_code = False
    print("Starting Algorithm")
    with open(r'contracts\AAPL.json') as f:
        contract_info = json.load(f)
    job = schedule.every().minute.at(":00").do(trader, contract_info = contract_info, ib = ib, debugging = debugging)
    while True:
        try:
            schedule.run_pending()
            if exit_code:
                break
            continue
        except Exception as e:
            print("Error:", e)
            ib.disconnect()
            ib = IB()   
            while not ib.isConnected():
                try:
                    ib.connect('127.0.0.1', 7497, clientId=i)
                    schedule.cancel_job(job)
                    job = schedule.every().minute.at(":00").do(trader, contract_info = contract_info, ib = ib, debugging = debugging)
                except Exception as e:
                    print("Error:",e)
                    print("Trying to reconnect with TWS")
                    ib.sleep(1)
            print("Reconnect with TWS")
            continue
    return None

debugging = False
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
est = pytz.timezone('US/Eastern')
if ib_open:
    main(ib,i)