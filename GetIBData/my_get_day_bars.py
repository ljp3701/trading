import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import datetime


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bars = None
        self.end_date = None

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        event_connect.set()

    def historicalData(self, reqId, bar):
        self.bars.append((bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.average))

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        df = pd.DataFrame(self.bars, columns="date open high low close volume wap".split()).set_index('date')
        df.to_csv(open('C:/junk/bars_year.csv', 'w'),  line_terminator='\n')
        event_datadone.set()

    def get_data(self, contract, queryTime):
        self.bars = []
        self.end_date = queryTime.strftime("%Y%m%d %H:%M:%S")
        app.reqHistoricalData(reqId=1,
                              contract=contract,
                              endDateTime=self.end_date,
                              durationStr='1 Y',
                              barSizeSetting='1 day',
                              whatToShow='TRADES',
                              useRTH=1,
                              formatDate=1,
                              keepUpToDate=False,
                              chartOptions=[])


def websocket_con():
    app.run()


event_connect = threading.Event()
event_datadone = threading.Event()
app = TradingApp()

app.connect("127.0.0.1", 4002, clientId=1)
threading.Thread(target=websocket_con, daemon=True).start()
event_connect.wait()
time.sleep(1)

contract = Contract()
contract.symbol = "SPY"
contract.exchange = "SMART"
contract.secType = "STK"
contract.currency = "USD"

dd = datetime.timedelta(days=1)
query_time_start = datetime.datetime(2020, 2, 14, 16, 30)
event_datadone.clear()
app.get_data(contract, query_time_start)
event_datadone.wait()


time.sleep(1)
app.disconnect()
print('DONE DONE DONE')
