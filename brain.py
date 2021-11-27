import pandas as pd
import numpy as np
import talib as ta
from datetime import date, timedelta
import datetime as dt
import config
import logging, time
from dbquery import Database
import asyncio, aiohttp, json, requests

mainfolder = config.mainfolder
db = Database('{}/app.db'.format(mainfolder))

trd_date = dt.datetime.today().strftime('%d_%m_%Y')
LOG_FORMAT = "%(levelname)s %(asctime)s %(name)s- %(message)s"
logging.basicConfig(filename='{}/logs/trade_day_{}.log'.format(mainfolder, trd_date),
                    level = logging.DEBUG,
                    format= LOG_FORMAT,
                    filemode = 'a')
logger = logging.getLogger(__name__)

class Signal:
    def __init__(self, name, exchange, ins_type, time, price, action):
        self.pa_webhooks = config.webhooks
        self.name = name
        self.exchange = exchange
        self.ins_type = ins_type
        self.time = time
        self.price = price
        self.action = action
        
    def getJsonStructure(self):
        dic = {"passphrase": "itsmebro",
                    "time": self.time,
                    "tkr": self.name,
                    "ins_type": self.ins_type,
                    "exng": self.exchange,
                    "strategy": {
                    "order_contracts": 0,
                    "order_price": self.price,
                    "trigger_price": 0,
                    "order_action": self.action,
                    "order_type": "Market",
                    "product_type": "MIS"}}
        return json.dumps(dic)
    
    async def post(self, login_url):
        async with self.session.post(login_url, data = self.payload) as resp:
            resps = await resp.json(content_type=None)
            return resps['message']

    async def pa_post(self):
        async with aiohttp.ClientSession() as self.session:
            tasks = []
            for webhook in self.pa_webhooks:
                login_url = webhook + '/pa_webhook'
                tasks.append(asyncio.ensure_future(Signal.post(self, login_url)))
            all_resps = await asyncio.gather(*tasks)
            messa = 'Spectre: ' + ": ".join(all_resps)
            logger.info(messa)
            telegramer(messa)
            return messa
    
    def post_signal(self):
        self.payload = Signal.getJsonStructure(self)
        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #only for windows
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy()) #For linux CHANGE before moving the code!
        return asyncio.run(Signal.pa_post(self))

class fetch_data:
    def __init__(self, fyers, symbol, timeframe):
        self.fyers = fyers
        self.symbol = symbol
        self.timeframe = timeframe

    def fetch(self):
        current_date = date.today()
        tommorrow = current_date + timedelta(days = 1)
        yesterday = current_date - timedelta(days = 4)
        data = {"symbol":self.symbol,"resolution":self.timeframe,"date_format":"1","range_from":yesterday,"range_to":tommorrow,"cont_flag":"1"}
        resp = self.fyers.history(data)
        if  resp['s']=='ok':
            return resp['candles']
        else:
            eve = f"Failed fetch for symbol {self.symbol}"
            logger.warning(eve)
            telegramer(eve)
            time.sleep(0.3)
            return fetch_data.fetch(self)

    def get_data(self):
            candleinfo = fetch_data.fetch(self)
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame(candleinfo, columns=columns)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s') + pd.Timedelta('05:30:00') #Adding 5:30hrs manually to UTC for VC
            df['minute'] = df.datetime.dt.time.map(str)
            df = df.set_index(pd.DatetimeIndex(df["datetime"]))
            df = df.drop(columns = ['timestamp'])
            return df

def exit_one(name, exchange, ins_type, current_time, ltp, eve):
    exit_signal = Signal(name, exchange, ins_type, current_time, ltp, 'exit_one')
    print(exit_signal.post_signal())
    logger.info(eve)
    telegramer(eve)
    print(eve)
    
def SL_trigger(SL, LTP, name, exchange, ins_type, now):
    try:
        if SL<0: #short position
            if LTP > abs(SL):
                eve = f"Buy SL triggered for {name}"
                db.update_position(name, None, 0)
                exit_one(name, exchange, ins_type, now, LTP, eve)
        elif SL>0: #long position
            if LTP < SL:
                eve = f"Sell SL triggered for {name}"
                db.update_position(name, None, 0)
                exit_one(name, exchange, ins_type, now, LTP, eve)
        else:
            pass
    except:
        logger.warning(f"SL check missed for {name}")
        pass

def fetch_ltp(fyers, name, symbol, c):
    if c < 4:
        try:
            return float(fyers.quotes({"symbols":symbol})['d'][0]['v']['lp'])
        except:
            eve = f"Glitch getting quote for {name}"
            logger.warning(eve)
            db.update_trade(name, False)
            c+=1
            time.sleep(0.2)
            fetch_ltp(fyers, name, symbol, c)
    else:
        eve = f"Fetch LTP failed for {name}"
        telegramer(eve)
        logger.error(eve)
        # sys.exit(eve)
        return None

def change_timeframe(data, timeframe): 
    ohlc = {
        'datetime':'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    df = data.resample(timeframe).apply(ohlc)
    df_tf = df.dropna()
    return df_tf

def P_T(df, timeframe, length):
    dft = change_timeframe(df, timeframe)
    plen = len(str(int(dft.close[-1]))) 
    tr = (dft.close - dft.open)*(10**plen)/dft.close
    dft['trn'] = round(ta.SMA(tr, length), 3) #!!!!
    return dft

def current_timeframe(data, timeframe): 
    ohlc = {
        'minute' : 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'Trend':'last',
        'strn':'last',
        'ltrn':'last',
        'ATR':'last'
    }
    df = data.resample(timeframe).apply(ohlc)
    df_tf = df.dropna()
    return df_tf

def T_T(fyers, symbol, name, exchange, ins_type, stfp, ltfp, ctfp, length, multi, start_time, end_time):
    # try:
        stf = str(stfp) + 'min'
        ltf = str(ltfp) + 'min'
        ctf = str(ctfp) + 'min'
        df1 = fetch_data(fyers, symbol, 1).get_data()
        dfS = P_T(df1, stf, length)
        dfL = P_T(df1, ltf, length)
        dfL['lATR'] = round(ta.ATR(dfL.high, dfL.low, dfL.close, timeperiod=length), 1)
        df1.loc[df1.datetime.isin(dfS.datetime),'strn'] = dfS['trn']
        df1.loc[df1.datetime.isin(dfL.datetime),'ltrn'] = dfL['trn']
        df1.loc[df1.datetime.isin(dfL.datetime),'ATR'] = dfL['lATR']
        df1.strn.ffill(inplace=True)
        df1.ltrn.ffill(inplace=True)
        df1.ATR.ffill(inplace=True)
        df1.strn.fillna(0, inplace=True)
        df1.ltrn.fillna(0, inplace=True)
        df1.ATR.fillna(0, inplace=True)
        df1.loc[(df1.strn > 0) & (df1.ltrn > 0), 'Trend'] = 'Uptrend'
        df1.loc[(df1.strn < 0) & (df1.ltrn < 0), 'Trend'] = 'Downtrend'
        df1.loc[((df1.strn > 0) & (df1.ltrn < 0)) | ((df1.strn < 0) & (df1.ltrn > 0)) , 'Trend'] = 'Sideways'
        df1.Trend.fillna('Sideways', inplace=True)
        dfc = current_timeframe(df1, ctf)
        now = dt.datetime.now().strftime("%H:%M:00")
        if dfc.minute.iloc[-1] == now:
            dfc.drop(dfc.tail(1).index,inplace=True)
        prev_signal, prev_stop = db.fetch_position(name)
        dfc.loc[(dfc['Trend'] == 'Uptrend'), 'signal'] = 'Buy'
        dfc.loc[(dfc['Trend'] == 'Downtrend'), 'signal'] = 'Sell'
        dfc.signal.ffill(inplace=True)
        ####################codes to logg the trade data are marked as #$%@! ######
        dfc['signal'].replace('', np.nan, inplace=True) #$%@!
        dfc.dropna(subset=['signal'], inplace=True) #$%@!
        dfc.loc[(dfc['minute'] < start_time), 'signal'] = 'Wait'
        dfc.loc[(dfc['minute'] > end_time), 'signal'] = 'Eod'
        dfc['prev_signal'] = dfc.signal.shift(periods=1)
        dfc.prev_signal.fillna('Wait', inplace=True) 
        # db.update_position(name, dfc['signal'][-1])
        dfc.loc[(((dfc.signal == 'Buy') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                ((dfc.signal == 'Buy') & (dfc.prev_signal == 'Sell'))), 'buy_entry'] = dfc.close
        dfc.loc[(((dfc.signal == 'Sell') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                ((dfc.signal == 'Sell') & (dfc.prev_signal == 'Buy'))), 'buy_entry'] = 0
        dfc.loc[(((dfc.signal == 'Sell') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                ((dfc.signal == 'Sell') & (dfc.prev_signal == 'Buy'))), 'sell_entry'] = dfc.close
        dfc.loc[(((dfc.signal == 'Buy') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                ((dfc.signal == 'Buy') & (dfc.prev_signal == 'Sell'))), 'sell_entry'] = 0
        dfc.buy_entry.ffill(inplace=True)
        dfc.sell_entry.ffill(inplace=True)
        dfc['cap_pnts'] = 0
        dfc.loc[((dfc.signal == 'Buy') & (dfc.prev_signal == 'Buy')), 'cap_pnts'] = dfc.close - dfc.buy_entry
        dfc.loc[((dfc.signal == 'Sell') & (dfc.prev_signal == 'Sell')), 'cap_pnts'] = dfc.sell_entry - dfc.close
        conditions = [(((dfc.signal == 'Buy') & (dfc.prev_signal == 'Buy')) | ((dfc.signal == 'Buy') & (dfc.prev_signal != 'Buy'))) & (dfc.cap_pnts < .0055*dfc.buy_entry),
                    (((dfc.signal == 'Buy' ) & (dfc.prev_signal == 'Buy')) & (dfc.cap_pnts > .0055*dfc.buy_entry)),
                    (((dfc.signal == 'Sell' ) & (dfc.prev_signal == 'Sell'))  | ((dfc.signal == 'Sell') & (dfc.prev_signal != 'Sell'))) & (dfc.cap_pnts < .0055*dfc.sell_entry),
                    (((dfc.signal == 'Sell' ) & (dfc.prev_signal == 'Sell')) & (dfc.cap_pnts > .0055*dfc.sell_entry)),
                    (dfc.signal != dfc.prev_signal)]
        choices = [(dfc.buy_entry - (multi*dfc.ATR)), (dfc.close - dfc.ATR), ((dfc.sell_entry + (multi*dfc.ATR))*-1), ((dfc.close + dfc.ATR)*-1), 0]
        dfc['stop_limit'] = np.select(conditions, choices, default=0)
        if prev_stop != 0 and ((dfc.signal.iloc[-1] == 'Buy' and prev_signal == 'Buy') or (dfc.signal.iloc[-1] == 'Sell' and prev_signal == 'Sell')):
            stop = max(dfc.stop_limit.iloc[-1], prev_stop)
            db.update_position(name, dfc['signal'][-1], stop)
        else:
            pass
        if dfc.signal.iloc[-1] == 'Buy' and prev_signal != 'Buy':
            buy_signal = Signal(name, exchange, ins_type, dfc.minute.iloc[-1], dfc.close.iloc[-1], 'buy')
            stop = (dfc.close.iloc[-1] - dfc.ATR.iloc[-1]) if (dfc.prev_signal.iloc[-1] == 'Buy' and prev_signal == 'Hold') else dfc.stop_limit.iloc[-1]
            db.update_position(name, dfc['signal'][-1], stop)
            print(buy_signal.post_signal())
            print(f"Buy Signal for {name}")
        elif dfc.signal.iloc[-1] == 'Sell' and prev_signal != 'Sell':
            sell_signal = Signal(name, exchange, ins_type, dfc.minute.iloc[-1], dfc.close.iloc[-1], 'sell')
            stop = (dfc.close.iloc[-1] + dfc.ATR.iloc[-1])*-1 if (dfc.prev_signal.iloc[-1] == 'Sell' and prev_signal == 'Hold') else dfc.stop_limit.iloc[-1]
            db.update_position(name, dfc['signal'][-1], stop)
            print(sell_signal.post_signal())
            print(f"Sell Signal for {name}")
        elif (dfc.signal.iloc[-1] == 'Buy' or dfc.signal.iloc[-1] == 'Sell') and dfc.signal.iloc[-1] == prev_signal and prev_stop == 0:
            db.update_position(name, 'Hold', dfc.stop_limit.iloc[-1])
        else:
            psignal, stop = db.fetch_position(name)
            SL_trigger(stop, dfc.close.iloc[-1], name, exchange, ins_type, now)
        try:
            dfc.to_csv('{}/data/{}.csv'.format(mainfolder, name))
        except:
            print(f'file writing failed for {name}')
        logger.info(f"{name} Scan Done for {dfc.minute.iloc[-1]} {ctfp} minute candle!")
        return f"{name} Scan Done for {dfc.minute.iloc[-1]} {ctfp} minute candle!"
    # except:
    #     eve = f"Failed scan for symbol {name}"
    #     logger.warning(eve)
    #     telegramer(eve)
    #     return f"Failed scan for symbol {name}"
  
def telegramer(messa):
    EQ_bot_token = config.EQ_bot_token
    chat_id = config.EQ_bot_chat_ids
    tele_url = f'https://api.telegram.org/bot{ EQ_bot_token }/sendMessage'
    payload = {'chat_id': chat_id, 'text': messa}
    requests.post(tele_url, data=payload)

