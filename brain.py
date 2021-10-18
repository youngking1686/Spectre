import pandas as pd
import numpy as np
import talib as ta
from datetime import date, timedelta
import datetime as dt
import config
import json, requests, logging
from dbquery import Database
import aiohttp
import asyncio

mainfolder = config.mainfolder
db = Database('{}/app.db'.format(mainfolder))

trd_date = dt.datetime.today().strftime('%d_%m_%Y')
LOG_FORMAT = "%(levelname)s %(asctime)s %(name)s- %(message)s"
logging.basicConfig(filename='{}/logs/trade_day_{}.log'.format(mainfolder, trd_date),
                    level = logging.DEBUG,
                    format= LOG_FORMAT,
                    filemode = 'a')
logger = logging.getLogger(__name__)

current_positions =[]
def validate_signal(name, side):
    if (name, side) in current_positions:
        return False
    else:
        if side == 'Buy' and (name, 'Sell') in current_positions:
            current_positions.remove((name,'Sell'))
        elif side == 'Sell' and (name, 'Buy') in current_positions:
            current_positions.remove((name,'Buy'))
        current_positions.append((name,side))
        return True

def fetch(fyers, symbol, timeframe):
    try:
        current_date = date.today()
        tommorrow = current_date + timedelta(days = 1)
        yesterday = current_date - timedelta(days = 4)
        data = {"symbol":symbol,"resolution":timeframe,"date_format":"1","range_from":yesterday,"range_to":tommorrow,"cont_flag":"1"}
        resp = fyers.history(data)
        candleinfo = resp['candles']
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(candleinfo, columns=columns)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s') + pd.Timedelta('05:30:00') #Adding 5:30hrs manually to UTC for VC
        df['minute'] = df.datetime.dt.time.map(str)
        df = df.set_index(pd.DatetimeIndex(df["datetime"]))
        df = df.drop(columns = ['timestamp'])
        return df
    except:
        print(f"Failed fetch for symbol {symbol}")
        logger.warning(f"Failed fetch for symbol {symbol}")

def change_timeframe(data, timeframe): 
    ohlc = {
        'datetime':'first',
        'open': 'first',
        'close': 'last'
    }
    df = data.resample(timeframe).apply(ohlc)
    df_tf = df.dropna()
    return df_tf

def P_T(df, timeframe, length):
    dft = change_timeframe(df, timeframe)   
    tr = (dft.close - dft.open)/dft.close
    dft['trn'] = ta.SMA(tr, length) #!!!!
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
        'ltrn':'last'
    }
    df = data.resample(timeframe).apply(ohlc)
    df_tf = df.dropna()
    return df_tf

def getJsonStructure(name, exchange, ins_type, time, price, action):
    dic = {"passphrase": "itsmebro",
                "time": time,
                "tkr": name,
                "ins_type": ins_type,
                "exng": exchange,
                "strategy": {
                "order_contracts": 0,
                "order_price": price,
                "trigger_price": 0,
                "order_action": action,
                "order_type": "Market",
                "product_type": "MIS"}}
    return json.dumps(dic)

async def post_man(session, url, payload):
    async with session.post(url, data=payload) as resp:
        resps = await resp.json()
        return resps['message']

async def post_signal(payload):
    async with aiohttp.ClientSession() as session:
        tasks = []
        pos_urls = config.webhooks
        for webhook in pos_urls:
            login_url = webhook + '/pa_webhook'
            tasks.append(asyncio.ensure_future(post_man(session, login_url, payload)))
        all_resps = await asyncio.gather(*tasks)
        logger.info(all_resps)
        return all_resps
        # for resp in all_resps:
        #     print(resp)
        #     logger.info(resp)

def T_T(fyers, symbol, name, exchange, ins_type, stfp, ltfp, ctfp, length, start_time, end_time, a):
    if a < 4:
        try:
            stf = str(stfp) + 'min'
            ltf = str(ltfp) + 'min'
            ctf = str(ctfp) + 'min'
            df1 = fetch(fyers, symbol, 1)
            dfS = P_T(df1, stf, length)
            dfL = P_T(df1, ltf, length)
            df1.loc[df1.datetime.isin(dfS.datetime),'strn'] = dfS['trn']
            df1.loc[df1.datetime.isin(dfL.datetime),'ltrn'] = dfL['trn']
            df1.strn.ffill(inplace=True)
            df1.ltrn.ffill(inplace=True)
            df1.strn.fillna(0, inplace=True)
            df1.ltrn.fillna(0, inplace=True)
            df1.loc[(df1.strn > 0) & (df1.ltrn > 0), 'Trend'] = 'Uptrend'
            df1.loc[(df1.strn < 0) & (df1.ltrn < 0), 'Trend'] = 'Downtrend'
            df1.loc[((df1.strn > 0) & (df1.ltrn < 0)) | ((df1.strn < 0) & (df1.ltrn > 0)) , 'Trend'] = 'Sideways'
            df1.Trend.fillna('Sideways', inplace=True)
            dfc = current_timeframe(df1, ctf)
            dfc['bar_len'] = (dfc.high-dfc.low).rolling(int(length/1.5)).mean().round(2)
            dfc.bar_len.fillna(0, inplace=True)
            dfc.loc[(dfc['Trend'] == 'Uptrend'), 'signal'] = 'Buy'
            dfc.loc[(dfc['Trend'] == 'Downtrend'), 'signal'] = 'Sell'
            dfc.signal.ffill(inplace=True)
            dfc['signal'].replace('', np.nan, inplace=True)
            dfc.dropna(subset=['signal'], inplace=True)
            dfc.loc[(dfc['minute'] < start_time), 'signal'] = 'Wait'
            dfc.loc[(dfc['minute'] > end_time), 'signal'] = 'Eod'
            dfc['prev_signal'] = dfc.signal.shift(periods=1)
            dfc.prev_signal.fillna('Wait', inplace=True)
            dfc.loc[(((dfc['signal'] == 'Buy') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                    ((dfc['signal'] == 'Buy') & (dfc.prev_signal == 'Sell'))), 'buy_entry'] = dfc.close
            dfc.loc[(((dfc['signal'] == 'Sell') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                    ((dfc['signal'] == 'Sell') & (dfc.prev_signal == 'Buy'))), 'buy_entry'] = 0
            dfc.loc[(((dfc['signal'] == 'Sell') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                    ((dfc['signal'] == 'Sell') & (dfc.prev_signal == 'Buy'))), 'sell_entry'] = dfc.close
            dfc.loc[(((dfc['signal'] == 'Buy') & ((dfc.prev_signal == 'Wait') | (dfc.prev_signal == 'Eod'))) | \
                    ((dfc['signal'] == 'Buy') & (dfc.prev_signal == 'Sell'))), 'sell_entry'] = 0
            dfc.buy_entry.ffill(inplace=True)
            dfc.sell_entry.ffill(inplace=True)
            conditions = [(((dfc.signal == 'Buy') & (dfc.prev_signal == 'Buy')) | ((dfc.signal == 'Buy') & (dfc.prev_signal != 'Buy'))), 
                            (((dfc.signal == 'Sell') & (dfc.prev_signal == 'Sell')) | ((dfc.signal == 'Sell') & (dfc.prev_signal != 'Sell')))]
            choices = [(dfc.buy_entry - (1.5*dfc.bar_len.astype(int))), (-1*(dfc.sell_entry + (1.5*dfc.bar_len.astype(int))))]
            dfc['stop_limit'] = np.select(conditions, choices, default=0)
            db.update_stop_limit(name, dfc.stop_limit.iloc[-1])
            dfc.to_csv('data/{}.csv'.format(name))
            if dfc.signal.iloc[-1] == 'Buy' and dfc.prev_signal.iloc[-1] != 'Buy':
                payload = getJsonStructure(name, exchange, ins_type, dfc.minute.iloc[-1], dfc.close.iloc[-1], 'buy')
                print(f"Buy Signal for {name}")
            elif dfc.signal.iloc[-1] == 'Sell' and dfc.prev_signal.iloc[-1] != 'Sell':
                payload = getJsonStructure(name, exchange, ins_type, dfc.minute.iloc[-1], dfc.close.iloc[-1], 'sell')
                print(f"Sell Signal for {name}")
            # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #Only for windows
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy()) #For linux
            print(asyncio.run(post_signal(payload)))
            logger.info(f"{name} Scan Done for {dfc.minute.iloc[-1]} {ctfp} minute candle!")
            return f"{name} Scan Done for {dfc.minute.iloc[-1]} {ctfp} minute candle!"
        except:
            a += 1
            print(f"Glitch Scanning {name}")
            logger.warning(f"Glitch Scanning {name}")
            T_T(fyers, symbol, name, ins_type, stfp, ltfp, ctfp, length, start_time, end_time, a)
    else:
        eve = f"Failed scan for symbol {name}"
        logger.warning(eve)
        telegramer(eve)
        return f"Failed scan for symbol {name}"

def telegramer(messa):
    EQ_bot_token = config.EQ_bot_token
    chat_id = config.EQ_bot_chat_ids
    tele_url = f'https://api.telegram.org/bot{ EQ_bot_token }/sendMessage'
    payload = {'chat_id': chat_id, 'text': messa}
    requests.post(tele_url, data=payload)
