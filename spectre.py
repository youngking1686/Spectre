import datetime as dt
import time
import threading
import config, sys
import gc, logging
import brok_auth, brain, db_load
import concurrent.futures
from dbquery import Database
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

timeframe = 60 # in seconds ctf*60

def is_candle_tf(tf, now):
    minu = now.strftime("%H:%M:00")
    trt1 = minu.split(':')
    tr2 = ((int(trt1[0]) * 60) + (int(trt1[1])) /(tf))
    if tr2.is_integer():
        return True
    else:
        return False

def SL_trigger(SL, LTP, symbol):
    try:
        if SL<0: #short position
            if LTP > abs(SL):
                eve = f"Buy SL triggered for {symbol}"
                logger.info(eve)
                brain.telegramer(eve)
                return True
        elif SL>0: #long position
            if LTP < SL:
                eve = f"Sell SL triggered for {symbol}"
                logger.info(eve)
                brain.telegramer(eve)
                return True
        else:
            pass
        return False
    except:
        logger.warning(f"SL check missed for {symbol}")
        return False

def fetch_ltp(fyers, symbol, c):
    if c < 4:
        try:
            return float(fyers.quotes({"symbols":symbol})['d'][0]['v']['lp'])
        except TypeError as e:
            eve = "Glitch get quote"
            logger.warning(eve)
            c+=1
            time.sleep(0.1)
            fetch_ltp(fyers, symbol, c)
    else:
        eve = "Oops! Check the connection"
        brain.telegramer("Fetch LTP fail Stopped Spectre!")
        logger.error(eve)
        sys.exit(eve)
        
def scanner(fyers):
    start = time.time()
    db_list = db.fetch_all()
    symbol_list = [active for active in db_list if active[-2]]
    if not symbol_list:
        eve = "Spectre: There are no active instruments to Trade, STOPPING!!"
        logger.error(eve)
        brain.telegramer(eve)
        sys.exit(eve)
    now = dt.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    tradable_symbols = []
    resp = []
    for out in symbol_list:
        symbol, name, exchange, ins_type, ctfp, start_time, end_time, trade, stop_limit = \
        out[1], out[2], out[3], out[4], out[7], out[9], out[10], out[11], out[12]
        ltp = fetch_ltp(fyers, symbol, 0)
        if current_time > end_time or SL_trigger(stop_limit, ltp, symbol):
            print(f"Exiting for the day for {symbol}")
            db.update_trade(name, False)
            payload = brain.getJsonStructure(name, exchange, ins_type, current_time, ltp, 'exit_one')
            print(brain.post_signal(payload))
        elif current_time > start_time and current_time < end_time and is_candle_tf(ctfp, now):
            tradable_symbols.append(out)
        elif current_time < start_time and trade:
            eve = f"Waiting till {start_time} to start {symbol}"
            print(eve)
            logger.info(eve)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(brain.T_T, fyers, out[1], out[2], out[3], out[4], out[5], out[6], out[7], out[8], \
                    out[9], out[10]) for out in tradable_symbols]
        for f in concurrent.futures.as_completed(results):
            new = f.result()
            resp.append(new)
    try:
        if resp:
            print(resp)
        else:
            print("Waiting for next scan")
    except:
        pass
    gc.collect()
    interval = timeframe - (time.time() - start)
    T = threading.Timer(interval, scanner, args=[fyers])
    T.start()
        
if __name__ == '__main__':
    fyers = brok_auth.fyers_login()
    db_load.data_load()
    pa_webhooks = config.webhooks
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #only for windows
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy()) #For linux CHANGE before moving the code!
    eve = asyncio.run(brok_auth.pa_clients_login(pa_webhooks))
    print(eve)
    logger.info(eve)
    brain.telegramer(eve)
    interval = timeframe - dt.datetime.now().second
    print(f"Wait for {interval} seconds To start")
    time.sleep(interval)
    scanner(fyers)