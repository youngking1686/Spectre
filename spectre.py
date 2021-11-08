import datetime as dt
import time
import threading
import config, sys, os
import gc, logging
import brok_auth, brain, db_load
import concurrent.futures
from dbquery import Database
import asyncio, requests

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

def pa_check():
    urls = config.webhooks
    for url in urls:
        url1 = url + '/pa_check'
        resp = requests.post(url1, data=None)
        if resp.content==b'ok':
            continue
        elif resp.content==b'error':
            url = url + '/alice_login'
            resp = requests.post(url, data=None)
            resps = resp.json()
            eve = resps['message'], 'Login Refreshed'
            print(eve)
            logger.warning(eve)
            brain.telegramer(url)
        continue 

def is_candle_tf(tf, now):
    minu = now.strftime("%H:%M:00")
    trt1 = minu.split(':')
    tr2 = ((int(trt1[0]) * 60) + (int(trt1[1])) /(tf))
    if tr2.is_integer():
        return True
    else:
        return False
             
def scanner(fyers):
    start = time.time()
    db_list = db.fetch_all()
    symbol_list = [active for active in db_list if active[-3]]
    if not symbol_list:
        eve = "Spectre: There are no active instruments to Trade, STOPPING!!"
        os.remove('{}/temp/fyers.obj'.format(mainfolder))
        logger.error(eve)
        brain.telegramer(eve)
        sys.exit(eve)
    now = dt.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    tradable_symbols = []
    resp = []
    for out in symbol_list:
        symbol, name, exchange, ins_type, ctfp, start_time, end_time, trade, stop_limit = \
        out[1], out[2], out[3], out[4], out[7], out[10], out[11], out[12], out[13]
        is_ctf = is_candle_tf(ctfp, now)
        ltp = brain.fetch_ltp(fyers, symbol, 0)
        if current_time > end_time and trade:
            print(f"Exiting poisition for {symbol}")
            brain.exit_one(name, exchange, ins_type, current_time, ltp)
            db.update_trade(name, False)
        elif not is_ctf:
            brain.SL_trigger(stop_limit, ltp, name, exchange, ins_type, current_time)
        if current_time > start_time and current_time < end_time and is_ctf:
            tradable_symbols.append(out)
        elif current_time < start_time and trade:
            eve = f"Waiting till {start_time} to start {symbol}"
            print(eve)
            logger.info(eve)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(brain.T_T, fyers, out[1], out[2], out[3], out[4], out[5], out[6], out[7], out[8], \
                    out[9], out[10], out[11]) for out in tradable_symbols]
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
    pa_check()
    interval = timeframe - (time.time() - start)
    T = threading.Timer(interval, scanner, args=[fyers])
    T.start()
        
if __name__ == '__main__':
    fyers = brok_auth.fyers_login().authenticate()
    db_load.data_load()
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #only for windows
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy()) #For linux CHANGE before moving the code!
    eve = asyncio.run(brok_auth.pa_clients_login())
    print(eve)
    logger.info(eve)
    brain.telegramer(eve)
    interval = timeframe - dt.datetime.now().second
    print(f"Wait for {interval} seconds To start")
    time.sleep(interval)
    scanner(fyers)