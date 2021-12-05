from fyers_api import fyersModel
from fyers_api import accessToken
import datetime as dt
import config, brain, sys
import dill as pickle
import requests
from urllib.parse import urlparse, parse_qs
import traceback, logging
import aiohttp
import asyncio

mainfolder = config.mainfolder

trd_date = dt.datetime.today().strftime('%d_%m_%Y')
LOG_FORMAT = "%(levelname)s %(asctime)s %(name)s- %(message)s"
logging.basicConfig(filename='{}/logs/trade_day_{}.log'.format(mainfolder, trd_date),
                    level = logging.DEBUG,
                    format= LOG_FORMAT,
                    filemode = 'a')
logger = logging.getLogger(__name__)

def read_pickle():
    readfile = open('{}/temp/fyers.obj'.format(mainfolder),'rb')
    fyers_obj = pickle.load(readfile)
    readfile.close()
    return fyers_obj

def update_pickle(fyers_obj):
    writefile = open('{}/temp/fyers.obj'.format(mainfolder),'wb')
    pickle.dump(fyers_obj,writefile)
    writefile.close()
    return fyers_obj

def market_check(fyers):
    market_status = fyers.market_status()['marketStatus'][6]['status'] #Checks commodities market exhc:12, segment:20
    if market_status == 'OPEN':
        eve = "Market is Open to Trade"
        logger.info(eve)
        brain.telegramer(eve)
        return "Market is Open to Trade"
    elif market_status == 'CLOSE':
        # cont = input("Market is closed. Do you want to continue? (y/n): ")
        # if cont == 'y':
        #     pass
        # else:
        eve = "Market is closed. Stopping the ALGO!!"
        logger.error(eve)
        brain.telegramer(eve)
        sys.exit(eve)

class fyers_login:
    def __init__(self):
        self.client_id = config.user_detail['fyers_client_id']
        self.secret_key = config.user_detail['fyers_secret_key']
        self.fyers_userid = config.user_detail['fyers_userid']
        self.password = config.user_detail['password']
        
        self.pan_dob = config.user_detail['pan_dob']
        self.pin = int(config.user_detail['pin'])
        self.app_id = config.user_detail['fyers_app_id']
        self.redirect_uri = config.user_detail['redirect_uri']
        self.response_type = "code"
        self.grant_type = "authorization_code"
        self.state = "private"
        self.nonce = "private"
    
    def authenticate(self):
        try:
            session = accessToken.SessionModel(client_id=self.client_id, secret_key=self.secret_key, redirect_uri=self.redirect_uri,
                                            response_type='code', grant_type='authorization_code')
            s = requests.Session()
            data1 = f'{{"fy_id":"{self.fyers_userid}","password":"{self.password}","app_id":"2","imei":"","recaptcha_token":""}}'
            r1 = s.post('https://api.fyers.in/vagator/v1/login', data=data1)
            request_key = r1.json()["request_key"]

            data2 = f'{{"request_key":"{request_key}","identity_type":"pin","identifier":"{self.pin}","recaptcha_token":""}}'
            r2 = s.post('https://api.fyers.in/vagator/v1/verify_pin', data=data2)

            headers = {
                'authorization': f"Bearer {r2.json()['data']['access_token']}",
                'content-type': 'application/json; charset=UTF-8'
            }
            data3 = f'{{"fyers_id":"{self.fyers_userid}","app_id":"{self.app_id}","redirect_uri":"{self.redirect_uri}","appType":"100","code_challenge":"","state":"abcdefg","scope":"","nonce":"","response_type":"code","create_cookie":true}}'
            r3 = s.post('https://api.fyers.in/api/v2/token', headers=headers, data=data3)
            parsed = urlparse(r3.json()['Url'])
            auth_code = parse_qs(parsed.query)['auth_code'][0]
            session.set_token(auth_code)
            response = session.generate_token()
            token = response["access_token"]
            fyers = fyersModel.FyersModel(client_id=self.client_id, token=token, log_path="{}/logs/".format(mainfolder))
            is_async = True
            print(market_check(fyers))
            eve = "Fyers Login Successfull!"
            print(eve)
            logger.info(eve)
            brain.telegramer(eve)
            return update_pickle(fyers)
        except:
            eve = f"Fyers Login Failed!! {traceback.print_exc()}"
            logger.error(eve)
            brain.telegramer(eve)
            sys.exit(eve)
            
async def alice_login(session, login_url):
    async with session.get(login_url) as resp:
        resps = await resp.json(content_type=None)
        return resps['message']

async def pa_clients_login():
    async with aiohttp.ClientSession() as session:
        pa_webhooks = config.webhooks
        tasks = []
        for webhook in pa_webhooks:
            login_url = webhook + '/alice_login'
            tasks.append(asyncio.ensure_future(alice_login(session, login_url)))
        all_resps = await asyncio.gather(*tasks)
        messa = 'Spectre: ' + ": ".join(all_resps)
        return messa