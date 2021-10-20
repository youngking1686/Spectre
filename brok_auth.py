from fyers_api import fyersModel
from fyers_api import accessToken
import datetime as dt
import config, brain, sys
import dill as pickle
import requests, json
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
        self.redirect_uri = config.user_detail['redirect_uri']
        self.response_type = "code"
        self.grant_type = "authorization_code"
        self.state = "private"
        self.nonce = "private"
    
    def authenticate(self):
        try:
            inpu = {"fyers_id": config.user_detail['fyers_id'], "password": config.user_detail['password'], "pan_dob": config.user_detail['pan_dob'], 
                    "app_id": config.user_detail['fyers_app_id'], "redirect_uri": config.user_detail['redirect_uri'], "appType": "100", "code_challenge": "", 
                    "state": "private", "scope": "", "nonce": "private", "response_type": "code", "create_cookie": "true"}
            session = accessToken.SessionModel(client_id=self.client_id, secret_key=self.secret_key,\
                redirect_uri=self.redirect_uri, response_type=self.response_type, grant_type=self.grant_type, state=self.state, nonce=self.nonce)
            response = session.generate_authcode()
            # print("Response from Try-Catch 1 is - \n", response)
            headers = {
                    "accept": "*/*",
                    "accept-language": "en-IN,en-US;q=0.9,en;q=0.8",
                    "content-type": "application/json; charset=UTF-8",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "referrer": response
            }
            result = requests.post("https://api.fyers.in/api/v2/token", headers=headers, json=inpu, allow_redirects=True)
            var = json.loads(result.content)
            URL = var["Url"]
            parsed = urlparse(URL)
            parsedlist = parse_qs(parsed.query)['auth_code']
            auth_code = parsedlist[0]
            session.set_token(auth_code)
            response = session.generate_token()
            access_token = response["access_token"]
            #INSERT YOUR DESKTOP PATH FOR LOG FILE CREATION
            fyers = fyersModel.FyersModel(client_id=self.client_id, token=access_token, log_path="{}/logs/".format(mainfolder)) 
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
        resps = await resp.json()
        return resps['message']

async def pa_clients_login(pa_webhooks):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for webhook in pa_webhooks:
            login_url = webhook + '/alice_login'
            tasks.append(asyncio.ensure_future(alice_login(session, login_url)))
        all_resps = await asyncio.gather(*tasks)
        messa = 'Spectre' + ": ".join(all_resps)
        return messa