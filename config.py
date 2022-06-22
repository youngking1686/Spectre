import sys, os

if getattr(sys, 'frozen', False):
    mainfolder = sys._MEIPASS
else:
    mainfolder = os.path.dirname(os.path.abspath(__file__))

user_detail = { 'fyers_client_id': '',
                'fyers_app_id': '',
                'fyers_secret_key': '',
                'fyers_userid': '',
                'password': '',
                'pan_dob': '',
                'pin':'',
                'redirect_uri': "http://127.0.0.1:5000/login"}

webhooks = ['https://pa-ks1.herokuapp.com'] #['https://eaglequant.pythonanywhere.com', 'https://pooranan.pythonanywhere.com']

# https://pa-ks1.herokuapp.com/ks_webhook
# webhooks = ['http://127.0.0.1:5000']

#Telegram
EQ_bot_token = ""
EQ_bot_chat_ids = ['']
