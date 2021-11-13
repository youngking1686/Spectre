import sys, os

if getattr(sys, 'frozen', False):
    mainfolder = sys._MEIPASS
else:
    mainfolder = os.path.dirname(os.path.abspath(__file__))

user_detail = { 'fyers_client_id': 'UH471HHHJP-100',
                'fyers_app_id': 'UH471HHHJP',
                'fyers_secret_key': 'SIZA82MRSB',
                'fyers_id': 'XP02781',
                'password': 'Fyer@hero3',
                'pan_dob': '27-01-1987',
                'redirect_uri': "http://127.0.0.1:5000/login"}

webhooks = ['https://pooranan.pythonanywhere.com'] #['https://eaglequant.pythonanywhere.com', 'https://pooranan.pythonanywhere.com']

# webhooks = ['http://127.0.0.1:5000']

#Telegram
EQ_bot_token = "1555749245:AAHPha-GOQZvCfmH-SgIydwEiyu_-JGdsto"
EQ_bot_chat_ids = '1276604461'