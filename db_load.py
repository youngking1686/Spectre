from dbquery import Database
import config, brain
import pandas as pd

mainfolder = config.mainfolder
db = Database('{}/app.db'.format(mainfolder))

def format_fix(start_time):
    return start_time.replace("9", "09") if start_time.split(':')[0] == '9' else start_time

def data_load():
    df = pd.read_csv('{}/symbol_load.csv'.format(mainfolder))
    for i in range(len(df)):
        try:
            db.insert(df.name[i], df.exchange[i], df.ins_type[i], int(df.stfp[i]), int(df.ltfp[i]), \
                int(df.ctfp[i]), int(df.lenght[i]), format_fix(df.start_time[i]), df.end_time[i], int(df.trade[i]), int(df.stop_limit[i]))
        except:
            db.update_settings(df.name[i], int(df.stfp[i]), int(df.ltfp[i]), int(df.ctfp[i]), \
                int(df.lenght[i]), format_fix(df.start_time[i]), df.end_time[i], int(df.trade[i]), int(df.stop_limit[i]))
            print(f"Updated for {df.name[i]}")
    print("DB update completed")
    brain.telegramer("DB update completed")
        
data_load()