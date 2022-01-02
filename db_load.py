from dbquery import Database
import config, brain
import pandas as pd
import gspread

mainfolder = config.mainfolder
db = Database('{}/app.db'.format(mainfolder))

format_fix = lambda x: x.replace("9", "09") if x.split(':')[0] == '9' else x
trade = lambda x: 1 if x == 'TRUE' else 0

def data_load():
    gc = gspread.service_account(filename="{}/auth/optimus-321709-9d03ace9da0c.json".format(mainfolder))
    sh = gc.open("spectre_load")
    df = pd.DataFrame(sh.worksheet('settings').get_all_records())
    lis_names = df['name'].to_list()
    out = db.fetch2('SELECT name from symbols')
    out1 = list(zip(*out))
    if out1:
        for name in out1[0]:
            if name not in lis_names:
                print(f"{name} removed")
                db.remove(name)
            else:
                continue
    else:
        pass
    for i in range(len(df)):
        try:
            db.insert(df.name[i], df.exchange[i], df.ins_type[i], int(df.stfp[i]), int(df.ltfp[i]), int(df.ctfp[i]), \
                int(df.lenght[i]), format_fix(df.start_time[i]), df.end_time[i], trade(df.trade[i]), df.position[i], df.prev_tkr[i])
        except:
            db.update_settings(df.name[i], int(df.stfp[i]), int(df.ltfp[i]), int(df.ctfp[i]), int(df.lenght[i]), \
                format_fix(df.start_time[i]), df.end_time[i], trade(df.trade[i]), df.position[i], df.prev_tkr[i])
    print("DB update completed")
    brain.telegramer("DB update completed")
    
# data_load()