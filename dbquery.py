import sqlite3
import os
import config
import threading

mainfolder = config.mainfolder
lock = threading.Lock()

if not os.path.isfile('{}/app.db'.format(mainfolder)):
    conn = sqlite3.connect('{}/app.db'.format(mainfolder))
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL UNIQUE,
            exchange TEXT NOT NULL,
            ins_type TEXT NOT NULL,
            stfp INTEGER NOT NULL,
            ltfp INTEGER NOT NULL,
            ctfp INTEGER NOT NULL,
            length INTEGER NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            trade BOOLEAN NOT NULL,
            position TEXT NOT NULL,
            prev_tkr TEXT NOT NULL
        )
    """)
    conn.commit() #When ever a new column added check the spectre.py column maps
    
class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db, check_same_thread=False)
        self.cur = self.conn.cursor()
        self.conn.commit()

    def fetch(self, symbol):
        self.cur.execute("""SELECT * FROM symbols WHERE name = ?""", (symbol,))
        rows = self.cur.fetchall()
        return rows[0]
            
    def fetch_position(self, symbol):
        try:
            lock.acquire(True)
            self.cur.execute("""SELECT position FROM symbols WHERE name = ?""", (symbol,))
            rows = self.cur.fetchall()
            return rows[0][0]
        finally:
            lock.release()
    
    def fetch_prev_tkr(self, symbol):
        self.cur.execute("""SELECT prev_tkr, position FROM symbols WHERE name = ?""", (symbol,))
        rows = self.cur.fetchall()
        return rows[0][0], rows[0][1]
    
    def fetch_all(self):
        self.cur.execute("""SELECT * FROM symbols""")
        rows = self.cur.fetchall()
        return rows
    
    def fetch2(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def insert(self, name, exchange, ins_type, stfp, ltfp, ctfp, length, start_time, end_time, trade, position, prev_tkr):
        if ins_type == 'FUT':
            symbol = exchange + ':' + name + ins_type
        elif ins_type == 'EQ':
            symbol = exchange + ':' + name + '-' + ins_type
        elif ins_type == 'INDEX':
            symbol = exchange + ':' + name + '-' + ins_type
        self.cur.execute("""INSERT INTO symbols (symbol, name, exchange, ins_type, stfp, ltfp, ctfp, length,
                            start_time, end_time, trade, position, prev_tkr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                         (symbol, name, exchange, ins_type, stfp, ltfp, ctfp, length, start_time, end_time, trade, position, prev_tkr))
        self.conn.commit()

    def remove(self, name):
        self.cur.execute("""DELETE FROM symbols WHERE name=?""", (name,))
        self.conn.commit()

    def update_settings(self, name, stfp, ltfp, ctfp, length, start_time, end_time, trade, position, prev_tkr):
        self.cur.execute("""UPDATE symbols SET stfp = ?, ltfp = ?, ctfp = ?, length = ?, start_time = ?, end_time = ?, trade = ?, position = ?, prev_tkr = ?
                         WHERE name = ?""", (stfp, ltfp, ctfp, length, start_time, end_time, trade, position, prev_tkr, name))
        self.conn.commit()
        
    def update_trade(self, name, trade):
        self.cur.execute("""UPDATE symbols SET trade = ? WHERE name = ?""", (trade, name))
        self.conn.commit()
                        
    def update_position(self, name, position):
        try:
            lock.acquire(True)
            self.cur.execute("""UPDATE symbols SET position = ? WHERE name = ?""", (position, name))
            self.conn.commit()
        finally:
            lock.release()
            
    def update_prev_tkr(self, name, prev_tkr):
        try:
            lock.acquire(True)
            self.cur.execute("""UPDATE symbols SET prev_tkr = ? WHERE name = ?""", (prev_tkr, name))
            self.conn.commit()
        finally:
            lock.release()

    def __del__(self):
        self.conn.close()