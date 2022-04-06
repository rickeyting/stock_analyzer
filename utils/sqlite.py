import sqlite3
import os
from sqlite3 import Error
import pandas as pd
root = os.path.abspath('..')
db_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_data.db'

class database():
    def __init__(self,db_dir):
        self.db_path = db_dir
        self.con = sqlite3.connect(self.db_path)
        self.obj = self.con.cursor()

    def insert_data(self, df, table_name):
        if table_name == 'stock_id':
            df.to_sql(name=table_name, con=self.con, index=False, if_exists='replace')
        else:
            df.to_sql(name=table_name, con=self.con, index=False, if_exists='append')

    def check(self, table_name, col, value):
        try:
            self.obj.execute("SELECT * FROM {} where {} = {}".format(table_name, col, value))
            if self.obj.fetchall():
                return True
            else:
                return False
        except Exception as e:
            print(e)
    
    def undo(self, table_name, condition, where=''):
        where += 'WHERE '
        for i in condition:
            where += str(i[0])+' = '+str(i[1])
            if condition.index(i) == len(condition)-1:
                pass
            else:
                where += ' AND '
        if self.table_check(table_name):
            self.obj.execute("SELECT * FROM {} {}".format(table_name, where))
            if self.obj.fetchone():
                return False
            else:
                return True
        else:
            return True
    
    def get_stock_id(self):
        self.obj.execute("SELECT stock_id FROM stock_id")
        return [item[0] for item in self.obj.fetchall()]
    
    def table_check(self, table_name):
        self.obj.execute("SELECT name FROM sqlite_master WHERE type='table'")
        #print([item[0] for item in self.obj.fetchall()])
        if self.obj.fetchone() and table_name in [item[0] for item in self.obj.fetchall()]:
            return True
        else:
            return False

    def close(self):
        self.con.commit()
        self.con.close()


if __name__ == '__main__':
    #save_path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_datas\stock_id.csv'
    #df = pd.read_csv(save_path)
    db = database(db_dir)
    #db.create_table('stock_id')
    #db.insert_data(df, 'stock_id')
    db.table_check('stock_id')
    #print(db.check('stock_id', 'stock_id', 1101))
