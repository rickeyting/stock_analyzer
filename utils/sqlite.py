import sqlite3
import os
from sqlite3 import Error
import pandas as pd
root = os.path.abspath('..')

class database():
    def __init__(self):
        self.db_path = os.path.join(root, 'raw_datas.db')
        self.con = sqlite3.connect(self.db_path)
        self.obj = self.con.cursor()

    def insert_data(self, df, table_name):
        if table_name == 'stock_id':
            df.to_sql(name=table_name, con=self.con, index=False, if_exists='replace')
        else:
            df.to_sql(name=table_name, con=self.con, index=False, if_exists='append')
        self.con.commit()
        self.con.close()

    def check(self, table_name, col, value):
        try:
            self.obj.execute("SELECT * FROM {} where {} = {}".format(table_name, col, value))
            if self.obj.fetchall():
                return True
            else:
                return False
        except Exception as e:
            print(e)

    #def get_stock_id(self):


#save_path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_datas\stock_id.csv'
#df = pd.read_csv(save_path)
db = database()
#db.create_table('stock_id')
#db.insert_data(df, 'stock_id')
print(db.check('stock_id', 'stock_id', 1101))