import os
from datetime import datetime

import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm

from utils.logic.technical import technical_analysis
from utils.postgredb import db_get_stock_id, db_insert_data, get_data
from utils.backtesting import trace_back
from utils.logic.western_electric import western_electric_test

BASE_YAML = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\utils\logic\base.yaml'
TABLE_LIST = os.path.join('..', 'table_list.yaml')


class BaseResult:
    def __init__(self, base_yaml=BASE_YAML, table_yaml=TABLE_LIST):
        self.table_yaml = table_yaml
        self.base_yaml = base_yaml
        self.stock_id_list = db_get_stock_id(market_type='sii')
        with open(self.base_yaml, 'r', encoding='utf-8-sig') as file:
            self.technical_list = yaml.safe_load(file)
        self.allin = pd.DataFrame()
        self.current = pd.DataFrame([[i]for i in self.stock_id_list],columns=['stock_id'])

    def insert_db(self, result, label):
        date = datetime.today().strftime('%Y-%m-%d')
        result = [label, date] + result
        result = pd.DataFrame([result], columns=['function', 'date', 'earn', 'neutral', 'loss'])
        db_insert_data('analyze_technical', result, table_yaml=self.table_yaml)

    def positive(self, recheck=True, daily_update=True):
        type_list = self.technical_list['POSITIVE']
        for function in type_list:
            types = type_list[function]
            for label in types:
                result = []
                status = []
                for i in tqdm(self.stock_id_list):
                    df = technical_analysis(function, type_list[function][label], i)
                    df, current_status = western_electric_test(df, 'ans', daily_update=daily_update)
                    if current_status == 'up':
                        status.append([i, 'buy'])
                    elif current_status == 'down':
                        status.append([i, 'sell'])
                    else:
                        status.append([i, 'neutral'])
                    df = df.rename(columns={'up': 'buy', 'down': 'sell'})
                    df['sell'] = df['sell'].bfill(axis='rows')
                    if recheck:
                        test = df.dropna(subset=['buy', 'sell'], how='all').copy()
                        test['stock_id'] = i
                        test['label'] = label
                        self.allin = pd.concat([self.allin, test])
                    df = df.dropna()
                    result.append(trace_back(df, i))
                result = [sum(x) for x in zip(*result)]
                self.insert_db(result, label)
                self.current = self.current.merge(pd.DataFrame(status, columns=['stock_id', label]), on='stock_id',
                                                  how='left')

    def negative(self, recheck=True, daily_update=True):
        type_list = self.technical_list['NEGATIVE']
        for function in type_list:
            types = type_list[function]
            for label in types:
                result = []
                status = []
                for i in tqdm(self.stock_id_list):
                    df = technical_analysis(function, type_list[function][label], i)
                    df, current_status = western_electric_test(df, 'ans', daily_update=daily_update)
                    if current_status == 'up':
                        status.append([i, 'sell'])
                    elif current_status == 'down':
                        status.append([i, 'buy'])
                    else:
                        status.append([i, 'neutral'])
                    df = df.rename(columns={'up': 'sell', 'down': 'buy'})
                    if recheck:
                        test = df.dropna(subset=['buy', 'sell'], how='all').copy()
                        test['stock_id'] = i
                        test['label'] = label
                        self.allin = pd.concat([self.allin, test])
                    df['sell'] = df['sell'].bfill(axis='rows')
                    df = df.dropna()
                    result.append(trace_back(df, i))
                result = [sum(x) for x in zip(*result)]
                self.insert_db(result, label)
                self.current = self.current.merge(pd.DataFrame(status, columns=['stock_id', label]), on='stock_id',
                                                  how='left')

    def allin_result(self):
        self.current.to_csv(r'C:\Users\mick7\Downloads\technical.csv')
        return self.allin


if __name__ == '__main__':
    a = BaseResult()
    a.positive(daily_update=True)
    a.negative(daily_update=True)
    result = a.allin_result()
    #result.to_csv(r'C:\Users\mick7\Downloads\base.csv')




