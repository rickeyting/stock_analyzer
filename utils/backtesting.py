from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from utils.postgredb import get_data


def trace_back(predict, stock_id, limit=0.03, days=365, banks=False):
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    start_data = datetime.strftime(datetime.now() - timedelta(days=decrease + days), '%Y-%m-%d')
    query = "SELECT date, close FROM stock_price WHERE date >= '{}' AND stock_id = '{}'".format(start_data, stock_id)
    df = get_data(query)
    df = df[['date', 'close']]
    df = df.sort_values(by='date')
    df.close = df.close.shift(-1)
    predict = predict.rename(columns={'buy': 'date'})
    df = df.rename(columns={'close': 'buy_price'})
    predict = predict.merge(df, on='date', how='left')
    predict = predict.rename(columns={'date': 'buy', 'sell': 'date'})
    df = df.rename(columns={'buy_price': 'sell_price'})
    predict = predict.merge(df, on='date', how='left')
    result = predict.rename(columns={'date': 'sell'})
    result['rate'] = (result.sell_price - result.buy_price) / result.buy_price
    if banks:
        banks = list(dict.fromkeys(result.bank.values.tolist()))
        bank_result = []
        for i in banks:
            earn = len(result.loc[(result['rate'] >= limit) & (result.bank == i)])
            loss = len(result.loc[(result['rate'] <= limit * -1) & (result.bank == i)])
            neural = len(result.loc[(result['rate'] > limit * -1) & (result['rate'] < limit) & (result.bank == i)])
            bank_result.append([i, stock_id, earn, neural, loss])
        bank_result = pd.DataFrame(np.array(bank_result), columns=['bank', 'stock_id', 'earn', 'neural', 'loss'])
        return bank_result
    else:
        earn = len(result.loc[result['rate'] >= limit])
        loss = len(result.loc[result['rate'] <= limit*-1])
        neural = len(result.loc[(result['rate'] > limit*-1) & (result['rate'] < limit)])
    return [earn, neural, loss]


'''
def confusion_matrix(df):
    TP = len(df.loc[(df.predicted == df.actual) & (df.predicted == 'buy')])
    FP = len(df.loc[(df.actual == 'sell') & (df.predicted == 'buy')])
    FN = len(df.loc[(df.actual == 'buy') & (df.predicted == 'sell')])
    TN = len(df.loc[(df.predicted == df.actual) & (df.predicted == 'sell')])
    Tneutral = len(df.loc[(df.actual == 'neutral') & (df.predicted == 'buy')])
    Fneutral = len(df.loc[(df.actual == 'neutral') & (df.predicted == 'sell')])
    #buy_ACC = TP / len(df.loc[df.predicted == 'buy'])
    #over_all_ACC = (TP+TN) / (TP+FP+FN+TN)
    result = [TP, FP, FN, TN, Tneutral, Fneutral]
    return result


test = pd.DataFrame({'buy': ['2022-04-01'], 'sell': ['2022-04-08']})
print(test)
trace_back(test,'1101')
'''