import pandas as pd
from utils.postgredb import get_data
from datetime import datetime, timedelta


def TaiwanStockKDJ(days):
    '''
    RSV = (close - lowest of days) / (highest of days - lowest of days) * 100
    K = past k * 2 / 3 + RSV * 1 / 3 ,default = 50
    D = past d * 2 / 3 + K * 1 / 3 ,default = 50
    J = ( 3 * d ) - ( 2 * k )
    '''
    weekday = datetime.today().weekday()
    sql = "SELECT * FROM stock_price WHERE date >= '2022-03-01'"
    df = get_data(sql)
    df['MinLow'] = df['min'].rolling(9, min_periods=9).min()
    df['MaxHigh'] = df['max'].rolling(9, min_periods=9).max()
    df['RSV'] = (df['close'] - df['MinLow']) / (df['MaxHigh'] - df['MinLow']) * 100
    df["K"] = df['RSV'].ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    #df.to_csv(r'C:\Users\mick7\PycharmProjects\New folder\test2.csv')

if __name__ == '__main__':
    TaiwanStockKDJ(321)