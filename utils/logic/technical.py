import pandas as pd
import talib

from utils.postgredb import get_data
from datetime import datetime, timedelta



def TaiwanStockKDJ(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['MinLow'] = df['min'].rolling(9, min_periods=9).min()
    df['MaxHigh'] = df['max'].rolling(9, min_periods=9).max()
    df['RSV'] = (df['close'] - df['MinLow']) / (df['MaxHigh'] - df['MinLow']) * 100
    df["K"] = df['RSV'].ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]
    result = df[['date', 'K', 'D', 'J', 'RSV']]
    return result


def TaiwanStockMACD(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['di'] = (df['max']+df['min']+2*df.close)/4
    df['ema12'] = df['di'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['di'].ewm(span=26, adjust=False).mean()
    df['dif'] = df['ema12'] - df['ema26']
    df['MACD'] = df['dif'].ewm(span=9, adjust=False).mean()
    result = df[['date', 'ema12', 'ema26', 'dif', 'MACD']]
    return result


def TaiwanStockRSI(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['spread'] = df['close'].diff()
    df['up'] = df['spread'].clip(lower=0)
    df['down'] = -1 * df['spread'].clip(upper=0)
    df['up'] = df['up'].ewm(com=5 - 1, adjust=True, min_periods=5).mean()
    df['down'] = df['down'].ewm(com=5 - 1, adjust=True, min_periods=5).mean()
    df['RSI5'] = df['up'] / df['down']
    df['RSI5'] = 100 - (100/(1+df['RSI5']))

    df['up'] = df['spread'].clip(lower=0)
    df['down'] = -1 * df['spread'].clip(upper=0)
    df['up'] = df['up'].ewm(com=10 - 1, adjust=True, min_periods=10).mean()
    df['down'] = df['down'].ewm(com=10 - 1, adjust=True, min_periods=10).mean()
    df['RSI10'] = df['up'] / df['down']
    df['RSI10'] = 100 - (100/(1+df['RSI10']))

    result = df[['date', 'spread', 'RSI5', 'RSI10']]
    return result


def TaiwanStockBIAS(stock_id, timeperiod, days=120):
    df = get_price_info(stock_id, days)
    df['mean'] = df['close'].rolling(timeperiod, min_periods=timeperiod).mean()
    df['ans'] = (df['close'] - df['mean']) / df['mean'] * 100
    result = df[['date', 'ans']]
    return result


def TaiwanStockWIL(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['highest'] = df['max'].rolling(9, min_periods=9).max()
    df['lowest'] = df['min'].rolling(9, min_periods=9).min()
    df['Williams'] = (df['highest'] - df['close']) / (df['highest'] - df['lowest']) * 100
    result = df[['date', 'Williams']]
    return result


def TaiwanStockBBI(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['mean3'] = df['close'].rolling(3, min_periods=3).mean()
    df['mean6'] = df['close'].rolling(6, min_periods=6).mean()
    df['mean12'] = df['close'].rolling(12, min_periods=12).mean()
    df['mean24'] = df['close'].rolling(24, min_periods=24).mean()
    df['BBI'] = (df['mean3'] + df['mean6'] + df['mean12'] + df['mean24'])/4
    result = df[['date', 'BBI']]
    return result


def TaiwanStockCDP(stock_id, days=120):
    df = get_price_info(stock_id, days)
    df['CDP'] = (df['close'] * 2 + df['max'] + df['min'])/4
    df['AH'] = df['CDP'] + df['max'] - df['min']
    df['NH'] = df['CDP'] * 2 - df['min']
    df['NL'] = df['CDP'] * 2 - df['max']
    df['AL'] = df['CDP'] - df['max'] + df['min']
    result = df[['date', 'CDP', 'AH', 'NH', 'NL', 'AL']]
    return result


def get_price_info(stock_id, days):
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    start_data = datetime.strftime(datetime.now() - timedelta(days=decrease + days), '%Y-%m-%d')
    query = "SELECT * FROM stock_price WHERE date >= '{}' AND stock_id = '{}'".format(start_data, stock_id)
    df = get_data(query)
    result = df.sort_values(by='date')
    return result


def technical_analysis(t_type, period, stock_id, days=365):
    df = get_price_info(stock_id, days)
    if t_type == 'RSI':
        rsi = talib.RSI(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': rsi})
        return result
    if t_type == 'BIAS':
        return TaiwanStockBIAS(stock_id, timeperiod=period)




if __name__ == '__main__':
    a = technical_analysis('STOCH', 5, '1101')
    print(TaiwanStockKDJ('1101'))
    print(a)