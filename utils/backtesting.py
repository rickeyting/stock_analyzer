from datetime import datetime, timedelta

from matplotlib import pyplot as plt

from utils.postgredb import get_data


def trace_back(predict, stock_id, limit=0.03, periods=7, days=365):
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    start_data = datetime.strftime(datetime.now() - timedelta(days=decrease + days), '%Y-%m-%d')
    query = "SELECT * FROM stock_price WHERE date >= '{}' AND stock_id = '{}'".format(start_data, stock_id)
    df = get_data(query)
    df = df.sort_values(by='date', ascending=False)
    df['buy'] = df['close'].shift(periods=1)
    df['max'] = df['max'].shift(periods=1)
    df['min'] = df['min'].shift(periods=1)
    df['up'] = df['max'].rolling(periods, min_periods=periods).max()
    df['down'] = df['min'].rolling(periods, min_periods=periods).min()
    df['sell'] = (df['up'] + df['down']) / 2
    df['earn'] = (df['sell'] - df['buy']) / df['buy']
    df = df.sort_values(by='date')
    df.loc[df['earn'] >= limit, 'actual'] = 'buy'
    df.loc[df['earn'] <= limit*-1, 'actual'] = 'sell'
    df['actual'] = df['actual'].fillna('neutral')
    df = df.dropna()
    result = df[['date', 'actual']]
    result = result.merge(predict, on=['date'], how='left') #predicted
    result = result.dropna()
    result = confusion_matrix(result)
    return result


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

