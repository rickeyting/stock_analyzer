

def traing(predict, stock_id, limit=0.03, periods=7, days=365):
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    start_data = datetime.strftime(datetime.now() - timedelta(days=decrease + days), '%Y-%m-%d')
    print(start_data)
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
    df.loc[df['earn'] >= limit, 'actual'] = 'buy'
    df.loc[df['earn'] <= limit*-1, 'actual'] = 'sell'
    df = df.dropna()
    result = df[['stock_id', 'date', 'actual']]
    result = result.merge(predict, on=['stock_id', 'date'], how='left') #predicted
    result = df.dropna()
    return result


def confusion_matrix(df):
    TP = len(df.loc[df.predicted == df.actual and df.predicted == 'buy'])
    FP = len(df.loc[df.actual == 'sell' and df.predicted == 'buy'])
    FN = len(df.loc[df.actual == 'buy' and df.predicted == 'sell'])
    TN = len(df.loc[df.predicted == df.actual and df.predicted == 'sell'])
    buy_ACC = TP / len(df.loc[df.predicted == 'buy'])
    over_all_ACC = (TP+TN) / (TP+FP+FN+TN)
    result = [over_all_ACC, buy_ACC, TP, FP, FN, TN]
    return result
