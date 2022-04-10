from utils.postgredb import get_data


def TaiwanStockKDJ(df, days):
    '''
    RSV = (close - lowest of days) / (highest of days - lowest of days) * 100
    K = past k * 2 / 3 + RSV * 1 / 3 ,default = 50
    D = past d * 2 / 3 + K * 1 / 3 ,default = 50
    J = ( 3 * d ) - ( 2 * k )
    '''
    past_k = 50
    past_d = 50
    df['MinLow'] = df['min'].rolling(9, min_periods=9).min()
    # df['MinLow'].fillna(value = df['Low'].expanding().min(), inplace = True)
    df['MaxHigh'] = df['max'].rolling(9, min_periods=9).max()
    # df['MaxHigh'].fillna(value = df['High'].expanding().max(), inplace = True)
    df['RSV'] = (df['close'] - df['MinLow']) / (df['MaxHigh'] - df['MinLow']) * 100
    df["K"] = df['RSV'].ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    df.to_csv(r'C:\Users\mick7\PycharmProjects\New folder\test1.csv')


def TaiwanStockMACD(df):
    df['di'] = (df.high-df.low+2*df.close)/4
    df['ema12'] = df['di'].rolling(12, min_periods=12).mean()
    df['ema26'] = df['di'].rolling(26, min_periods=26).mean()
    df['ema12'] = [np.nan] + df['ema12'].tolist()[:-1]
    print(df)
    ema12 = []
    for d,e in df[['di','ema12']].values.tolist():
        try:
            ema12.append(e*11/13+d*2/13)
        except:
            ema12.append(np.nan)
    df['ema12'] = ema12
    print(df)




if __name__ == '__main__':
    sql = "SELECT * FROM stock_price WHERE date >= '2022-03-01'"
    get_data(sql)