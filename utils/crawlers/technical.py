import pandas as pd
import numpy as np
from tqdm import tqdm
from utils.sqlite import database
import requests
import json
db_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_datas.db'

def crawl_technical(db_dir):
    db = database(db_dir)
    stock_id_list = db.get_stock_id()
    for i in tqdm(stock_id_list):
        result = TaiwanStockPrice(i)
        if db.table_check('Price'):
            insert_db = result.sort_values(by='date', ascending=False).reset_index(drop=True)
            db.insert_date_duplicate(insert_db, 'Price', ['stock_id', 'date'])
        else:
            db.insert_data(result, 'Price')
        #result = TaiwanStockKDJ(result, 9)
        TaiwanStockMACD(result)
        break


def TaiwanStockPrice(stock_id):
    url = 'https://tw.quote.finance.yahoo.net/quote/q?type=ta&perd=d&mkt=10&sym={}&v=1'.format(stock_id)
    content = requests.get(url).text
    content = content[5:-2]
    content = content.replace(',}', '}')
    content = json.loads(content)
    content = content["ta"]
    result = pd.DataFrame(content)
    if not result.empty:
        result.columns = ['date', 'open', 'high', 'low', 'close', 'value']
        result['stock_id'] = stock_id
    return result


def TaiwanStockKDJ(df, days):
    '''
    RSV = (close - lowest of days) / (highest of days - lowest of days) * 100
    K = past k * 2 / 3 + RSV * 1 / 3 ,default = 50
    D = past d * 2 / 3 + K * 1 / 3 ,default = 50
    J = ( 3 * d ) - ( 2 * k )
    '''
    past_k = 50
    past_d = 50
    df['MinLow'] = df['low'].rolling(9, min_periods=9).min()
    # df['MinLow'].fillna(value = df['Low'].expanding().min(), inplace = True)
    df['MaxHigh'] = df['high'].rolling(9, min_periods=9).max()
    # df['MaxHigh'].fillna(value = df['High'].expanding().max(), inplace = True)
    df['RSV'] = (df['close'] - df['MinLow']) / (df['MaxHigh'] - df['MinLow']) * 100
    df["K"] = df['RSV'].ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]
    '''
    result = []
    for i in range(days-1, len(df)):
        lowest = df.loc[i-days+1:i+1, 'low'].min()
        highest = df.loc[i-days+1:i+1, 'high'].max()
        rsv = (df.loc[i]['close'] - lowest) / (highest - lowest) * 100
        k = past_k * 2 / 3 + rsv * 1 / 3
        d = past_d * 2 / 3 + k * 1 / 3
        j = (3 * d) - (2 * k)
        result.append([df.loc[i]['date'], k, d, j, rsv, df.loc[i]['stock_id']])
    result = pd.DataFrame(np.array(result), columns=['date', 'K', 'D', 'J', 'RSV', 'stock_id'])
    '''
    df.to_csv(r'C:\Users\mick7\PycharmProjects\New folder\test.csv')


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
    crawl_technical(db_dir)