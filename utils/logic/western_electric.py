from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt


'''
Western Electric Rule
One point above(1) UCL or below(2) LCL
Two points above(3)/below(4) 2 sigma (three in two)
Four out of five points above(5)/below(6) 1 sigma (five in four)
Eight points in a row above(7)/below(8) the center line (nine in nine)
'''
#BBands
def test(df, col, term=30):

    a = df[col].rolling(30, ).mean()


def western_electric_test(df, col, daily_update=False):
    df = df.dropna()
    if daily_update:
        update = len(df)
    else:
        update = 9
    result = []
    current_status = 'neutral'
    for i in range(len(df)-update + 1):
        test = df[:update+i].copy().reset_index(drop=True)
        if test.empty:
            continue
        mid = test[col].mean()
        sigma = test[col].std()
        if np.all(test.loc[update + i - 1:, col] >= (mid + 3 * sigma)) or np.all(
                test.loc[update + i - 3:, col] >= (mid + 2 * sigma)) or np.all(
                test.loc[update + i - 5:, col] >= (mid + 1 * sigma)) or np.all(test.loc[update + i - 9:, col] > mid):
            result.append([test['date'].iat[-1], None])
            if update == len(df)-i:
                current_status = 'up'
        elif np.all(test.loc[update + i - 1:, col] <= (mid + -3 * sigma)) or np.all(
                test.loc[update + i - 3:, col] <= (mid + -2 * sigma)) or np.all(
                test.loc[update + i - 5:, col] <= (mid + -1 * sigma)) or np.all(test.loc[update + i - 9:, col] < mid):
            result.append([None, test['date'].iat[-1]])
            if update == len(df)-i:
                current_status = 'down'
        else:
            result.append([None, None])
    if len(result) > 0:
        result = pd.DataFrame(np.array(result), columns=['up', 'down'])
    else:
        result = pd.DataFrame(columns=['up', 'down'])
    return result, current_status



def western_electric(df, col, term=30, sort_lable='date'):
    #df = df.iloc[:90]
    df['mid'] = df[col].mean()
    df['sigma'] = df[col].std()
    df.loc[df[col] >= df.mid + 3*df.sigma, 'up'] = df.loc[df[col] >= df.mid + 3*df.sigma, 'date']
    df.loc[df[col] <= df.mid - 3*df.sigma, 'down'] = df.loc[df[col] <= df.mid - 3*df.sigma, 'date']

    df['temp'] = 0
    df.loc[df[col] >= df.mid + 2*df.sigma, 'temp'] += 1
    df['temp'] = df['temp'].rolling(3,min_periods=3).sum()
    df.loc[df.temp == 2, 'up'] = df.loc[df.temp == 2, 'date']
    df['temp'] = 0
    df.loc[df[col] <= df.mid - 2*df.sigma, 'temp'] += 1
    df['temp'] = df['temp'].rolling(3,min_periods=3).sum()
    df.loc[df.temp == 2, 'down'] = df.loc[df.temp == 2, 'date']

    df['temp'] = 0
    df.loc[df[col] >= df.mid + df.sigma, 'temp'] += 1
    df['temp'] = df['temp'].rolling(5, min_periods=5).sum()
    df.loc[df.temp == 4, 'up'] = df.loc[df.temp == 4, 'date']
    df['temp'] = 0
    df.loc[df[col] <= df.mid - df.sigma, 'temp'] += 1
    df['temp'] = df['temp'].rolling(5, min_periods=5).sum()
    df.loc[df.temp == 4, 'down'] = df.loc[df.temp == 4, 'date']

    df['temp'] = 0
    df.loc[df[col] >= df.mid, 'temp'] += 1
    df['temp'] = df['temp'].rolling(9, min_periods=9).sum()
    df.loc[df.temp == 9, 'up'] = df.loc[df.temp == 9, 'date']
    df['temp'] = 0
    df.loc[df[col] <= df.mid, 'temp'] += 1
    df['temp'] = df['temp'].rolling(9, min_periods=9).sum()
    df.loc[df.temp == 9, 'down'] = df.loc[df.temp == 9, 'date']
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    current_date = datetime.strftime(datetime.now() - timedelta(days=decrease), '%Y-%m-%d')
    #current_date = '2022-04-13'#datetime.today().strftime('%Y-%m-%d')
    current_status = df.loc[df['date'] == current_date].fillna('neutral')
    try:
        if current_status['up'].iloc[-1] != 'neutral':
            current_status = 'up'
        elif current_status['down'].iloc[-1] != 'neutral':
            current_status = 'down'
        else:
            current_status = 'neutral'
    except:
        current_status = 'neutral'
    return df[['up', 'down']], current_status
    '''
    df['+sigma'] = df.mid + df.sigma
    df['-sigma'] = df.mid - df.sigma
    df['+2sigma'] = df.mid + 2*df.sigma
    df['-2sigma'] = df.mid - 2*df.sigma
    df['+3sigma'] = df.mid + 3*df.sigma
    df['-3sigma'] = df.mid - 3*df.sigma

    plt.plot(df[sort_lable],df[[col, 'mid', '+sigma', '-sigma', '+2sigma', '-2sigma']])
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
    df = df.dropna(subset=['up'])
    #df = df[df.result != '']
    df = df[df.up.str.contains('-')]
    for i, j in df[['date', col]].values.tolist():
        plt.scatter(i, j, color='red')
    plt.xticks(rotation=90,fontsize=5)
    plt.show()
    return df[['up', 'down']], current_status
    '''
    
#df = pd.read_csv(r'C:\Users\mick7\PycharmProjects\Stock Analysis\data\stock_price\1101.csv')
#western_electric_test(df, 'close')


