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


def western_electric_test(df, col, term=30, sort_lable='date'):
    #mean_ok = round(df[col].mean(),2)
    #stdev_ok = round(df[col].std(),2)
    #df = df.sort_values(by=sort_lable, ascending=False).reset_index(drop=True)
    df['mid'] = df[col].rolling(term,min_periods=term).mean()
    df['sigma'] = df[col].rolling(term,min_periods=term).std()
    df['mid'] = df[col].mean()
    df['sigma'] = df[col].std()
    df['result'] = ''
    df.loc[df[col] >= df.mid + 3*df.sigma,'result'] += '1,'
    df.loc[df[col] <= df.mid - 3*df.sigma,'result'] += '2,'
    
    df['temp'] = 0
    df.loc[df[col] >= df.mid + 2*df.sigma,'temp'] += 1
    df['temp'] = df['temp'].rolling(3,min_periods=3).sum()
    df.loc[df.temp == 2,'result'] += '3,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid - 2*df.sigma,'temp'] += 1
    df['temp'] = df['temp'].rolling(3,min_periods=3).sum()
    df.loc[df.temp == 2,'result'] += '4,'
    
    df['temp'] = 0
    df.loc[df[col] >= df.mid + df.sigma,'temp'] += 1
    df['temp'] = df['temp'].rolling(5, min_periods=5).sum()
    df.loc[df.temp == 4,'result'] += '5,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid - df.sigma,'temp'] += 1
    df['temp'] = df['temp'].rolling(5, min_periods=5).sum()
    df.loc[df.temp == 4,'result'] += '6,'
    
    df['temp'] = 0
    df.loc[df[col] >= df.mid,'temp'] += 1
    df['temp'] = df['temp'].rolling(9, min_periods=9).sum()
    df.loc[df.temp == 9,'result'] += '7,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid,'temp'] += 1
    df['temp'] = df['temp'].rolling(9, min_periods=9).sum()
    df.loc[df.temp == 9, 'result'] += '8,'

    df['+sigma'] = df.mid + df.sigma
    df['-sigma'] = df.mid - df.sigma
    df['+2sigma'] = df.mid + 2*df.sigma
    df['-2sigma'] = df.mid - 2*df.sigma
    df['+3sigma'] = df.mid + 3*df.sigma
    df['-3sigma'] = df.mid - 3*df.sigma
    df.plot(sort_lable,[col, 'mid', '+sigma', '-sigma', '+2sigma', '-2sigma', '+3sigma', '-3sigma'])
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
    df = df[df.result != '']
    df = df[df.result.str.contains('8')]
    for i, j in df[[sort_lable, col]].values.tolist():
        plt.scatter(i, j, color='red')
    plt.show()
    


df = pd.read_csv(r'C:\Users\mick7\PycharmProjects\New folder\test1.csv')
df['date'] = df.index
western_electric_test(df,'close')
