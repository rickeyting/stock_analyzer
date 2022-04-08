import pandas as pd
import numpy as np


'''
Western Electric Rule
One point above(1) UCL or below(2) LCL
Two points above(3)/below(4) 2 sigma
Four out of five points above(5)/below(6) 1 sigma
Eight points in a row above(7)/below(8) the center line
'''

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
    df['temp'].rolling(2).sum()
    df.loc[df.temp == 2,'result'] += '3,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid - 2*df.sigma,'temp'] += 1
    df['temp'].rolling(2).sum()
    df.loc[df.temp == 2,'result'] += '4,'
    
    df['temp'] = 0
    df.loc[df[col] >= df.mid + df.sigma,'temp'] += 1
    df['temp'].rolling(4).sum()
    df.loc[df.temp == 4,'result'] += '5,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid - df.sigma,'temp'] += 1
    df['temp'].rolling(4).sum()
    df.loc[df.temp == 4,'result'] += '6,'
    
    df['temp'] = 0
    df.loc[df[col] >= df.mid,'temp'] += 1
    df['temp'].rolling(8).sum()
    df.loc[df.temp == 8,'result'] += '7,'
    df['temp'] = 0
    df.loc[df[col] <= df.mid,'temp'] += 1
    df['temp'].rolling(8).sum()
    df.loc[df.temp == 8,'result'] += '8,'
    
    print(df)
    df['+sigma'] = df.mid + df.sigma
    df['-sigma'] = df.mid - df.sigma
    df['+2sigma'] = df.mid + 2*df.sigma
    df['-2sigma'] = df.mid - 2*df.sigma
    df['+3sigma'] = df.mid + 3*df.sigma
    df['-3sigma'] = df.mid - 3*df.sigma
    df.plot(sort_lable,[col,'mid','+sigma','-sigma','+2sigma','-2sigma','+3sigma','-3sigma'])
    plt.legend(bbox_to_anchor= (1.05, 1), loc='upper left', borderaxespad=0)
    df = df[df.result != '']
    for i,j in df[[sort_lable,'content']].values.tolist():
        plt.scatter(i,j,color='red')
    
ttt = [random.uniform(-1,1) for i in range(60)]
df = pd.DataFrame(np.array(ttt),columns=['content'])
df = pd.read_csv(r'D:\Project\AVI_AI\data\tt.csv')
df['date'] = df.index
western_electric_test(df,'content')
