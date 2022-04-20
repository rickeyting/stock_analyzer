import numpy as np
import pandas as pd
import talib
import scipy.stats as st
from sklearn.preprocessing import normalize
from utils.postgredb import get_data
from datetime import datetime, timedelta


def get_price_info(stock_id, days):
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    start_data = datetime.strftime(datetime.today() - timedelta(days=decrease + days), '%Y-%m-%d')
    query = "SELECT * FROM stock_price WHERE date >= '{}' AND stock_id = '{}'".format(start_data, stock_id)
    df = get_data(query)
    result = df.sort_values(by='date')
    return result


def technical_analysis(t_type, period, stock_id, days=365):
    df = get_price_info(stock_id, days)
    if t_type == 'DEMA':
        sarext = talib.DEMA(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'KAMA':
        sarext = talib.KAMA(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'MIDPRICE':
        sarext = talib.MIDPRICE(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'SAR':
        sarext = talib.SAR(df['max'], df['min'], acceleration=0, maximum=0)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'T3':
        sarext = talib.T3(df.close, timeperiod=period, vfactor=0)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'TEMA':
        sarext = talib.TEMA(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'WMA':
        sarext = talib.WMA(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ATR':
        sarext = talib.ATR(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'NATR':
        sarext = talib.NATR(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'AD':
        sarext = talib.AD(df['max'], df['min'], df.close, df.trading_turnover)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ADOSC':
        sarext = talib.ADOSC(df['max'], df['min'], df.close, df.trading_turnover, fastperiod=3, slowperiod=10)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'OBV':
        sarext = talib.OBV(df.close, df.trading_turnover)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'HT_DCPERIOD':
        sarext = talib.HT_DCPERIOD(df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'HT_DCPHASE':
        sarext = talib.HT_DCPHASE(df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'HT_TRENDMODE':
        sarext = talib.HT_TRENDMODE(df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ADX':
        sarext = talib.ADX(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ADXR':
        midprice = talib.ADXR(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': midprice})
        return result
    if t_type == 'APO':
        midprice = talib.APO(df.close, fastperiod=12, slowperiod=26, matype=0)
        result = pd.DataFrame(data={'date': df['date'], 'ans': midprice})
        return result
    if t_type == 'AROON':
        midprice = talib.AROON(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': midprice})
        return result
    if t_type == 'AROONOSC':
        midprice = talib.AROONOSC(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': midprice})
        return result
    if t_type == 'BOP':
        midprice = talib.BOP(df['open'], df['max'], df['min'], df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': midprice})
        return result
    if t_type == 'CCI':
        sarext = talib.CCI(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'CMO':
        cmd = talib.CMO(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': cmd})
        return result
    if t_type == 'DX':
        tema = talib.DX(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': tema})
        return result
    if t_type == 'MACD':
        MACD_macd,MACD_macdsignal,MACD_macdhist = talib.MACD(df.close, fastperiod=12, slowperiod=26, signalperiod=9)
        result = pd.DataFrame(data={'date': df['date'], 'ans': MACD_macdhist})
        return result
    if t_type == 'MFI':
        sarext = talib.MFI(df['max'], df['min'], df.close, df.trading_turnover, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'MINUS_DI':
        di = talib.MINUS_DI(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df.loc[len(df)-len(di):, 'date'], 'ans': di})
        return result
    if t_type == 'MINUS_DM':
        sarext = talib.MINUS_DM(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result

    if t_type == 'LINEARREG_ANGLE':
        angle = talib.LINEARREG_ANGLE(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': angle})
        return result
    if t_type == 'LINEARREG_INTERCEPT':
        intercept = talib.LINEARREG_INTERCEPT(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df.loc[len(df) - len(intercept):, 'date'], 'ans': intercept})
        return result
    if t_type == 'LINEARREG_SLOPE':
        sarext = talib.LINEARREG_SLOPE(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'LINEARREG':
        sarext = talib.LINEARREG(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'RSI':
        rsi = talib.RSI(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': rsi})
        return result
    if t_type == 'TRANGE':
        trange = talib.TRANGE(df['max'], df['min'], df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': trange})
        return result
    if t_type == 'HT_DCPERIOD':
        ad = talib.HT_DCPERIOD(df.close)
        result = pd.DataFrame(data={'date': df['date'], 'ans': ad})
        return result
    if t_type == 'AROONOSC':
        sar = talib.AROONOSC(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sar})
        return result



    if t_type == 'MOM':
        sarext = talib.MOM(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'PLUS_DI':
        sarext = talib.PLUS_DI(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'PLUS_DM':
        sarext = talib.PLUS_DM(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ROC':
        sarext = talib.ROC(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ROCP':
        sarext = talib.ROCP(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ROCR':
        sarext = talib.ROCR(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'ROCR100':
        sarext = talib.ROCR100(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'BETA':
        sarext = talib.BETA(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'WILLR':
        sarext = talib.WILLR(df['max'], df['min'], df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'CORREL':
        sarext = talib.CORREL(df['max'], df['min'], timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'TSF':
        sarext = talib.TSF(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result
    if t_type == 'VAR':
        sarext = talib.VAR(df.close, timeperiod=period)
        result = pd.DataFrame(data={'date': df['date'], 'ans': sarext})
        return result





if __name__ == '__main__':
    '''
    a = technical_analysis('MINUS_DI', 14, '2603')
    stat, p = st.shapiro(a['ans'].dropna())
    #a['ans'] = a['ans'] + abs(a['ans'].min())+1
    print(a)
    print(p)
    if p >= 0.05:
        print('yes')
    else:
        a, _ = st.boxcox((a['ans']+a['ans'].min()).dropna())
        stat, p = st.shapiro(a)
        if p >= 0.05:
            print(a)
            print('t_yes')
        else:
            print('t_no')
    #print(TaiwanStockMACD('1101'))
    '''