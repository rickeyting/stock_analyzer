from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm

from utils.backtesting import trace_back
from utils.postgredb import db_get_stock_id, get_data, db_insert_data, get_init_date
import os

LOCAL_DEALER_YAML = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\utils\logic\local_dealer.yaml'
TABLE_LIST = os.path.join('..', 'table_list.yaml')


def analyze_local_dealer(stock_id):
    query = "SELECT * FROM stock_local_dealer WHERE stock_id = '{}'".format(stock_id)
    df = get_data(query)
    if not df.empty:
        df['bank'] = df['bank'].str[:4]
        df['total_price'] = (df.buy_shares-df.sell_shares) * df.price
        fees = df['total_price'].abs().mean()
        fees += df['total_price'].std()
        df.loc[(df.buy_shares * df.price <= fees, 'buy_shares')] = 0
        df.loc[(df.sell_shares * df.price <= fees, 'sell_shares')] = 0
        df['fees'] = (df.buy_shares - df.sell_shares)*df.price
        df = df.groupby(by=['stock_id', 'date', 'bank']).agg({'fees': 'sum'}).reset_index()
        df.loc[df.fees >= fees, 'buy'] = df.loc[df.fees >= fees, 'date']
        df.loc[df.fees <= -1*fees, 'sell'] = df.loc[df.fees <= -1*fees, 'date']
        df = df.loc[(df.fees >= fees) | (df.fees <= -1*fees)]
    result = []
    banks = list(dict.fromkeys(df.bank.values.tolist()))
    for i in banks:
        pre_df = df[df.bank == i].copy()
        pre_df['sell'] = pre_df['sell'].bfill(axis='rows')
        pre_df = pre_df.dropna()
        if not pre_df.empty:
            result.append(pre_df)
    if result:
        result = pd.concat(result)
        return result[['stock_id', 'bank', 'buy', 'sell']]
    else:
        return pd.DataFrame()


def update_local_dealer(table_yaml=TABLE_LIST):
    stock_list = db_get_stock_id(market_type='sii')
    result = []
    for i in tqdm(stock_list):
        df = analyze_local_dealer(i)
        if not df.empty:
            test_result = trace_back(df, i, banks=True)
            if not test_result.empty:
                result.append(test_result)
    result = pd.concat(result)
    result = result.groupby(['bank', 'stock_id']).sum().reset_index()
    result['earn'] = result['earn'].astype(int)
    result['neural'] = result['neural'].astype(int)
    result['loss'] = result['loss'].astype(int)
    result_banks = result.groupby(['bank', 'stock_id']).agg({'earn': 'sum', 'neural': 'sum', 'loss': 'sum'}).reset_index()
    by_banks = result_banks.groupby('bank').sum().reset_index()
    by_banks['stock_id'] = 'all'
    query = "SELECT stock_id, 產業類別 FROM stock_id"
    industry_list = get_data(query)
    by_industry = result_banks.merge(industry_list, on='stock_id', how='left')
    by_industry = by_industry.groupby(['bank', '產業類別']).sum().reset_index()
    by_industry['stock_id'] = by_industry['產業類別']
    by_industry = by_industry.drop(['產業類別'], axis=1)
    result_banks = pd.concat([by_banks, by_industry, result_banks])
    result_banks['date'] = datetime.today().strftime('%Y-%m-%d')
    db_insert_data('analyze_local_dealer', result_banks, table_yaml=table_yaml)


def daily_special_result(loca_dealer_yaml=LOCAL_DEALER_YAML, table_yaml=TABLE_LIST):
    with open(loca_dealer_yaml, 'r', encoding='utf-8-sig') as file:
        loca_dealer_filter = yaml.safe_load(file)
    loca_dealer_filter = loca_dealer_filter['SPECIAL']
    loca_dealer_filter.reverse()
    query = "SELECT * FROM local_dealer_special"
    current_df = get_data(query)
    if current_df.empty:
        updated_date = get_init_date('stock_local_dealer', 'min')
    else:
        updated_date = get_init_date('local_dealer_special', 'max')
    updated_date = datetime.strptime(updated_date, '%Y-%m-%d') + timedelta(days=1)
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    final_date = datetime.today() - timedelta(days=decrease)
    to_csv_df = []
    while updated_date < final_date:
        check_date = updated_date.strftime('%Y-%m-%d')
        query = "SELECT * FROM local_dealer_special"
        result = get_data(query)
        if result.empty:
            result = pd.DataFrame(np.array(loca_dealer_filter), columns=['bank', 'stock_id'])
            result['banks_special'] = 'neural'
            result['shares'] = 0
            result['high'] = 0
            result['current'] = 0
        for i in tqdm(loca_dealer_filter):
            query = "SELECT * FROM stock_local_dealer WHERE stock_id = '{}' AND date = '{}'".format(i[1], check_date)
            df = get_data(query)
            df['bank'] = df['bank'].str[:4]
            df = df[df.bank == i[0]]
            total_price = ((df.buy_shares - df.sell_shares) * df.price).sum()
            result.loc[(result.bank == str(i[0])) & (result.stock_id == str(i[1])), 'shares'] += (df.buy_shares - df.sell_shares).sum()
            result.loc[(result.bank == str(i[0])) & (result.stock_id == str(i[1])), 'current'] += total_price
        result['date'] = check_date
        result.loc[result.current > 2e6, 'banks_special'] = 'buy'
        result.loc[result.current < 0, 'current'] = 0
        result.loc[result.shares < 0, 'current'] = 0
        query = "SELECT stock_id, close FROM stock_price WHERE date = '{}'".format(check_date)
        result.loc[result.high < result.current, 'high'] = result.loc[result.high < result.current, 'current']
        price_df = get_data(query)
        result = result.merge(price_df, on='stock_id', how='left')
        result.loc[(result.close*result.shares) < result.current*0.7, 'banks_special'] = 'buy'
        result.loc[(result.close * result.shares) >= result.current*0.7, 'banks_special'] = 'neural'
        result.loc[(result.high / 2) > result.current, 'banks_special'] = 'sell'
        result.loc[(result.high / 2) > result.current, ['high', 'current', 'shares']] = [0, 0, 0]
        result = result.drop('close', axis=1)
        db_insert_data('local_dealer_special', result, table_yaml=table_yaml)
        to_csv_df.append(result)
        updated_date += timedelta(days=1)
    to_csv_df = pd.concat(to_csv_df)
    to_csv_df.to_csv(r'C:\Users\mick7\Downloads\special_history.csv')


    '''
    for i in tqdm(loca_dealer_filter):
        query = "SELECT * FROM stock_local_dealer WHERE stock_id = '{}' AND date = '{}'".format(i[1], check_date)
        df = get_data(query)
        df['bank'] = df['bank'].str[:4]
        df = df[df.bank == i[0]]
        total_price = ((df.buy_shares - df.sell_shares) * df.price).sum()
    '''


def daily_special_result1(loca_dealer_yaml=LOCAL_DEALER_YAML):
    with open(loca_dealer_yaml, 'r', encoding='utf-8-sig') as file:
        loca_dealer_filter = yaml.safe_load(file)
    loca_dealer_filter = loca_dealer_filter['SPECIAL']
    loca_dealer_filter.reverse()
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    check_date = datetime.strftime(datetime.today() - timedelta(days=decrease), '%Y-%m-%d')
    result = pd.DataFrame(np.array(loca_dealer_filter), columns=['bank', 'stock_id'])
    result = result.groupby('stock_id').first().reset_index()
    result['banks_special'] = 'None'
    for i in tqdm(loca_dealer_filter):
        query = "SELECT * FROM stock_local_dealer WHERE stock_id = '{}' AND date = '{}'".format(i[1], check_date)
        df = get_data(query)
        df['bank'] = df['bank'].str[:4]
        df = df[df.bank == i[0]]
        df['total_price'] = (df.buy_shares - df.sell_shares) * df.price
        #df = df.loc[(df['total_price'] > 2e6) | (df['total_price'] < -2e6)]
        #print(df['total_price'].sum())

        if not df.empty:
            if df['total_price'].sum() > 2e6:
                result.loc[result.stock_id == str(i[1]), 'banks_special'] = 'buy'
            elif df['total_price'].sum() < -2e6:
                result.loc[result.stock_id == str(i[1]), 'banks_special'] = 'sell'
            else:
                result.loc[result.stock_id == str(i[1]), 'banks_special'] = 'neutral'
        else:
            result.loc[result.stock_id == str(i[1]), 'banks_special'] = 'neutral'
    result = result[['stock_id', 'banks_special']]
    result.to_csv(r'C:\Users\mick7\Downloads\special.csv')
    return result


def daily_banks_result(loca_dealer_yaml=LOCAL_DEALER_YAML):
    with open(loca_dealer_yaml, 'r', encoding='utf-8-sig') as file:
        loca_dealer_filter = yaml.safe_load(file)
    loca_dealer_filter = loca_dealer_filter['BANKS']
    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    check_date = datetime.strftime(datetime.today() - timedelta(days=decrease), '%Y-%m-%d')
    query = "SELECT * FROM stock_local_dealer WHERE date = '{}'".format(check_date)
    df = get_data(query)
    df['bank'] = df['bank'].str[:4]
    df = df.loc[df.bank.isin(loca_dealer_filter)]
    df['total_price'] = (df.buy_shares - df.sell_shares) * df.price
    df = df.loc[(df['total_price'] >= 2e6) | (df['total_price'] <= -2e6)]
    df = df.groupby(['stock_id']).agg({'date': 'first', 'total_price': 'sum'}).reset_index()
    df['banks'] = 'neutral'
    df.loc[df.total_price > 5e6, 'banks'] = 'buy'
    df.loc[df.total_price < -5e6, 'banks'] = 'sell'
    df = df[['stock_id', 'banks']]
    df.to_csv(r'C:\Users\mick7\Downloads\banks_lo.csv')
    return df


def daily_industry_result():
    query = "SELECT stock_id, 產業類別 FROM stock_id"
    industry_list_df = get_data(query)
    industry_list = industry_list_df['產業類別'].tolist()
    query = "SELECT * FROM analyze_local_dealer"
    industry = get_data(query)
    industry = industry.loc[industry['stock_id'].isin(industry_list)]
    industry = industry.loc[industry.earn != 0]
    industry['rate'] = industry.earn / (industry.earn+industry.neural+industry.loss)
    industry = industry.sort_values('rate', ascending=False)
    industry = industry.loc[industry.rate != 1]
    industry = industry.loc[industry.rate > 0.8]

    weekday = datetime.today().weekday()
    if weekday > 5:
        decrease = weekday - 4
    else:
        decrease = 0
    check_date = datetime.strftime(datetime.today() - timedelta(days=decrease), '%Y-%m-%d')
    query = "SELECT * FROM stock_local_dealer WHERE date = '{}'".format(check_date)
    df = get_data(query)
    df['bank'] = df['bank'].str[:4]
    df = df.merge(industry_list_df, on='stock_id', how='left')
    df = df.loc[df.set_index(['bank', '產業類別']).index.isin(industry[['bank', 'stock_id']].values.tolist())]
    df['total_price'] = (df.buy_shares - df.sell_shares) * df.price
    df = df.loc[(df['total_price'] >= 1e6) | (df['total_price'] <= -1e6)]
    df = df.groupby(['bank', 'stock_id']).agg({'date': 'first', 'total_price': 'sum'}).reset_index()
    df['result'] = 0
    df.loc[df['total_price'] > 0, 'result'] = 1
    df.loc[df['total_price'] < 0, 'result'] = -1
    df = df.groupby(['stock_id']).agg({'result': 'sum'}).reset_index()
    df.loc[df['result'] > 0, 'industry'] = 'buy'
    df.loc[df['result'] < 0, 'industry'] = 'sell'
    df.loc[df['result'] == 0, 'industry'] = 'neutral'
    df = df[['stock_id', 'industry']]
    df.to_csv(r'C:\Users\mick7\Downloads\industry.csv')
    return df


def get_sum():
    df = pd.read_csv(r'C:\Users\mick7\Downloads\technical.csv')
    df1 = pd.read_csv(r'C:\Users\mick7\Downloads\banks_lo.csv')
    df2 = pd.read_csv(r'C:\Users\mick7\Downloads\special.csv')
    df3 = pd.read_csv(r'C:\Users\mick7\Downloads\industry.csv')
    result = df.merge(df1, on='stock_id', how='left')
    result = result.merge(df2, on='stock_id', how='left')
    result = result.merge(df3, on='stock_id', how='left')
    result = result.replace({'buy': 1, 'sell': -1, 'neutral': 0})
    result = result.fillna(0)
    result.to_csv(r'C:\Users\mick7\Downloads\sum.csv')


def summary_test():
    df = pd.read_csv(r'C:\Users\mick7\Downloads\base.csv')
    label_list = df[['stock_id', 'label']].groupby('label').first().reset_index()
    label_list = label_list['label'].values.tolist()
    df = df.fillna(0)
    df.loc[df['buy'] != 0, 'date'] = df.loc[df['buy'] != 0, 'buy']
    df.loc[df['sell'] != 0, 'date'] = df.loc[df['sell'] != 0, 'sell']
    result = df.groupby(['stock_id', 'date']).first().reset_index()
    result = result[['stock_id', 'date']]
    for i in label_list:
        test = df.loc[df['label'] == i]
        test = test.fillna(0)
        test.loc[test['buy'] != 0, i] = 1
        test.loc[test['sell'] != 0, i] = -1
        result = result.merge(test[['stock_id', 'date', i]], on=['stock_id', 'date'], how='left')
    result['ans'] = result[label_list].sum(axis=1)
    result.loc[result.ans > 10, 'buy'] = result.loc[result.ans > 10, 'date']
    result.loc[result.ans < -10, 'sell'] = result.loc[result.ans < -10, 'date']
    result = result.sort_values('date')
    stock_list = list(dict.fromkeys(df.stock_id.values.tolist()))
    final = []
    for i in stock_list:
        ans = result.loc[result.stock_id == i].copy()
        ans['sell'] = ans['sell'].bfill(axis='rows')
        ans = ans[['buy', 'sell']].dropna()
        test_result = trace_back(ans, i)
        final.append(test_result)
    result = [sum(x) for x in zip(*final)]
    print(result)


if __name__ == '__main__':
    #update_local_dealer()
    #daily_special_result1()
    #daily_banks_result()
    #daily_industry_result()
    get_sum()
    #df = df[['buy', 'sell']]
