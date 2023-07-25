import pandas as pd
import psycopg2
from tqdm import tqdm
import glob
import os
from sqlalchemy import create_engine
import datetime
import yaml

db_user = os.environ.get('USERNAME')
db_password = os.environ.get('DB_PASSWORD')
ENGINE = create_engine('postgresql://postgres:{}@localhost:5432/stockdata'.format('wqsjtul8'))
TABLE_LIST = os.path.join('.', 'table_list.yaml')


def init_tables(engine=ENGINE, table_yaml=TABLE_LIST):
    with open(table_yaml, 'r', encoding='utf-8-sig') as file:
        table_list = yaml.safe_load(file)
    for i in table_list:
        if check_table_exist(i):
            continue
        table = table_list[i]
        df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in table['cols'].items()})
        df.to_sql("{}".format(i), engine, index=False)
        if 'primaries' in table_list[i]:
            query = "ALTER TABLE {} ADD PRIMARY KEY (".format(i)
            for j in table['primaries']:
                query += j
                if table['primaries'].index(j) != len(table['primaries']) -1:
                    query += ", "
            query += ");"
            engine.execute(query)
    return 'tables created'


def check_table_exist(table_name, engine=ENGINE):
    check_table = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog';"
    list_tables = engine.execute(check_table)
    list_tables = [item[0] for item in list_tables]
    if table_name in list_tables:
        return True
    else:
        return False


#download from https://www.twse.com.tw/zh/brokerService/brokerServiceAudit loacte in folder data(rename 證券商代號 to bank)
def update_banks_list(list_dir, table_name='bank_id', engine=ENGINE, table_yaml=TABLE_LIST):
    df = pd.read_csv(list_dir)
    db_insert_data(table_name, df, table_yaml=table_yaml)


def init_local_dealer(path, table_yaml=TABLE_LIST):
    all_target = glob.glob(os.path.join(path, '**\*.csv'), recursive=True)
    for i in tqdm(all_target):
        date = i.split('\\')[-2]
        filename = os.path.basename(i)
        stock_id = filename.split('.', 1)[0]
        df = pd.read_csv(i)
        df = df.rename(columns={'order': 'order_num'})
        df['date'] = date
        df['stock_id'] = stock_id
        df = df.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)
        if not df.empty:
            db_insert_data('stock_local_dealer', df, table_yaml=table_yaml)


def db_insert_data(table_name, df, table_yaml=TABLE_LIST, engine=ENGINE):
    if table_name == 'stock_id' or table_name == 'local_dealer_special':
        if data_empty(table_name):
            df.to_sql("{}".format(table_name), engine, if_exists='append', index=False)
        else:
            del_data = "TRUNCATE {};".format(table_name)
            engine.execute(del_data)
            df.to_sql("{}".format(table_name), engine, if_exists='append', index=False)
    else:
        with open(table_yaml, 'r', encoding='utf-8-sig') as file:
            table_list = yaml.safe_load(file)
        cols = table_list[table_name]['cols']
        insert_init = """INSERT INTO {} (""".format(table_name)
        col = ",".join(cols)
        col_end = ") VALUES "
        vals = ""
        for date, row in df.iterrows():
            vals += '('
            for i in cols:
                type = table_list[table_name]['cols']['{}'.format(i)]
                if type == 'str':
                    vals += "'{}'".format(row[i])
                else:
                    vals += "{}".format(row[i])
                if list(cols)[-1] != i:
                    vals += ','
            vals += '),'
        vals = vals[:-1]
        if 'primaries' in table_list[table_name]:
            primaries_key = table_list[table_name]['primaries']
            end = " ON CONFLICT ("
            for i in primaries_key:
                end += "{}".format(i)
                if list(primaries_key)[-1] != i:
                    end += ', '
            end += ') DO nothing;'
            '''
            last_col = list(set(list(cols)) - set(list(primaries_key)))
            for i in last_col:
                end += "{} = EXCLUDED.{}".format(i, i)
                if list(last_col)[-1] != i:
                    end += ', '
            end += ';'
            '''
            query = insert_init + col + col_end + vals + end
        else:
            query = insert_init + col + col_end + vals
        engine.execute(query)


def data_empty(table_name, engine=ENGINE):
    query = "SELECT COUNT(1) FROM {};".format(table_name)
    raws = [item[0] for item in engine.execute(query)][0]
    if raws == 0:
        return True
    else:
        return False


def db_get_stock_id(market_type='all', engine=ENGINE):
    if market_type == 'all':
        query = "SELECT stock_id FROM stock_id;"
    else:
        query = "SELECT stock_id FROM stock_id where market_type='{}';".format(market_type)
    stock_list = engine.execute(query)
    stock_list = [item[0] for item in stock_list]
    return stock_list


def db_get_bank_id(engine=ENGINE):
    query = "SELECT bank FROM bank_id"
    bank_list = engine.execute(query)
    bank_list = [item[0] for item in bank_list]
    return bank_list



def db_get_exist(table_name, table_yaml=TABLE_LIST, engine=ENGINE, date=None):
    with open(table_yaml, 'r', encoding='utf-8-sig') as file:
        table_list = yaml.safe_load(file)
    check_key = table_list[table_name]['check_key']
    get_cols = ",".join(check_key)
    query = "SELECT {} FROM {}".format(get_cols, table_name)
    if date:
        query += " WHERE date = '{}'".format(date)
    result = engine.execute(query)
    result = list(dict.fromkeys(result))
    result = [item[:len(check_key)] for item in result]
    return result


def get_init_date(table_name, function, engine=ENGINE):
    if function == 'min':
        query = "SELECT min(date) FROM {}".format(table_name)
    elif function == 'max':
        query = "SELECT max(date) FROM {}".format(table_name)
    result = engine.execute(query)
    result = [item[0] for item in result]
    return result[0]



def get_data(sql, engine=ENGINE):
    result = pd.read_sql(sql, engine)
    '''
    query = "SELECT * FROM {} WHERE date >= '{}'".format(table_name, date)
    result = engine.execute(query)
    result = [i for i in result]
    '''
    print(result)
    return result


def export_bull_data(stock_id, date_range=5, engine=ENGINE):
    sql = 'stock_price'
    bull_date = pd.read_sql(sql, engine)
    #bull_date.to_csv(r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\test.csv')
    bull_date = bull_date[bull_date.stock_id == stock_id]
    bull_date = bull_date[['stock_id', 'date', 'close']]
    bull_date['change'] = bull_date['close'].diff(date_range) / bull_date['close'].shift(date_range, axis=0)
    bull_date['date'] = bull_date['date'].shift(date_range, axis=0)
    bull_date = bull_date[bull_date.change >= 0.05]
    bull_date = bull_date.date.tolist()
    #print(bull_date)
    scan_date = []
    for i in bull_date:
        end = i
        start = datetime.datetime.strptime(i, '%Y-%m-%d') - datetime.timedelta(days=30)
        start = datetime.datetime.strftime(start, '%Y-%m-%d')
        scan_date.append([start, end])
    #print(scan_date)
    holder_data = 'stock_local_dealer'
    result = []
    for j in scan_date:
        query = "SELECT * FROM {} WHERE stock_id = '{}' and date >= '{}' and date <= '{}'".format(holder_data, stock_id, j[0], j[1])
        range_data = pd.read_sql(query, engine)
        result.append(range_data)
    result = pd.concat(result)
    result = result[['bank', 'buy_shares', 'sell_shares']].groupby('bank').sum().reset_index()
    result['result'] = result.buy_shares - result.sell_shares
    result = result.sort_values('result', ascending=False)
    #print(result.iloc[0, 0])
    return result.iloc[0, 0]


def export_local_dealer(stock_id, engine=ENGINE, status='training'):
    table = 'stock_local_dealer'
    query = "SELECT * FROM {} WHERE stock_id = '{}' AND date > '2023-01-01'".format(table, stock_id)
    result = pd.read_sql(query, engine)
    result['bank'] = result['bank'].str[:4]
    result['shares'] = result.buy_shares - result.sell_shares
    result = result.groupby(['stock_id', 'date', 'bank']).agg({'shares': 'sum'}).reset_index()
    result = result.pivot(columns='bank', index=['stock_id', 'date']).fillna(0).reset_index()
    bank_id_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\data\bank_id.csv'
    bank_id = pd.read_csv(bank_id_dir)
    bank_id = bank_id['bank'].tolist()
    result.columns = ['stock_id', 'date'] + result['shares'].columns.tolist()
    #result = result.iloc[:-1, :]
    for c in result.columns.tolist():
        if not c in ['stock_id', 'date'] + bank_id:
            result = result.drop(c, axis=1)
    for b in bank_id:
        if not b in result.columns:
            result[b] = 0
    result = result[['stock_id', 'date'] + bank_id]
    #result = result.xs('location1', level='loc', axis=1)
    price_data = pd.read_sql('stock_price', engine)
    price_data = price_data[price_data.stock_id == stock_id]
    price_data['change'] = (price_data['close'] - price_data['close'].shift(5, axis=0)) / price_data['close'].shift(5, axis=0)
    price_data['change'] = price_data['change'].shift(-5,axis=0)
    price_data = price_data[['stock_id', 'date', 'close', 'change']]
    price_data['date'] = pd.to_datetime(price_data['date'])
    price_data = price_data.set_index('date').resample('D').ffill().reset_index()
    price_data['date'] = price_data['date'].astype(str)
    result = pd.merge(result, price_data, on=['stock_id', 'date'], how='left')
    if status == 'training':
        result = result.dropna(axis=0)
        result.to_csv(r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\stock_data\{}.csv'.format(stock_id))
    else:
        result = result.iloc[:]
        result.to_csv(
            r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\tracing_data\{}.csv'.format(
                stock_id))
#fundamental_get_exist('taiwanstockcashflows')
#get_stock_id()
#init_local_dealer(r'C:\Users\mick7\PycharmProjects\twse-captcha-breaker\stock_local_dealer')
#init_tables()
#data_empty('stock_id')
def export_all(status='training'):

    all_stock_id = db_get_stock_id()
    bank_id_match = []
    for s in tqdm(all_stock_id):
        if status == 'training':
            path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\stock_data\{}.csv'.format(s)
        else:
            path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\tracing_data\{}.csv'.format(
                s)
        #export_local_dealer(s)

        try:
            export_local_dealer(s, status=status)
        except:
            pass

#export_all()
#export_all(status='tracing')
'''
all_stock_id = db_get_stock_id('sii')
bank_id_match = []
for s in tqdm(all_stock_id):
    try:
        bank_id = export_bull_data(s)
        bank_id_match.append([s, bank_id])
    except:
        pass
end = pd.DataFrame(bank_id_match, columns=['stock_id', 'bank_id'])
'''
#end.to_csv(r"C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\est.csv")
# Path to the directory containing the CSV files
def get_stock_info():
    csv_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\lstm\Stock-Price-Prediction-LSTM\result\\'

    # Search pattern for CSV files
    csv_pattern = csv_dir + 'pred_*.csv'

    # Get a list of all CSV files in the directory
    csv_files = glob.glob(csv_pattern)
    print(csv_files)
    # Initialize an empty list to store the data from all CSV files
    dataframes = []

    # Iterate over each CSV file
    for csv_file in csv_files:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        # Append the DataFrame to the list
        dataframes.append(df)

    # Concatenate all DataFrames in the list vertically
    combined_df = pd.concat(dataframes, ignore_index=True)
    info_stock_path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\data\stock_id.csv'
    info_df = pd.read_csv(info_stock_path)
    info_df = info_df[['stock_id', '公司簡稱', '產業類別']]
    info_df['stock_id'] = info_df['stock_id'].astype(str)
    combined_df['stock_id'] = combined_df['stock_id'].str[:4]
    combined_df = combined_df[['stock_id']]
    combined_df = combined_df.drop_duplicates(subset='stock_id')
    combined_df['stock_id'] = combined_df['stock_id'].astype(str)
    combined_df = pd.merge(combined_df, info_df, on='stock_id', how='left')
    combined_df.to_csv(r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\data\checked_types.csv', index=False, encoding='utf-8-sig')
#get_stock_info()
