import pandas as pd
import psycopg2
from tqdm import tqdm
import glob
import os
from sqlalchemy import create_engine
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


def init_local_dealer(path):
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
            db_insert_data('stock_local_dealer', df)


def db_insert_data(table_name, df, table_yaml=TABLE_LIST, engine=ENGINE):
    if table_name == 'stock_id':
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


def db_get_exist(table_name, table_yaml=TABLE_LIST, engine=ENGINE):
    with open(table_yaml, 'r', encoding='utf-8-sig') as file:
        table_list = yaml.safe_load(file)
    check_key = table_list[table_name]['check_key']
    get_cols = ",".join(check_key)
    query = "SELECT {} FROM {}".format(get_cols, table_name)
    result = engine.execute(query)
    result = list(dict.fromkeys(result))
    result = [item[:len(check_key)] for item in result]
    return result


def get_data(sql, engine=ENGINE):
    result = pd.read_sql(sql,engine)
    '''
    query = "SELECT * FROM {} WHERE date >= '{}'".format(table_name, date)
    result = engine.execute(query)
    result = [i for i in result]
    '''
    return result
#fundamental_get_exist('taiwanstockcashflows')
#get_stock_id()
#init_local_dealer(r'C:\Users\mick7\PycharmProjects\twse-captcha-breaker\stock_local_dealer')
#init_tables()
#data_empty('stock_id')
#get_data('stock_price','2022-03-01')