import pandas as pd
from datetime import datetime
from tqdm import tqdm
from utils.postgredb import db_get_stock_id, db_insert_data, db_get_exist
import requests
import json
import os
TABLE_LIST = os.path.join('..', 'table_list.yaml')

def crawl_price(crawl_data, table_name='stock_price', table_yaml=TABLE_LIST):
    stock_id_list = get_undo_stock(table_name, crawl_data, table_yaml=table_yaml)
    for i in tqdm(stock_id_list):
        result = TaiwanStockPrice(i)
        db_insert_data(table_name, result, table_yaml=table_yaml)


def TaiwanStockPrice(stock_id):
    url = 'https://tw.quote.finance.yahoo.net/quote/q?type=ta&perd=d&mkt=10&sym={}&v=1'.format(stock_id)
    content = requests.get(url).text
    content = content[5:-2]
    content = '{' + content[content.find('"ta":'):]
    content = content.replace(',}', '}')
    content = json.loads(content)
    content = content["ta"]
    result = pd.DataFrame(content)
    if not result.empty:
        result.columns = ['date', 'open', 'max', 'min', 'close', 'trading_turnover']
        result['stock_id'] = stock_id
    result['date'] = pd.to_datetime(result['date'],format='%Y%m%d')
    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
    return result


def get_undo_stock(table_name, today=datetime.today().strftime('%Y-%m-%d'), table_yaml=TABLE_LIST):
    stock_id_list = db_get_stock_id()
    result = db_get_exist(table_name, table_yaml=table_yaml)
    done = [i[0] for i in result if i[1] == today]
    result = list(set(stock_id_list) - set(done))
    return result

