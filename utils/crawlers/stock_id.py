import pandas as pd
from bs4 import BeautifulSoup
import requests
from utils.postgredb import db_insert_data

def crawl_stock_id():
    url = 'https://mops.twse.com.tw/mops/web/t51sb01'
    result = []
    for i in ['sii', 'otc']:
        header = {"encodeURIComponent": 1, "step": 1, "firstin": 1, "TYPEK": i, "code": ""}
        request = requests.post(url, data=header)
        soup = BeautifulSoup(request.text, 'html.parser')
        div = soup.select_one("div#table01")
        table = pd.read_html(str(div))
        table = table[0]
        table = table[table['公司名稱'] != '公司名稱']
        table['market_type'] = i
        result.append(table)
    result = pd.concat(result)
    result = result.rename(columns={'公司代號': 'stock_id'})
    if not result.empty:
        db_insert_data('stock_id', result)



if __name__ == '__main__':
    crawl_stock_id()
