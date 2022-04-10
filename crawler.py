from utils.crawlers import banks_holder, fundamental, stock_price, stock_id
from utils.postgredb import init_tables, update_banks_list
from datetime import datetime, timedelta
import os

driver = os.path.abspath('chromedriver.exe')
root = os.path.abspath('.')
TABLE_LIST = os.path.join(root, 'utils', 'table_list.yaml')
BANK_LIST = os.path.join(root, 'data', 'bank_id.csv')

def current_time():
    return '[{}]'.format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def crawler_time_arrange(table_yaml=TABLE_LIST):
    init_tables(table_yaml=table_yaml)
    update_banks_list(BANK_LIST, table_yaml=table_yaml)
    print("{} UPDATE STOCK ID".format(current_time()))
    stock_id.crawl_stock_id()

    weekday = datetime.today().weekday()
    if weekday > 5:
        print("{} CRAWLING FUNDAMENTAL DATA".format(current_time()))
        fundamental.crawl_fundamental(driver, table_yaml=table_yaml)

    print("{} CRAWLING PRICE DATA".format(current_time()))
    if weekday > 5:
        decrease = weekday - 4
    crawl_data = datetime.strftime(datetime.now() - timedelta(days=decrease), '%Y-%m-%d')
    stock_price.crawl_price(crawl_data, table_yaml=table_yaml)
    print("{} CRAWLING BANKS HOLDERS".format(current_time()))
    banks_holder.crawl_banks_holder(driver, table_yaml=table_yaml)


if __name__ == '__main__':
    crawler_time_arrange()

