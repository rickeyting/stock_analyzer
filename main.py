from utils.crawlers import stock_id, fundamental, banks_holder
import os

DB_DIR = os.path.join('.', 'raw_datas.db')
DRIVER = os.path.join('.', 'chromedriver.exe')


if __name__ == '__main__':
    #stock_id.update_stock_id(DRIVER, DB_DIR, False)
    fundamental.crawl_fundamental(DRIVER, DB_DIR)
    banks_holder.crawl_banks_holder(DRIVER, DB_DIR)
