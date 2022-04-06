from utils.crawlers import stock_id, fundamental
import os

DB_DIR = os.path.join('.','raw_data.db')
DRIVER = os.path.join('.','chromedriver.exe')


if __name__ == '__name__':
  stock_id.update_stock_id(DRIVER, DB_DIR)
  fundamental.run_crawler(DRIVER, DB_DIR, False)
  
