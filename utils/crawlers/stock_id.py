import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from utils.sqlite import database

driver = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\chromedriver.exe'
save_path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\data\stock_id.csv'
db_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_data.db'

def exist_path(browser, xpath):
    try:
        browser.find_element(By.XPATH, value=xpath)
        return True
    except:
        return False


def update_stock_id(driver, db_dir, hide=True):
    url = 'https://mops.twse.com.tw/mops/web/t51sb01'
    stock_status_xpath = '//*[@id="search"]/table/tbody/tr/td/select[1]'
    stokc_types = '//*[@id="search"]/table/tbody/tr/td/select[2]'
    options = webdriver.ChromeOptions()
    if hide:
        options.add_argument('headless')
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
    browser = webdriver.Chrome(executable_path=driver, options=options)
    browser.get(url)
    if exist_path(browser, stock_status_xpath):
        result = []
        for status in ['上市', '上櫃']:
            Select(browser.find_element(By.XPATH, value=stock_status_xpath)).select_by_visible_text(status)
            Select(browser.find_element(By.XPATH, value=stokc_types)).select_by_visible_text('') #select empty = all
            browser.find_element(by=By.XPATH, value='//*[@id="search_bar1"]/div/input').click()
            time.sleep(15)
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            div = soup.select_one("div#table01")
            table = pd.read_html(str(div))
            table = table[0]
            table = table[table['公司名稱'] != '公司名稱']
            result.append(table)
        result = pd.concat(result)
        result = result.rename(columns={"公司代號": "stock_id"})
        db = database(db_dir)
        if db.table_check('stock_id'):
            db_stock_id = db.get_stock_id()
            print('Current numbers of stock_id: {}'.format(len(result)))
            print('Exist numbers of stock_id: {}'.format(len(db_stock_id)))
            if len(result) != len(db_stock_id):
                print('Update stock list')
                db.insert_data(result, 'stock_id')
            else:
                print('Stock list unchanged')
        else:
            db.insert_data(result, 'stock_id')
    browser.close()
    browser.quit()
        #result.to_csv(save_path, encoding='utf-8-sig',index=False)


if __name__ == '__main__':
    update_stock_id(driver, db_dir)
