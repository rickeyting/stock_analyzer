import time
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from utils.postgredb import db_get_stock_id, db_insert_data, db_get_exist
import os
TABLE_LIST = os.path.join('..', 'table_list.yaml')
driver = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\chromedriver.exe'


def crawl_fundamental(driver, hide=True, table_yaml=TABLE_LIST):
    options = webdriver.ChromeOptions()
    if hide:
        options.add_argument('headless')
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome = webdriver.Chrome(executable_path=driver, options=options)
    ans = []
    year_q = get_undo_stock('taiwanstockcashflows', table_yaml=table_yaml)
    for stock_id, y, q in tqdm(year_q):
        # taiwanstockcashflows
        df = TaiwanStockCashFlows(chrome, stock_id, y, q)
        if not df.empty:
            db_insert_data('taiwanstockcashflows', df, table_yaml=table_yaml)
    year_q = get_undo_stock('taiwanstockbalance', table_yaml=table_yaml)
    for stock_id, y, q in tqdm(year_q):
        # taiwanstockbalance
        df = TaiwanStockBalance(chrome, stock_id, y, q)
        if not df.empty:
            db_insert_data('taiwanstockbalance', df, table_yaml=table_yaml)
    year_q = get_undo_stock('taiwanstockfinancial', table_yaml=table_yaml)
    for stock_id, y, q in tqdm(year_q):
        # taiwanstockfinancial
        df = TaiwanStockFinancial(chrome, stock_id, y, q)
        if not df.empty:
            db_insert_data('taiwanstockfinancial', df, table_yaml=table_yaml)
    chrome.close()
    chrome.quit()
    return ans


def xpath_exist(browser,xpath):
    try:
        browser.find_element(By.XPATH, value=xpath)
        return True
    except:
        return False


def TaiwanStockCashFlows(browser, stock_id, y, q):
    tw_y = str(y - 1911)
    tw_q = str(q)
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb05'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(tw_y)
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(tw_q)
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return pd.DataFrame()
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            div = soup.select_one("div#table01")
            table = pd.read_html(str(div))

            if len(table[1].columns) > 2:
                table = table[1]
            else:
                table = table[2]
            table = table.iloc[:, 0:2]
            table.columns = ['elements', 'values']
            table['year'] = y
            table['quarter'] = q
            table['stock_id'] = stock_id
            table['values'] = table['values'].fillna(-1)
            return table
        except Exception as e:
            time.sleep(15)


def TaiwanStockBalance(browser, stock_id, y, q):
    tw_y = str(y - 1911)
    tw_q = str(q)
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb03'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(tw_y)
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(str(tw_q))
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return pd.DataFrame()
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            div = soup.select_one("div#table01")
            table = pd.read_html(str(div))
            if len(table[1].columns) > 2:
                table = table[1]
            else:
                table = table[2]
            table = table.iloc[:, 0:2]
            table.columns = ['elements', 'values']
            table['year'] = y
            table['quarter'] = q
            table['stock_id'] = stock_id
            return table
        except Exception as e:
            time.sleep(15)


def TaiwanStockFinancial(browser, stock_id, y, q):
    tw_y = str(y - 1911)
    tw_q = str(q)
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb04'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(tw_y)
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(str(tw_q))
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return pd.DataFrame()
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            div = soup.select_one("div#table01")
            table = pd.read_html(str(div))
            if len(table[1].columns) > 2:
                table = table[1]
            else:
                table = table[2]
            table = table.iloc[:, 0:2]
            table.columns = ['elements', 'values']
            table['year'] = y
            table['quarter'] = q
            table['stock_id'] = stock_id
            return table
        except Exception as e:
            time.sleep(15)


def get_undo_stock(table_name, start_date='2021-05-27', table_yaml=TABLE_LIST):
    y_now = pd.Timestamp(datetime.today()).year
    q_now = pd.Timestamp(datetime.today()).quarter
    y_start = pd.Timestamp(start_date).year
    q_start = pd.Timestamp(start_date).quarter
    y_q_list = []
    while y_start != y_now or q_start != q_now:
        y_q_list.append([y_start, q_start])
        q_start += 1
        if q_start == 5:
            y_start += 1
            q_start = 1
    result = []
    stock_id_list = db_get_stock_id()
    for i in stock_id_list:
        for y, q in y_q_list:
            result.append((i, y, q))

    done = db_get_exist(table_name, table_yaml=table_yaml)
    result = list(set(result)-set(done))
    result.sort(key=sortstockid)
    #result = [list(i) for i in result]
    return result


def sortstockid(val):
    return val[0]


if __name__ == '__main__':
    #get_y_q('taiwanstockcashflows')
    crawl_fundamental(driver)
