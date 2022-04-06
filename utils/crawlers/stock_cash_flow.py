import time
from tqdm import tqdm
import numpy as np
import pandas as pd
import os
import glob
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from ..sqlite import database

driver = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\chromedriver.exe'
save_path = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_datas\TEST.csv'
db_dir = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_data.db'

def run_crawler(driver, stock_id_list, db_dir, hide=False):
    options = webdriver.ChromeOptions()
    if hide:
        options.add_argument('headless')
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome = webdriver.Chrome(executable_path=driver, options=options)
    ans = []
    year_q = get_y_q()
    db = database(db_dir)
    for i in tqdm(stock_id_list[1350:]):
        for j in year_q:
            df = TaiwanStockCashFlows(chrome, i, j)
            if df:
                db.insert_data(df, 'CashFlows')
                df = False
            

    chrome.close()
    chrome.quit()
    return ans


def xpath_exist(browser,xpath):
    try:
        browser.find_element(By.XPATH, value=xpath)
        return True
    except:
        return False


def TaiwanStockCashFlows(browser, stock_id, y_q):
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb05'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(y_q[0])
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(str(y_q[1]))
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return False
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
            table['year'] = y_q[0]
            table['quarter'] = y_q[1]
            table['stock_id'] = stock_id
            return table
        except Exception as e:
            print(e)
            time.sleep(15)


def TaiwanStockBalance(browser, stock_id, y_q):
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb03'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(y_q[0])
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(str(y_q[1]))
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return False
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
            table['year'] = y_q[0]
            table['quarter'] = y_q[1]
            table['stock_id'] = stock_id
            return table
        except Exception as e:
            print(e)
            time.sleep(15)


def TaiwanStockFinancial(browser, stock_id, y_q):
    while True:
        try:
            url = 'https://mops.twse.com.tw/mops/web/t164sb04'
            browser.get(url)
            Select(browser.find_element(By.ID, value='isnew')).select_by_visible_text("歷史資料")
            browser.find_element(By.ID, value='co_id').send_keys(stock_id)
            browser.find_element(By.ID, value='year').send_keys(y_q[0])
            Select(browser.find_element(By.ID, value='season')).select_by_visible_text(str(y_q[1]))
            browser.find_element(by=By.CSS_SELECTOR, value="#search_bar1 > div > input[type=button]").click()
            time.sleep(2)
            if xpath_exist(browser, '//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input') == True:
                browser.find_element(by=By.XPATH, value='//*[@id="fm1"]/table/tbody/tr[2]/td[3]/input').click()
                time.sleep(2)
            if xpath_exist(browser, '//*[@id="table01"]/center/h4/font') == True:
                status = browser.find_element(By.XPATH, value='//*[@id="table01"]/center/h4/font').text
                if '查無所需資料' in status or '第二上市' in status:
                    return False
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
            table['year'] = y_q[0]
            table['quarter'] = y_q[1]
            table['stock_id'] = stock_id
            return table
        except Exception as e:
            print(e)
            time.sleep(15)


def get_y_q():
    y_now = pd.Timestamp(datetime.today()).year - 1911
    q_now = pd.Timestamp(datetime.today()).quarter
    y_start = 2021 - 1911
    q_start = pd.Timestamp('2021-05-27').quarter
    result = []
    while y_start != y_now or q_start != q_now:
        result.append([y_start, q_start])
        q_start += 1
        if q_start == 5:
            y_start += 1
            q_start = 1
    return(result)


if __name__ == '__main__':
    stock_id = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\raw_datas\stock_id.csv'
    df = pd.read_csv(stock_id)
    stock_id = df.stock_id.tolist()
    run_crawler(driver ,stock_id ,db_dir,False)
