import time
import base64
import pytesseract
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import cv2
from selenium import webdriver
from datetime import datetime, timedelta
from utils.postgredb import db_get_stock_id, db_insert_data, db_get_exist
import os
TABLE_LIST = os.path.join('..', 'table_list.yaml')
driver = r'C:\Users\mick7\PycharmProjects\stock_analyzer\stock_analyzer\chromedriver.exe'

def preprocess(image_gray):
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    image_opening = cv2.morphologyEx(image_gray, cv2.MORPH_OPEN, kernel, iterations=1)
    image_blur = cv2.GaussianBlur(image_opening, (5, 5), 0)
    _, image_thresh = cv2.threshold(image_blur, 180, 255, cv2.THRESH_BINARY)
    num_components, components, stats, _ = cv2.connectedComponentsWithStats(image_thresh,
                                                                            None, None, None, 4, cv2.CV_32S)
    sizes = stats[:, -1]
    image_connected = np.zeros(components.shape, np.uint8)
    for i in range(1, num_components):
        if sizes[i] >= 64:
            image_connected[components == i] = 255

    kernel = np.ones((2, 2), np.uint8)
    image_dilation = cv2.dilate(image_connected, kernel, iterations=1)
    return image_dilation


def image_to_string(image):
    custom_config = r'--oem 2 --psm 7'
    s = pytesseract.image_to_string(image, config=custom_config)
    punctuations = " !()-[]{};:\'\"\\,<>./?@#$%^+^&*_~‘"
    for punctuation in punctuations:
        s = s.replace(punctuation, '')
    s = s.split('\n')[0].upper()
    if len(s) != 5:
        custom_config = r'--oem 1 --psm 7'
        s = pytesseract.image_to_string(image, config=custom_config)
        punctuations = " !()-[]{};:\'\"\\,<>./?@#$%^&*_~‘"
        for punctuation in punctuations:
            s = s.replace(punctuation, '')
        s = s.split('\n')[0].upper()
    if len(s) != 5:
        custom_config = r'--oem 0 --psm 7'
        s = pytesseract.image_to_string(image, config=custom_config)
        punctuations = " !()-[]{};:\'\"\\,<>./?@#$%^&*_~‘"
        for punctuation in punctuations:
            s = s.replace(punctuation, '')
        s = s.split('\n')[0].upper()
    return s


def current_time():
    return '[{}]'.format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def get_captcha_text(browser):
    captcha_element = browser.find_element_by_xpath(
        "//*[@id='Panel_bshtm']/table/tbody/tr/td/table/tbody/tr[1]/td/div/div[1]/img")
    img_captcha_base64 = browser.execute_async_script("""
        var ele = arguments[0], callback = arguments[1];
        ele.addEventListener('load', function fn(){
          ele.removeEventListener('load', fn, false);
          var cnv = document.createElement('canvas');
          cnv.width = this.width; cnv.height = this.height;
          cnv.getContext('2d').drawImage(this, 0, 0);
          callback(cnv.toDataURL('image/jpeg').substring(22));
        }, false);
        ele.dispatchEvent(new Event('load'));
        """, captcha_element)
    image_bytes = base64.b64decode(img_captcha_base64)
    image_array = np.frombuffer(image_bytes, dtype=np.uint8)
    image = cv2.imdecode(image_array, flags=cv2.IMREAD_GRAYSCALE)
    image = preprocess(image)
    text = image_to_string(image)
    return text


def extract_data(soup):
    tables = []
    left_tables = soup.select('td [valign="top"]')
    right_tables = soup.select('td [align="left"]')
    for i in range(len(left_tables)):
        tables.append(pd.read_html(str(left_tables[i]))[0])
    for i in range(len(right_tables)):
        tables.append(pd.read_html(str(right_tables[i]))[0])
    # print("{} stock id {} tables extracted".format(current_time(), stock_id))
    if not tables:
        return pd.DataFrame()
    data = pd.concat(tables).iloc[:-1]
    # print("{} stock id {} tables concat".format(current_time(), stock_id))
    data.columns = ['order', 'bank', 'price', 'buy_shares', 'sell_shares']
    data = data[data['order'] != '序']
    data['order'] = data['order'].astype('int')
    data = data.sort_values(by='order')
    data = data.rename(columns={'order': 'order_num'})
    data.reset_index(inplace=True, drop=True)
    return data


def get_data(browser, stock_id):
    data = pd.DataFrame()
    POST_URL = 'https://bsr.twse.com.tw/bshtm/bsMenu.aspx'
    browser.get(POST_URL)
    # time.sleep(1)
    browser.find_element_by_id("TextBox_Stkno").send_keys(stock_id)
    check_text = '驗證碼錯誤!'

    while check_text and check_text != '查無資料':
        text = None
        while not text:
            try:
                text = get_captcha_text(browser)
            except Exception as e:
                print('{} Cannot get captcha image, {}'.format(current_time(), e))
                browser.refresh()
                browser.find_element_by_id("TextBox_Stkno").send_keys(stock_id)

        browser.find_element_by_name("CaptchaControl1").send_keys(text)
        browser.find_element_by_id("btnOK").click()
        time.sleep(1)
        check_text = browser.find_element_by_id("Label_ErrorMsg").text
        if not check_text:
            print("{} Submit stock id {} and validation code {}: {}".format(current_time(), stock_id, text, '查詢成功'))
        else:
            print("{} Submit stock id {} and validation code {}: {}".format(current_time(), stock_id, text, check_text))

    if check_text == "查無資料":
        return data
    DATA_URL = 'https://bsr.twse.com.tw/bshtm/bsContent.aspx?v=t'
    browser.get(DATA_URL)
    # print("{} stock id {} html page received".format(current_time(), stock_id))
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    # print("{} stock id {} bs4 parsing html done".format(current_time(), stock_id))
    data = extract_data(soup)
    # print("{} stock id {} tables cleaned".format(current_time(), stock_id))
    table_date = browser.find_element_by_id("receive_date").text.split('/')
    table_date = '-'.join(table_date)
    table_stock_id = browser.find_element_by_id("stock_id").text[:4]
    #table_date = datetime.strptime(table_date, '%Y-%m-%d')
    #data['date'] = table_date
    data['date'] = table_date
    data['stock_id'] = table_stock_id
    return data


def crawl_banks_holder(driver, table_name='stock_local_dealer', hide=True, table_yaml=TABLE_LIST):
    options = webdriver.ChromeOptions()
    if hide:
        options.add_argument('headless')
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome = webdriver.Chrome(executable_path=driver, options=options)
    stock_id_list = get_undo_stock(table_name, '2022-04-08', table_yaml=table_yaml)
    for i in range(len(stock_id_list)):
        code = stock_id_list[i]
        print("{} Download stock id {} data ({}/{})".format(current_time(), code, i + 1, len(stock_id_list)))
        df = get_data(chrome, code)
        if not df.empty:
            print('{} Data save at {}'.format(current_time(), code))
            db_insert_data(table_name, df, table_yaml=table_yaml)
        else:
            print('{} No data found, invalid stock id.'.format(current_time()))
        time.sleep(1)

    chrome.close()
    chrome.quit()


def get_undo_stock(table_name,today=datetime.today().strftime('%Y-%m-%d'), table_yaml=TABLE_LIST):
    stock_id_list = db_get_stock_id('sii')
    result = db_get_exist(table_name, table_yaml=table_yaml)
    done = [i[0] for i in result if i[1] == today]
    result = list(set(stock_id_list) - set(done))
    return result


if __name__ == '__main__':
    crawl_banks_holder(driver)
