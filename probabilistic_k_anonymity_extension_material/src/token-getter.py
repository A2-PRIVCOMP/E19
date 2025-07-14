from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

import pymongo
import sys
driver_path = '/home/linkedin-angel2/Documents/prokanon/chromedriver-linux64/chromedriver'
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
driver = webdriver.Chrome(options=options)

user = sys.argv[1]
passwd = sys.argv[2]
act_id = sys.argv[3]
adset_id = sys.argv[4]

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['prokanon']
tokens_col = db['tokens']

driver.set_window_size(1024, 2000)
driver.get("https://facebook.com")
time.sleep(2)
print(user)
try:
    accept_button = driver.find_element(By.XPATH, '//*[@data-cookiebanner="accept_button"]')
    accept_button.click()
except:
    pass
time.sleep(2)
email_input = driver.find_element(By.XPATH, '//*[@id="email"]')
email_input.send_keys(user)
time.sleep(2)
passwd_input = driver.find_element(By.XPATH, '//*[@id="pass"]')
passwd_input.send_keys(passwd)
time.sleep(2)
login_button = driver.find_element(By.XPATH, '//*[@name="login"]')
login_button.send_keys(Keys.RETURN)
time.sleep(2)
driver.save_screenshot('cap.png')
driver.get(f'https://adsmanager.facebook.com/adsmanager/manage/adsets/edit?act={act_id}&selected_adset_ids={adset_id}&breakdown_regrouping=1&nav_source=no_referrer')

for request in driver.requests:
    if 'delivery_estimate' in request.url:
        access_token = request.url.split('access_token=')[1].split('&')[0]
        cookies = request.headers['cookie']
        tokens_col.update_one({'user': user}, {"$set": {'token':access_token,'cookies':cookies}}, upsert=True)
        print(access_token)
