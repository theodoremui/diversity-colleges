#########################################################################
#
# ourrequests.py
#
# @author: Phil Mui
# @email: thephilmui@gmail.com
# @date: Mon Jan 23 16:45:56 PST 2023
#
# Retry logic: bit.ly/requests-retry
#########################################################################

import os
import random
import time

import aiohttp
import asyncio
import requests
from requests.adapters import HTTPAdapter, Retry
from fake_useragent import UserAgent
BAD_HTTP_CODES = (400,401,403,404,406,408,409,410,429,500,502,503,504)
USER_AGENT = UserAgent()

HTTP_RETRIES = 6
SCRAPINGDOC_API_KEY=os.environ.get('SCRAPINGDOC_API_KEY')
SCRAPINGDOG_PROXY='https://api.scrapingdog.com/scrape'
SCRAPINGDOG_PAYLOAD={'api_key': SCRAPINGDOC_API_KEY, 
                    'url': '', 'wait':'5000', 'session_number':''}

COUNTRY_CODE=['us','eu']
SCRAPERAPI_API_KEY=os.environ.get('SCRAPERAPI_API_KEY')
SCRAPERAPI_DEVICETYPE=['desktop', 'mobile']
SCRAPERAPI_PROXY='http://api.scraperapi.com'
SCRAPERAPI_PAYLOAD={'api_key': SCRAPERAPI_API_KEY,
                    'url': '', 'device_type': ''}

HEADERS  = [
{ 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Android 4.4; Mobile; rv:41.0) Gecko/41.0 Firefox/41.0' },
{ 'User-Agent': 'Mozilla/5.0 (Android 4.4; Tablet; rv:41.0) Gecko/41.0 Firefox/41.0' },
{ 'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0' },
{ 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0' },
{ 'User-Agent': 'Mozilla/5.0 (Maemo; Linux armv7l; rv:10.0) Gecko/20100101 Firefox/10.0 Fennec/10.0' },
{ 'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Focus/1.0 Chrome/59.0.3029.83 Mobile Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Focus/1.0 Chrome/59.0.3029.83 Safari/537.36' },
{ 'User-Agent': 'Mozilla/5.0 (Android 7.0; Mobile; rv:62.0) Gecko/62.0 Firefox/62.0' },
]

def requestWithProxy(url, numRetries=5):
    html = ""

    # ScrapingDog
    SCRAPINGDOG_PAYLOAD['url'] = url
    SCRAPINGDOG_PAYLOAD['session_number'] = random.randint(0,9999)
    result = requests.get(SCRAPINGDOG_PROXY, params=SCRAPINGDOG_PAYLOAD)
    if result.status_code == 200: 
        html = result.text.strip()
        print(f"\tpr1: {len(html)}: {url}")

    # ScraperAPI
    if result.status_code != 200 or len(html) < 10:
        SCRAPERAPI_PAYLOAD['url'] = url
        SCRAPERAPI_PAYLOAD['device_type'] = random.choice(SCRAPERAPI_DEVICETYPE)
        SCRAPERAPI_PAYLOAD['country_code'] = random.choice(COUNTRY_CODE)
        SCRAPERAPI_PAYLOAD['session_number'] = random.randint(0,9999)
        # SCRAPERAPI_PAYLOAD['render'] = 'true'
        result = requests.get(url = SCRAPERAPI_PROXY, params = SCRAPERAPI_PAYLOAD)
        if result != None: html = result.text.strip()
        print(f"\tpr2: {len(html)}: {url}")
    return html

async def asyncGet(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            # Get the response status code
            status = response.status
            # Read the response body
            body = await response.text().strip()
            print(f"\tasy: {len(body)}: {url}")
            # print(f"\tpre: {status}: {body[:50]} ...")

HTTP_RETRIES = 5
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
CHROME_OPTIONS = Options()
CHROME_PREFS = {"profile.managed_default_content_settings.images": 2,
                "profile.managed_default_content_settings.javascript": 2}
CHROME_OPTIONS.add_argument("--disable-javascript")
CHROME_OPTIONS.add_argument("--disable-extensions")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_experimental_option("prefs", CHROME_PREFS)
CHROME_OPTIONS.headless = True
CHROME_DRIVER = webdriver.Chrome(options=CHROME_OPTIONS)
# CHROME_DRIVER.quit()

def requestWithChrome(url, waitSeconds=1):
    try:
        CHROME_DRIVER.implicitly_wait(waitSeconds)
        CHROME_DRIVER.set_page_load_timeout(waitSeconds)
        CHROME_DRIVER.get(url)
    except Exception as e:
        print(f"\t{str(e)[:65]}")
    html = ""
    if CHROME_DRIVER.page_source != None:
        html = CHROME_DRIVER.page_source.encode("utf-8").strip()
    print(f"\tchr: {len(html)}: {url}: ...{html[-7:]}")
    return html

def requestWithRetry(url, numRetries=HTTP_RETRIES):
    html = ""
    s = requests.Session()
    retries = Retry(total=2*numRetries,
                    connect=numRetries,
                    read=numRetries,
                    backoff_factor=0.1,
                    status_forcelist=BAD_HTTP_CODES)
    adapter = HTTPAdapter(max_retries=retries)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    html = ""
    try:
        response = s.get(url, headers=random.choice(HEADERS))
        response.raise_for_status()
        html = response.text.strip()
    except Exception as e:
        print(f"\trequest failed for {url}: {e}")
    s.close()
    print(f"\treq: {len(html)}: {url}: ...{html[-7:]}")

    return html

def requestHtml(url, attempt, useProxy=False):
    html = ""
    try: # selenium chrome is slow -- use judiciously
        if attempt >=2 and attempt <= 3:
            html = requestWithChrome(url, waitSeconds=(attempt-1))
        elif useProxy:
            html = requestWithProxy(url, HTTP_RETRIES)
        else:
            html = requestWithRetry(url, HTTP_RETRIES)
    except Exception as e: 
        print(f"\tex: {str(e)[:70]}")

    return html



