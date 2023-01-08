#########################################################################
# stanford2parquet.py
# ------------------
# Scraping Stanford University student paper opinion pieces with proxy 
# services & saving to S3
# 
# @author Theodore Mui
# @email theodoremui@gmail.com
# @date Sat Jan  7 14:23:24 PST 2023
#
# Retry logic: bit.ly/requests-retry
#########################################################################

import os
import re
import requests
import random
import time
import pandas as pd

from bs4 import *

SCHOOL="stanford"
SUBJECT="opinions"
OUTPUT_DIR="./data"

DATE_PATTERN = re.compile("https://stanforddaily.com/(\d+)/(\d+)/(\d+)")
BASE_URL = f"https://stanforddaily.com/category/{SUBJECT}/page/"
HEADER  = { 'User-Agent': 'Mozilla/5.0' }
SCHEMA = {'title': str, 'url': str, 'body': str, 
          'year': int, 'month': int, 'day': int}

def getArticleText(url):
    html = requests.get(url, headers=HEADER).text
    soup = BeautifulSoup(html, 'html.parser')
    lines = soup.get_text().splitlines()
    output = ""
    for line in lines:
        words = re.split('[\s\t]', line)
        # only save those lines with at least 3 words
        if len(words) > 3: output += line + '\n'
    return output

def getArticles(baseURL, numPages=248, showProgress=False):
    
    df = pd.DataFrame(columns=SCHEMA.keys()).astype(SCHEMA)
    pageNumber = 1
    hasArticles = True
    while hasArticles and pageNumber <= numPages:
        html = requests.get(baseURL+str(pageNumber), headers=HEADER).text
        soup = BeautifulSoup(html, "html.parser")
        articleList = soup.select("div > h3 > a[href^='http']")

        if showProgress: print(f"-> {pageNumber} : {len(articleList)}")
        if len(articleList) <= 0: hasArticles = False
        if hasArticles:
            for article in articleList:
                url = article.get('href')
                date_groups = DATE_PATTERN.search(url)
                # print(f"\t{article.text} : {url}: {date_groups.group(1)}-{date_groups.group(2)}-{date_groups.group(3)}")

                if article.text != None and len(article.text) > 0:
                    a = { 'title' : article.text,
                          'url'   : url,
                          'body'  : getArticleText(url),
                          'year'  : int(date_groups.group(1)), # year
                          'month' : int(date_groups.group(2)), # month
                          'day'  : int(date_groups.group(3))  # day
                    }
                    a_df = pd.DataFrame(a, index=[url])
                    df = pd.concat([df, a_df])
            pageNumber += 1
            # give the website a small break before next ping
            time.sleep(random.randint(0, 100) / 100.0)

    return df

def saveByYear(df):
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    oldest_year = df.iloc[-1][-3]
    latest_year = df.iloc[0][-3]

    for y in range(oldest_year, latest_year+1):
        print(f"{y} has {df[df.year == y].shape[0]} articles")
        df[df.year == y].to_parquet(f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-{y}.parquet")

if __name__ == "__main__":
    df = getArticles(BASE_URL, numPages=248, showProgress=True)

    # DataFrame df has dimensions of (# articles, #attributes)
    print(f"Total articles: {df.shape[0]}, each with attributes: {df.shape[1]}")

    # In reverse order: earlier article is last row: df.iloc[-1]
    print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
    print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")

    saveByYear(df)


