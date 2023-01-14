import os
import re
import requests
import random
import time
import pandas as pd
from urllib.parse import urlparse
from bs4 import *

SCHOOL = "boston"
SUBJECT = "opinions"
OUTPUT_DIR = "./data"
DATE_PATTERN = re.compile("https://www.bcheights.com/(\d+)/(\d+)/(\d+)")
#BASE_URL = f"https://www.bcheights.com/{SUBJECT}"
YEAR = 2022
BASE_URL = f"https://www.bcheights.com/{YEAR}/page/"
HEADER = {'User-Agent': 'Mozilla/5.0'}

SCHEMA = {'url': str, 'body': str,
          'year': int, 'month': int, 'day': int}

def getArticleText(url):
    html = requests.get(url, headers=HEADER).text
    soup = BeautifulSoup(html, 'html.parser')

    #c = soup.find(class_ = "category")
    #category = str(c)
    #print (category)
    #if category.find('Opinions') != -1:
    lines = soup.get_text().splitlines()
    output = ""
    for line in lines:
        words = re.split('[\s\t]', line)
        # only save those lines with at least 3 words
        if len(words) > 3:output += line + '\n'
    return output

def getArticles():
    df = pd.DataFrame(columns=SCHEMA.keys()).astype(SCHEMA)
    domainName = urlparse(BASE_URL).netloc
    urlSet = set()
    year = "2021"

    for i in range (1):
        url = f"https://www.bcheights.com/{year}/page/"
        print (year)
        pageNumber = 2

        while pageNumber < 5:
            print (pageNumber)
            html = requests.get(url+str(pageNumber), headers=HEADER).text
            soup = BeautifulSoup(html, "html.parser")

            for link in soup.find_all('a'):
                linkUrl = link.get('href')
                domain = urlparse(linkUrl).netloc
                if (domain == domainName):
                    if ("-" in linkUrl):
                        urlSet.add(linkUrl)
            #print (urlSet)

            for url in urlSet:
                date_groups = DATE_PATTERN.search(url)
                text = getArticleText(url)
                if text != None:
                    if date_groups:
                        a = {'url': url,
                             'body': text,
                             'year': int(date_groups.group(1)),
                             'month': int(date_groups.group(2)),
                             'day': int(date_groups.group(3)) }
                        a_df = pd.DataFrame(a, index=[url])
                        df = pd.concat([df, a_df])

            pageNumber += 1

        yd = int(year)
        yd += 1
        year = str(yd)

    print (df)
    return (df)


def saveByYear(df):
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    oldest_year = df.iloc[-1][-3]
    latest_year = df.iloc[0][-3]

    for y in range(oldest_year, latest_year + 1):
        df[df.year == y].to_parquet(f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-{y}.parquet")
        print (str(y) + "saved")


if __name__ == "__main__":
   df = getArticles()
   #print(f"Total articles: {df.shape[0]}, each with attributes: {df.shape[1]}")

    # In reverse order: earlier article is last row: df.iloc[-1]
   print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
   print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")
   saveByYear(df)
