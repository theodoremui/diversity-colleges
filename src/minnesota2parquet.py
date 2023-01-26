import os
import re
import requests
import random
import time
import pandas as pd
from urllib.parse import urlparse
from bs4 import *

SCHOOL = "minnesota"
SUBJECT = "opinion"
OUTPUT_DIR = "./data"
DATE_PATTERN = re.compile("opinion/opinion")
BASE_URL = f"https://mndaily.com/category/{SUBJECT}/page/"
HEADER = {'User-Agent': 'Mozilla/5.0'}

SCHEMA = {'url': str, 'body': str,
          'year': int, 'month': int, 'day': int}

def getArticleText(url):
    html = requests.get(url, headers=HEADER).text
    soup = BeautifulSoup(html, 'html.parser')
    lines = soup.get_text().splitlines()
    output = ""

    innerBackground = soup.find(class_ = "innerbackground")
    timeWrapper = innerBackground.find(class_ = "time-wrapper")
    dateText = str(timeWrapper)

    year = re.search("20[0-9][0-9]", dateText)

    for line in lines:
        words = re.split('[\s\t]', line)
            # only save those lines with at least 3 words
        if len(words) > 3:output += line + '\n'

    return (output, year.group(0))

def getArticles():
    df = pd.DataFrame(columns=SCHEMA.keys()).astype(SCHEMA)
    domainName = urlparse(BASE_URL).netloc
    pageNumber = 1

    while pageNumber < 100:
        urlSet = set()
        print (pageNumber)
        html = requests.get(BASE_URL+str(pageNumber), headers=HEADER).text
        print (BASE_URL+str(pageNumber))
        soup = BeautifulSoup(html, "html.parser")

        for link in soup.find_all('a'):
            linkUrl = link.get('href')
            domain = urlparse(linkUrl).netloc
            if (domain == domainName):
                if linkUrl not in df.values:
                   if ("opinion" in linkUrl):
                        urlSet.add(linkUrl)

        for url in urlSet:
            date_groups = DATE_PATTERN.search(url)
            text,year = getArticleText(url)

            if text != None:
                 if date_groups:
                    a = {'url': url,
                         'body': text,
                         'year': int(year),
                         'month': 1,
                         'day': 1 }
                    print (year, url)

                    a_df = pd.DataFrame(a, index=[url])
                    df = pd.concat([df, a_df])

        pageNumber += 1

    return (df)


def saveByYear(df):
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    latest_year = df.iloc[-1][-3]
    oldest_year= df.iloc[0][-3]

    for y in range(oldest_year, latest_year + 1):
        df[df.year == y].to_parquet(f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-{y}.parquet")
        print (str(y) + " saved")


if __name__ == "__main__":
   df = getArticles()
   df.sort_values('year')

    # In reverse order: earlier article is last row: df.iloc[-1]
   print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
   print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")
   saveByYear(df)