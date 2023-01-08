#########################################################################
# wellesley2parquet.py
# ------------------
# Scraping Wellesley College student paper opinion pieces with proxy
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
import random
import sys
import time
import asyncio
import pandas as pd
from datetime import datetime
from bs4 import *

import ouraws
import ourrequests
RETRIES = 6
CHECKPOINT_FREQUENCY = 10 # every 10 pages

OUTPUT_DIR="data"
SCHOOL="wellesley"
SUBJECT="opinions"
CHECKPOINT_FILENAME  = f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"

LISTING_BASE_URL = f"https://thewellesleynews.com/category/{SUBJECT}/page/"

DATE_FORMAT="%B %d, %Y"
def getArticleText(url, numRetries, useProxy=False):
    attempts = 0
    title = ""
    content = ""
    dateObj = None
    while attempts <= numRetries and len(content) < 5: 
        # only use proxy if we have tried and failed in attempt 0
        if attempts > 3: useProxy = True
        html = ourrequests.requestHtml(url, attempts, useProxy)
        soup = BeautifulSoup(html, 'html.parser')
        titleObj   = soup.select_one("article header h1.entry-title")
        contentObj = soup.select_one("article div.wrapper div[itemprop^='articleBody']")
        dateObj    = soup.select_one("article header time[itemprop^='datePublished']")
        if titleObj is not None: title = titleObj.text.strip('"')
        if contentObj is not None:
            content = title + "\n" + contentObj.text.strip()
        if dateObj is not None: 
            dateObj = datetime.strptime(dateObj.text.strip('"'), DATE_FORMAT)
        attempts += 1

        # give the website a small break before next ping
        time.sleep(random.randint(0, 100 * attempts) / 1000.0)

    print(f"\t\t\t{len(content)} ...{content[-18:]}")
    return content, dateObj

def getArticleList(listUrl, numRetries, showProgress=False, useProxy=False):
    ''' Get articles linked off listing pages
        Retry logic: bit.ly/requests-retry
    '''
    articleList = []
    attempts = 0
    while attempts <= numRetries and len(articleList) == 0:
        # only use proxy if we have tried and failed in attempt 0
        if attempts > 3: useProxy = True
        html = ourrequests.requestHtml(listUrl, attempts, useProxy)
        soup = BeautifulSoup(html, "html.parser")
        articleList = soup.select("article h2.entry-title[itemprop^='headline'] > a[href^='https']")
        if showProgress: print(f"\tretrieved: {len(articleList)}")
        attempts += 1
        if len(articleList) == 0:
            # give the website a small break before next ping
            time.sleep(random.randint(0, 100 * attempts) / 1000.0)

    return articleList

def getArticles(baseURL, pageList, showProgress=False, useProxy=False):
    failedPages = []
    articles = []
    dateObj = None
    for pageNumber in pageList:
        articleList = getArticleList(baseURL+str(pageNumber), 
                                     RETRIES, showProgress, useProxy)
        wordCount = 0
        if len(articleList) == 0: failedPages.append(pageNumber)
        else:
            for article in articleList:
                url = article.get('href')
                if article.text != None and len(article.text) > 0:
                    body, dateObj = getArticleText(url, RETRIES)
                    articles.append({ 
                        'title' : article.text,
                        'url'   : url,
                        'body'  : body,
                        'year'  : dateObj.year,
                        'month' : dateObj.month,
                        'day'   : dateObj.day
                    })
                    wordCount += body.count(' ')
        if pageNumber % CHECKPOINT_FREQUENCY == 0: 
            ouraws.saveNewArticles(articles, checkpoint_name=CHECKPOINT_FILENAME)
        if showProgress: 
            print("-> {} : {} : {} : {} : {}"
                  .format(pageNumber, len(articleList), wordCount, 
                          baseURL+str(pageNumber), dateObj))
            print(f"\t{failedPages}")
        pageNumber += 1
    df = ouraws.saveNewArticles(articles, checkpoint_name=CHECKPOINT_FILENAME)
    return df, failedPages

def startProcessing(startPage, endPage, numRetries):
    df, failedPages = getArticles(LISTING_BASE_URL, range(startPage,endPage+1), 
                                  showProgress=True)
    while len(failedPages) > 0 and numRetries > 0:
        retry_df, failedPages = getArticles(LISTING_BASE_URL, 
                                            failedPages, 
                                            showProgress=True,
                                            useProxy=True)
        # merging together retrieved articles with newly retrieved ones
        if not retry_df.empty: 
            df = df[~ df['url'].isin(retry_df['url'])]
            df = pd.concat([df, retry_df])
        print(f"failed pages: {failedPages}")
        numRetries -= 1

    # DataFrame df has dimensions of (# articles, #attributes)
    print("Total articles: {}, each with attributes: {}"
            .format(df.shape[0], df.shape[1]))

    # In reverse order: earlier article is last row: df.iloc[-1]
    print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
    print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")

    ouraws.saveByYear(df, output_dir=OUTPUT_DIR, 
                    prefix=f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}")


def printUsage(progname):
    print("Usage: python {} <startPage> <endPage> <numRetries>".format(
        progname
    ))

#=====================================================================
# main
#=====================================================================
if __name__ == "__main__":

    if len(sys.argv) != 4:
        printUsage(sys.argv[0]) 
        sys.exit(0)

    try:
        startPage  = int(sys.argv[1])
        endPage    = int(sys.argv[2])
        numRetries = int(sys.argv[3])
        print(f"Processing {SCHOOL} {SUBJECT} from {startPage} to {endPage}")
        startProcessing(startPage, endPage, numRetries)
    except Exception as e:
        print(f"Exception: {e}")


#############
# Testing getting a specific content page or list of content
#############
# if __name__ == "__main__":
#     url = 'https://dailycal.org/2016/10/20/solidarity-and-space'
#     text = getArticleText(url, 2)
#
#     print(f"article: {text}")

#############
# Testing proxy for getting list of articles
#############
# if __name__ == "__main__":
#     articleList = getArticleList(LISTING_BASE_URL+str(230), 
#                                  HTTP_RETRIES, True)
#     print(len(articleList))
#     print("\n".join([a.get('href') for a in articleList]))
