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
RETRIES = 0
CHECKPOINT_FREQUENCY = 10 # every 10 pages

OUTPUT_DIR="data"
SCHOOL="middlebury"
SUBJECT="opinion" # check if this is 'opinion'
CHECKPOINT_FILENAME  = f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"

DATE_PATTERN = re.compile("https://www.middleburycampus.com/article/(\d+)/(\d+)")
LISTING_BASE_URL = f"https://www.middleburycampus.com/section/{SUBJECT}"

def getArticleText(url, numRetries, useProxy=False):
    attempts = 0
    title = ""
    body = ""
    while attempts <= numRetries and len(title) < 5 and len(body) < 5: 
        # only use proxy if we have tried and failed in attempt 0
        if attempts > 2: useProxy = True
        html = ourrequests.requestHtml(url, attempts, useProxy)
        soup = BeautifulSoup(html, 'html.parser')
#         bodyObj = soup.select_one("div[class^='article-content']") #for pages 1-5
        bodyObj  = soup.select_one("div[class^='imported article-content']") # for pages 7 and after
        if bodyObj != None:  body  = bodyObj.text
        attempts += 1

        # give the website a small break before next ping
        time.sleep(random.randint(0, 100 * attempts) / 100.0)

    text = title + "\n" + body.strip()
    print(f"\t\t\t{len(text)} ...{text[-18:]}")
    return text

ARTICLE_SELECTOR = \
    "div[class^='col-12 col-md-4'] > " + \
    "div[class^='image-container'] > " + \
    "a[href^='http']"

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
        articleList = soup.select(ARTICLE_SELECTOR)
        if showProgress: print(f"\tretrieved: {len(articleList)}")
        attempts += 1
        if len(articleList) == 0:
            # give the website a small break before next ping
            time.sleep(random.randint(0, 100 * attempts) / 100.0)

    return articleList

def getArticles(baseURL, pageList, showProgress=False, useProxy=False):
    failedPages = []
    articles = []
    for pageNumber in pageList:
        articleList = getArticleList(baseURL+"?page="+str(pageNumber)+"&per_page=20", RETRIES, showProgress, useProxy)
        wordCount = 0
        if len(articleList) == 0: 
            failedPages.append(pageNumber)
        else:
            for article in articleList:
                url = article.get('href')
                date_groups = DATE_PATTERN.search(url)
                if article.text != None and len(article) > 0:
                    body = getArticleText(url, RETRIES)
                    articles.append({ 
                        'url'   : url,
                        'body'  : body,
                        'year'  : int(date_groups.group(1)), # year
                        'month' : int(date_groups.group(2)) # month
                    })
                    wordCount += body.count(' ')
        if pageNumber % CHECKPOINT_FREQUENCY == 0:
            ouraws.saveNewArticles(articles, checkpoint_name=CHECKPOINT_FILENAME)
        if showProgress: 
            print("-> {} : {} : {} : {}"
                  .format(pageNumber, len(articleList), wordCount, 
                          baseURL+"?page="+str(pageNumber)+"&per_page=20"))
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
        progname.split('/')[-1]
    ))

#=====================================================================
# main
#=====================================================================
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(len(sys.argv))
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

#     url = 'https://www.middleburycampus.com/article/2001/10/students-blind-to-worlds-realities'
#     date_groups = DATE_PATTERN.search(url)
#     print(int(date_groups.group(1)))
#     print(int(date_groups.group(2)))
#     text = getArticleText(url, 2)

#     print(f"article: {text}")

#############
# Testing proxy for getting list of articles
#############
# if __name__ == "__main__":
#     print("starting")
#     articleList = getArticleList(LISTING_BASE_URL+"?page="+str(100)+"&per_page=20", 
#                                  RETRIES, True)
#     print("got list")
#     print(len(articleList))
#     print("\n".join([a.get('href') for a in articleList]))
