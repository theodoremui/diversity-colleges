import random
import re
import sys
import time
import pandas as pd
from datetime import datetime
from bs4 import *

import ouraws
import ourrequests

import json

RETRIES = 6
CHECKPOINT_FREQUENCY = 10 # every 10 pages


OUTPUT_DIR="data"
# SCHOOL="harvard"
# SUBJECT="opinion"
# CHECKPOINT_FILENAME  = f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"

#LISTING_BASE_URL = f"https://www.thecrimson.com/tag/columns/page/"

# DATE_PATTERN = re.compile("https://www.thecrimson.com/article/(\d+)/(\d+)/(\d+)")

def getArticleText(url, numRetries, title_selector, content_selector, date_selector, DATE_FORMAT, date_in_url=False, useProxy=False):
    attempts = 0
    content = ""
    while attempts <= numRetries and len(content) < 5: 
        # only use proxy if we have tried and failed in attempt 0
        if attempts > 3: useProxy = True
        html = ourrequests.requestHtml(url, attempts, useProxy)
        soup = BeautifulSoup(html, 'html.parser')

        titleObj = soup.select_one(title_selector)
        contentObj  = soup.select_one(content_selector)
        if titleObj is not None:  title = titleObj.text.strip()
        if contentObj is not None:  content = contentObj.text.strip()
        
        dateObj = None
        date = None
        if not date_in_url:
            dateObj = soup.select_one(date_selector)
            if dateObj is not None: date = datetime.strptime(dateObj.text, DATE_FORMAT)
        

        attempts += 1

        # give the website a small break before next ping
        time.sleep(random.randint(0, 100 * attempts) / 1000.0)

    print(f"\t\t\t{len(content)} ...{content[-18:]}")
    return title, content, date

def getArticleList(listUrl, article_link_selector, numRetries, showProgress=False, useProxy=False):
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
        articles = soup.select(article_link_selector)
        if articles is not None and len(articles) > 0: articleList += articles

        if showProgress: print(f"\tretrieved: {len(articleList)}")
        attempts += 1

    return articleList

# BASE_URL = "https://www.thecrimson.com"
def getArticles(listingBaseURL, articleBaseURL, pageList, checkpoint_filename, article_information,
                showProgress=False, useProxy=False):
    failedPages = []
    articles = []
    date_in_url = article_information[2]

    for pageNumber in pageList:
        articleList = getArticleList(listingBaseURL+str(pageNumber), article_information[5],
                                     RETRIES, showProgress, useProxy)
        wordCount = 0
        if len(articleList) == 0: failedPages.append(pageNumber)
        else:
            for article in articleList:
                url = articleBaseURL + article.get('href')
                
                if date_in_url:
                    date_pattern = re.compile(article_information[4])
                    date_groups = date_pattern.search(url)
                
                if article.text != None and len(article.text) > 0 and \
                   url is not None and len(url) > 10:
                    title, body, date = getArticleText(url, RETRIES, 
                                                       article_information[0], article_information[1],
                                                       article_information[6], article_information[3],
                                                       date_in_url=date_in_url)
                    # print(date_in_url)
                    articles.append({ 
                        'title' : title,
                        'url'   : url,
                        'body'  : body,
                        'year'  : int(date_groups.group(1)) if date_in_url else date.year, # year
                        'month' : int(date_groups.group(2)) if date_in_url else date.month, # month
                        'day'   : int(date_groups.group(3)) if date_in_url else date.day # day
                    })

                    wordCount += body.count(' ')
        if pageNumber % CHECKPOINT_FREQUENCY == 0: 
            ouraws.saveNewArticles(articles, checkpoint_name=checkpoint_filename)
        if showProgress: 
            print("-> {} : {} : {} : {} : {}"
                  .format(pageNumber, len(articleList), wordCount, 
                          articleBaseURL+str(pageNumber), int(date_groups.group(1)) if date_in_url else date.year))
            print(f"\t{failedPages}")
        pageNumber += 1
    df = ouraws.saveNewArticles(articles, checkpoint_name=checkpoint_filename)
    return df, failedPages


def startProcessing(school, subject, article_listing_base_url, article_base_url, 
                    startPage, endPage, numRetries, article_information, checkpoint_filename):

    df, failedPages = getArticles(article_listing_base_url, article_base_url, 
                                  range(startPage,endPage+1), checkpoint_filename, 
                                  article_information, showProgress=True)

    while len(failedPages) > 0 and numRetries > 0:
        retry_df, failedPages = getArticles(LISTING_BASE_URL, 
                                            failedPages, 
                                            showProgress=True,
                                            useProxy=True)
    

    # merginDg together retrieved articles with newly retrieved ones
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
                    prefix=f"{OUTPUT_DIR}/{school}-{subject}")



def printUsage(progname):
    print("Usage: python {} <json_data_file>".format(
        progname
    ))



if __name__ == "__main__":

    if len(sys.argv) != 2:
        printUsage(sys.argv[0]) 
        sys.exit(0)
    
    try:
        colleges_path = open(sys.argv[1])
        col_dict = json.load(colleges_path)

        for college in col_dict["Colleges"]:
            school = college['School']
            subject = college['Subject']
            startPage = college['Start']
            endPage = college['End']
            numRetries = college['Retries']

            checkpoint_filename = f"{OUTPUT_DIR}/{school}-{subject}-SNAPSHOT.parquet"
            print(checkpoint_filename)

            # Article Listing Specific
            article_listing_base_url = college['Article_Listing_URL']
            article_base_url = college['Article_Base_URL']

            # Article Specific
            article_link_selector = college['Article_Link_Selector']
            title_selector = college['Title_Selector']
            content_selector = college['Content_Selector']
            date_selector = college['Date_Selector']
            is_date_in_url = college['Date_In_Url']
            date_format = college['Date_Format']
            date_pattern = college['Date_Pattern']

            article_information = [title_selector, content_selector, is_date_in_url, 
                                   date_format, date_pattern, article_link_selector, date_selector]

            print(f"Processing " + school + " " + subject + " from " + str(startPage) + " to " + str(endPage))
            startProcessing(school, subject, article_listing_base_url, article_base_url,
                            startPage, endPage, numRetries, article_information, checkpoint_filename)
        
    except Exception as e:
        print(f"Exception: {e}")

