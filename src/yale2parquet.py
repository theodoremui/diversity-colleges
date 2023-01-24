import os
import re
import requests
import random
import sys
import time
import asyncio
import pandas as pd
from bs4 import *

import ouraws
import ourrequests
RETRIES = 6
CHECKPOINT_FREQUENCY = 10

OUTPUT_DIR="data"
SCHOOL="yale"
SUBJECT="opinion"
CHECKPOINT_FILENAME = f"{OUTPUT_DIR}/{SCHOOL}--{SUBJECT}-SNAPSHOT.parquet"

DATE_PATTERN = re.compile("https://yaledailynews.com/blog/(\d+)/(\d+)/(\d+)")
LISTING_BASE_URL = f"https://yaledailynews.com/blog/category/opinion/page/"
HEADER = { 'User-Agent' : 'Mozilla5.0' }

def get_article_text(url, num_retries, use_proxy=False):
    html = requests.get(url, headers=HEADER).text
    soup = BeautifulSoup(html, 'html.parser')
    lines = soup.get_text().splitlines()

    output = ""
    for line in lines:
        words = re.split("[\s\t]", line)
        if len(words) > 3:
            output += line + "\n"

    return output

def get_articles(base_url, page_list, show_progress=False):
    failed_pages = []
    articles = []
    for page_number in page_list:
        page_content = requests.get(base_url + str(page_number), headers=HEADER)
        soup = BeautifulSoup(page_content.text, 'html.parser')
        article_link_list = soup.select("section > div > div > div > a[href^='http']")
        article_title_list = map(
            lambda x: re.search(r'^\s*(.*?)\s*$', x.text).group(1), # removes whitespace from titles
            soup.select("section > div > div > div > a > div > div")[1::2] # gets all the titles
        )

        word_count = 0
        if len(article_title_list) == 0: failed_pages.append(page_number)
        else:
            if article_link_list and article_title_list:
                for (link, article) in zip(article_link_list, article_title_list):
                    url = link.get('href')
                    date_groups = DATE_PATTERN.search(url)

                    if article != None and len(article) > 0:
                        articles.append({
                            "title": article,
                            "url": url,
                            "body": get_article_text(url),
                            "year": int(date_groups.group(1)),
                            "month": int(date_groups.group(2)),
                            "day": int(date_groups.group(3)),
                        })
                        word_count += get_article_text(url).count(" ")
        
        if page_number % CHECKPOINT_FREQUENCY == 0:
            ouraws.saveNewArticles(articles, checkpoint_name=CHECKPOINT_FILENAME)
        if show_progress:
            print("-> {} : {} : {} : {}"
                .format(page_number, len(article_title_list), word_count, base_url + str(page_number)))
            print(f"\t{failed_pages}")
        page_number += 1
    
    df = ouraws.saveNewArticles(articles, checkpoint_name=CHECKPOINT_FILENAME)
    return df, failed_pages

def start_processing(start_page, end_page, num_retries):
    df, failed_pages = get_articles(LISTING_BASE_URL, range(start_page, end_page), show_progress=True)
    while len(failed_pages) > 0 and num_retries > 0:
        retry_df, failed_pages = get_articles(LISTING_BASE_URL, failed_pages, show_progress=True)
        if not retry_df.empty:
            df = df[~ df['url'].isin(retry_df['url'])]
            df = pd.concat([df, retry_df])
        print(f"failed pages: {failed_pages}")
        num_retries -= 1

    print("total articles: {}, each with attributes: {}".format(df.shape[0], df.shape[1]))
    print(f"earliest: {df.iloc[-1][3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")
    print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")

    ouraws.saveByYear(df, output_dir=OUTPUT_DIR, prefix=f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}")

def print_usage(progname):
    print("Usage: python {} <start_page> <end_page> <num_retries>".format(progname))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print_usage(sys.argv[0])
        sys.exit(0)
    try:
        start_page = int(sys.argv[1])
        end_page = int(sys.argv[2])
        num_retries = int(sys.argv[3])
        print(f"Processing {SCHOOL} {SUBJECT} from {start_page} to {end_page}")
        start_processing(start_page, end_page, num_retries)
    except Exception as e:
        print(f"Exception: {e}")