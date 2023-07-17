#########################################################################
# harvardBipartisan.py
# --------------------
# Calculating bipartisan score per year for Harvard
# 
# @author Theodore Mui
# @email theodoremui@gmail.com
# @date Sun Feb 12 16:34:09 PST 2023
#
#########################################################################

import requests
import os
import threading
import requests
import os
import time
import queue
import pandas as pd

from util.ouraws import getFromS3, saveBipartisanResults
from util.convert import safeFloat

SCHOOL="harvard"
SUBJECT="opinion"
START_YEAR=2013
FINAL_YEAR=2022
DATA_DIR="data"      # should be 'data'
OUTPUT_DIR="output"  # should be 'output'
S3_ARTICLES_KEY = f"{DATA_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"
S3_POLITICS_KEY = f"{DATA_DIR}/{SCHOOL}-POLARITY.parquet"

BIPARTISAN_API_KEY = os.environ.get("BIPARTISAN_API_KEY")
BIPARTISAN_URL = "https://api.thebipartisanpress.com/api/endpoints/beta/robert"
NUM_PARALLEL_TASKS = 10


def tabulateYearlyResults(year, results_queue):
    value_sum = 0.0
    article_count = 0
    while not results_queue.empty():
        val = safeFloat(results_queue.get())
        if val is not None:
            value_sum += val
            article_count += 1
    return {
        'year': year,
        'article_count': article_count,
        'polarity_sum': value_sum,
        'polarity_avg': 0 if article_count==0 else float(value_sum)/article_count
    }

import random
def getBipartisanScore(index, text):
    payload = {"API": BIPARTISAN_API_KEY, "Text": text.encode("utf-8")}
    time.sleep(0.1 * random.randint(0, 100)/100)
    response = requests.post(BIPARTISAN_URL, data=payload)
    return response.text # results are string type


def storeBipartisanScore(index, text, q):
    scoreText = getBipartisanScore(index, text)
    q.put(scoreText) # results are string type


def queryBipartisanWithThreads(articles_list, max_articles, results_queue, 
                              num_threads=5):
    threads = []
    for index, article in enumerate(articles_list):
        t = threading.Thread(target=storeBipartisanScore,
                            args=(index, article, results_queue))
        threads.append(t)
        t.start()
        if (index+1) % num_threads == 0 or \
            (index+1) == max_articles:
            for t in threads: 
                t.join()
            print("o", end="", flush=True)
        if (index+1) == max_articles: break


def getBipartisanBatch(year, articles_list, max_articles):
    value_sum = 0.0
    article_count = 0
    for index, article in enumerate(articles_list):
        scoreText = getBipartisanScore(index, article)
        val = safeFloat(scoreText)
        if val is not None:
            value_sum += val
            article_count += 1
        if (index+1) % NUM_PARALLEL_TASKS == 0:
            print("o", end="", flush=True)
        if (index+1) == max_articles: break

    return {
        'year': year,
        'article_count': article_count,
        'polarity_sum': value_sum,
        'polarity_avg': 0 if article_count==0 else float(value_sum)/article_count
    }

def processArticles(df, start_year, end_year, MAX_PER_YEAR=1e8):
    results = []
    for year in range(start_year, end_year):
        yeardf = df[df['year'] == year]
        print(f"Year: {year} {yeardf.shape[0]} ", end="", flush=True)

        articles_list = yeardf['body'].to_list()
        start_time = time.time()
        num_articles = min(MAX_PER_YEAR, len(articles_list))
        if num_articles > 0:
            results_queue = queue.Queue()
            yearResults = getBipartisanBatch(year, articles_list, num_articles)
            print("\t{:4}\t{:.2f}s\t{:.2f}".format(
                yearResults['article_count'],
                time.time() - start_time,
                yearResults['polarity_avg']
            ), flush=True)
            results.append(yearResults)
            saveBipartisanResults(results, primary_key='year', 
            output_name=S3_POLITICS_KEY)
        else: print(f"NONE")
    return results


if __name__ == '__main__':
    df = getFromS3(S3_ARTICLES_KEY)
    print(f"Every 'o' is ~{NUM_PARALLEL_TASKS} articles")
    print(f"{SCHOOL} data: {df.shape}")

    results = processArticles(df, START_YEAR, FINAL_YEAR) #, MAX_PER_YEAR=10)
    print(f"Successfully processed {len(results)} years")
