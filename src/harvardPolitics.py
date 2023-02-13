#########################################################################
# harvardPolitics.py
# ------------------
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

from util.ouraws import getFromS3, saveNewResults
from util.convert import safeFloat

SCHOOL="harvard"
SUBJECT="opinion"
START_YEAR=2010
FINAL_YEAR=2022
DATA_DIR="data"      # should be 'data'
OUTPUT_DIR="output"  # should be 'output'
S3_ARTICLES_KEY = f"{DATA_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"
S3_POLITICS_KEY = f"{DATA_DIR}/{SCHOOL}-POLARITY.parquet"

BIPARTISAN_API_KEY = os.environ.get("BIPARTISAN_API_KEY")
BIPARTISAN_URL = "https://api.thebipartisanpress.com/api/endpoints/beta/robert"
NUM_PARALLEL_TASKS = 4


# Local results storage
# def saveNewResults(new_results, primary_key, output_name):
#     new_df = pd.DataFrame.from_records(new_results)
#     stored_df = pd.read_parquet(output_name)
#     if stored_df is None or stored_df.size == 0:
#         stored_df = new_df
#     else:
#         stored_df = stored_df[~ stored_df[primary_key].isin(new_df[primary_key])]
#         stored_df = pd.concat([stored_df, new_df])
#     print(f"\t{output_name}: {stored_df.shape}")
#     stored_df.to_parquet(output_name)
#     return stored_df


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


def getBipartisanScore(index, text, q):
    payload = {"API": BIPARTISAN_API_KEY, "Text": text.encode("utf-8")}
    response = requests.post(BIPARTISAN_URL, data=payload)
    q.put(response.text) # results are string type


def queryBipartisan(articles_list, max_articles, results_queue, num_threads=5):
    threads = []
    for index, article in enumerate(articles_list):
        t = threading.Thread(target=getBipartisanScore,
                            args=(index, article, results_queue))
        threads.append(t)
        t.start()
        if (index+1) % num_threads == 0 or \
            (index+1) == max_articles:
            for t in threads: 
                t.join()
            print("o", end="", flush=True)
        if (index+1) == max_articles: break


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
            queryBipartisan(articles_list, num_articles, 
                            results_queue, NUM_PARALLEL_TASKS)
            yearResults = tabulateYearlyResults(year, results_queue)
            print("\t{:4}\t{:.2f}s\t{:.2f}".format(
                yearResults['article_count'],
                time.time() - start_time,
                yearResults['polarity_avg']
            ), flush=True)
            results.append(yearResults)
            saveNewResults(results, primary_key='year', output_name=S3_POLITICS_KEY)
        else: print(f"NONE")
    return results


if __name__ == '__main__':
    df = getFromS3(S3_ARTICLES_KEY)
    print(f"Every 'o' is ~{NUM_PARALLEL_TASKS} articles")
    print(f"{SCHOOL} data: {df.shape}")

    results = processArticles(df, START_YEAR, FINAL_YEAR, MAX_PER_YEAR=2)
    print(f"Successfully processed {len(results)} years")
