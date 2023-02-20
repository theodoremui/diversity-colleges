#########################################################################
# readParquet.py
# ------------------
# outputting number of articles for each year for a particular school's college 
# newspaper in a specific subject area
# 
# Usage:
#        > python src/readParquet <school> <subject>
#
# where:
#        school  = name of school such as "harvard"
#        subject = section in school newspaper such as "opinion" or "opinions"
#
# @author Theodore Mui
# @email theodoremui@gmail.com
# @date Sun Feb 19 17:37:21 PST 2023
#
# Retry logic: bit.ly/requests-retry
#########################################################################

import os
import sys
import pandas as pd
import ouraws

OUTPUT_DIR="data"

def getStoredArticles(filename):
    return ouraws.getFromS3(filename)

def printResults(filename):
    print(f"reading from {filename}")
    df = getStoredArticles(filename)

    print(f"Number of records: {df.shape}")
    
    # In reverse order: earlier article is last row: df.iloc[-1]
    print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
    print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")

    end_year = df.iloc[0][-3]
    start_year = df.iloc[-1][-3]
    for year in range(start_year, end_year+1):
        year_df = df[df.year==year]
        print(f"{year}\t{year_df.shape[0]}")


def printUsage(progname):
    print("Usage: python {} <school> <subject>".format(
        progname
    ))

if __name__ == "__main__":

    if len(sys.argv) != 3:
        printUsage(sys.argv[0]) 
        sys.exit(0)

    try:
        school  = sys.argv[1]
        subject = sys.argv[2]
        filename=f"{OUTPUT_DIR}/{school}-{subject}-SNAPSHOT.parquet"
        printResults(filename)

    except Exception as e:
        print(f"Exception: {e}")
