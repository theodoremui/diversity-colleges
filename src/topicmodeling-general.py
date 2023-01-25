#########################################################################
#
# topicmodeling-general.py
#
# @author: Phil Mui
# @email: thephilmui@gmail.com
# @date: Mon Jan 23 16:45:56 PST 2023
#
#########################################################################

import sys
import time
import numpy as np
import pandas as pd

import textutil
import ouraws
import ourembeddings
import ourgraphs

DATA_DIR = 'data'

def printParquetInfo(df):
    # DataFrame df has dimensions of (# articles, #attributes)
    print("\tTotal articles: {}, each with attributes: {}"
            .format(df.shape[0], df.shape[1]))

    # In reverse order: earlier article is last row: df.iloc[-1]
    print(f"\tEarliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
    print(f"\tLatest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")
    return df


def processSchoolByYear(df, start_year, end_year):

    results = []
    print("{:<10}{:<10}{:<10}{:<10}{:<10}{:<10}{:<10}".format(
        "year", "num_docs", "mentions", "trace", "norm1", "norm2", "pairwise"))
    for year in range(start_year, end_year+1):
        year_df = df[df.year==year]

        if year_df.shape[0] > 0:
            docs, diversity_norm = textutil.filterText(year_df.body) 
            docvecs = ourembeddings.getDocEmbeddings(docs.tolist())
            # docvecs = ourembeddings.getTFIDFDocEmbeddings(docs.tolist())
                        
            pairwise = textutil.getNormalizedPairwiseDispersion(docvecs)
            cov = textutil.getCovDispersion(docvecs)
            
            print("{:<10}{:<10}{:<10}{:<10.3e}{:<10.3e}{:<10.3e}{:<10.3f}".format(
                year, cov[0], diversity_norm, cov[1], cov[2], cov[3], pairwise
            ))
            
            result = {'year':       year,
                    'mention-norm': diversity_norm,
                    'pairwise':     pairwise,
                    'size':         cov[0],
                    'trace':        cov[1],
                    'norm-1':       cov[2], 
                    'norm-2':       cov[3], 
                    'norm-inf':     cov[4], 
                    }
            results.append(result)
    return pd.DataFrame.from_records(results)


def processSchool(school_name, section_name, start_year, end_year):

    S3OBJECT_KEY = f"{DATA_DIR}/{school_name}-{section_name}-SNAPSHOT.parquet"
    print(f"\tloading parquet data from {S3OBJECT_KEY}")
    raw = ouraws.getFromS3(S3OBJECT_KEY)
    printParquetInfo(raw)

    results_df = processSchoolByYear(raw, start_year, end_year)
    results_df.set_index('year')
    ourgraphs.saveResults(results_df, school_name, section_name, 
                          start_year, end_year)

#=====================================================================
# main
#=====================================================================
def printUsage(progname):
    print("Usage: python {} <school> <opinion-string> <start-year> <end-year>".format(
        progname
    ))

if __name__ == "__main__":

    if len(sys.argv) != 5:
        printUsage(sys.argv[0]) 
        sys.exit(0)

    try:
        start_year = int(sys.argv[3])
        end_year = int(sys.argv[4])
        start_time = time.time()
        processSchool(sys.argv[1], sys.argv[2], start_year, end_year)
        minutes, seconds = divmod(time.time() - start_time, 60)
        print("elapsed time: {0:.0f}m {1:.2f}s".format(minutes, seconds))
    except Exception as e:
        print(f"error message: {e}")
