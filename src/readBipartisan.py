import os
import pandas as pd
import ouraws

DATA_DIR="data"
SCHOOL= "berkeley"

S3_POLITICS_KEY = f"{DATA_DIR}/{SCHOOL}-POLARITY.parquet"

def getStoredResults():
    return ouraws.getFromS3(S3_POLITICS_KEY)

if __name__ == "__main__":
    print(f"reading from {S3_POLITICS_KEY}")
    results_df = getStoredResults()

    print(f"Number of records: {results_df.shape}")
    print(results_df.sort_values(by='year', ascending=True))
