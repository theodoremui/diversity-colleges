import os
import pandas as pd
import ouraws

DATA_DIR="data"
SCHOOL= "harvard"

S3_POLITICS_KEY = f"{DATA_DIR}/{SCHOOL}-POLARITY.parquet"

def getStoredResults():
    return ouraws.getFromS3(S3_POLITICS_KEY)

if __name__ == "__main__":
    print(f"reading from {S3_POLITICS_KEY}")
    df = getStoredResults()

    print(f"Number of records: {df.shape}")
    print(df.sort_values(by='year', ascending=True))
