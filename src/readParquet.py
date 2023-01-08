import os
import pandas as pd
import ouraws

OUTPUT_DIR="data"
SCHOOL="wellesley"
SUBJECT="opinions"
# SCHOOL="stanford"
# SUBJECT="opinions"

FILENAME=f"{OUTPUT_DIR}/{SCHOOL}-{SUBJECT}-SNAPSHOT.parquet"

def getStoredArticles():
    return ouraws.getFromS3(FILENAME)

if __name__ == "__main__":
    print(f"reading from {FILENAME}")
    df = getStoredArticles()

    print(f"Number of records: {df.shape}")
    
    # In reverse order: earlier article is last row: df.iloc[-1]
    print(f"Earliest: {df.iloc[-1][-3]}-{df.iloc[-1][-2]}-{df.iloc[-1][-1]}")

    # Latest article is first row: df.iloc[0]
    print(f"Latest:   {df.iloc[0][-3]}-{df.iloc[0][-2]}-{df.iloc[0][-1]}")
