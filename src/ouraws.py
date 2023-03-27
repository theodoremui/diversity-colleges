#########################################################################
#
# ouraws.py
#
#########################################################################

import os
import pandas as pd

import ouraws

import boto3
import botocore
import pyarrow as pa
import pyarrow.parquet as pq

S3_BUCKET="collegier"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, 
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def getFromS3(s3object_key):
    df = None

    # for key in s3.list_objects(Bucket=S3_BUCKET)['Contents']:
    #     print(key['Key'])
    try:
        s3.download_file(S3_BUCKET, s3object_key, s3object_key)
        df = pd.read_parquet(s3object_key)
    except botocore.exceptions.ClientError as e:
        print(f"\tgetFromS3(): did not find object: {s3object_key}")
    return df

def putToS3(s3object_key, df):
    # Save the dataframe to a local parquet file
    df.to_parquet('df.parquet', engine='pyarrow')

    # Upload the parquet file to the specified S3 bucket and file
    s3.upload_file('df.parquet', S3_BUCKET, s3object_key)

def saveNewArticles(new_articles, checkpoint_name):
    new_df = pd.DataFrame.from_records(new_articles)
    stored_df = ouraws.getFromS3(checkpoint_name)
    if stored_df is None or stored_df.size == 0:
        stored_df = new_df
    else:
        stored_df = stored_df[~ stored_df['url'].isin(new_df['url'])]
        stored_df = pd.concat([stored_df, new_df])

    print(f"\t{checkpoint_name}: {stored_df.shape} articles")
    ouraws.putToS3(checkpoint_name, stored_df)
    return stored_df

def saveByYear(df, output_dir, prefix):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    oldest_year = df.iloc[-1][-3]
    latest_year = df.iloc[0][-3]

    for y in range(oldest_year, latest_year+1):
        print(f"{y} has {df[df.year == y].shape[0]} articles")
        ouraws.putToS3(f"{output_dir}/{prefix}-{y}.parquet", df[df.year == y])
