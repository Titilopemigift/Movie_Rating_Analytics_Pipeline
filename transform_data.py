import datetime as dt
import os
from pathlib import Path

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv

from extract_files_to_s3 import df_movie_dataset

# convert release date to datetime
df_movie_dataset['release_date'] = pd.to_datetime(df_movie_dataset['release_date'], errors='coerce')



# drop duplicates

df_duplicate = df_movie_dataset.drop_duplicates()

# check for null values
df_null =df_movie_dataset.isnull().sum()

# Extract date parts
df_movie_dataset['release_year'] = df_movie_dataset['release_date'].dt.year.fillna(0).astype(int)
df_movie_dataset['release_month'] = df_movie_dataset['release_date'].dt.month.fillna(0).astype(int)


df_movie_dataset.to_csv ("transformed_movie_dataset.csv", index =False)

print("csv file successfully created")


df_transformed_dataset = pd.read_csv("transformed_movie_dataset.csv")

df_transformed_dataset['release_date'] = pd.to_datetime(df_transformed_dataset['release_date'], errors='coerce')
# print (df_transformed_dataset.info())

# incremental loading

checkpoint_file = Path("last_release_date.txt")
if checkpoint_file.exists():
        with open(checkpoint_file, "r") as f:
            last_date = pd.to_datetime(f.read().strip())
else:
        last_date = pd.to_datetime("1900-01-01")

df_new = df_transformed_dataset[df_transformed_dataset['release_date'] > last_date]

if not df_new.empty:
        latest = df_new['release_date'].max()
        with open("last_release_date.txt", "w") as f:
            f.write(str(latest))
    
df_new.to_csv("transformed.csv", index=False)


# Load environment variables from .env file
load_dotenv()

secret_key = os.getenv("MY_SECRET_KEY")
access_key = os.getenv("MY_ACCESS_KEY")
region_key = os.getenv("REGION_KEY")

# s3 Bucket details
s3_bucket = 'movie-rating-project'
s3_folder = 'movie_rating'
s3_filename = 'transformed'

s3_path = f"s3://{s3_bucket}/{s3_folder}/{s3_filename}"


# read merge file
df_transformed_dataset = pd.read_csv("transformed.csv")

session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# Upload dataframe as a parquet file to S3
wr.s3.to_parquet(
    df = df_transformed_dataset, 
    path=s3_path,
    dataset=True,
    mode='overwrite',
    index=False,
    boto3_session=session)

print(f"Data successfully uploaded to {s3_path}")





