import gdown
import os
from pathlib import Path

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv


Path("csv_files").mkdir(exist_ok=True)

# Download data_movie_lens"
url = "https://drive.google.com/file/d/1-3S-XOgZyo9D3sVoXtjPvmFdsihjfQhN/view"
output = "data_movie_lens.csv"
gdown.download(url, f"raw_csv_files/{output}", quiet=False, fuzzy=True)

# Download item_movie_lens"
url = "https://drive.google.com/file/d/188tIKLJKek62rGmzj1Ylc03fe4Pgb5co/view"
output = "item_movie_lens.csv"
gdown.download(url, f"raw_csv_files/{output}", quiet=False, fuzzy=True)

# Download user_movie_lens"
url ="https://drive.google.com/file/d/1_wAww5beF2K7dpx-SU_gUUddNWeaeZqv/view"
output = "user_movie_lens.csv"
gdown.download(url, f"raw_csv_files/{output}", quiet=False , fuzzy=True)

# read csv files
df_data_movie = pd.read_csv("raw_csv_files/data_movie_lens.csv")
df_user_movie = pd.read_csv("raw_csv_files/user_movie_lens.csv")
df_item_movie = pd.read_csv("raw_csv_files/item_movie_lens.csv")

# merge csv files

rating_movie = pd.merge(df_data_movie, df_item_movie, on='item_id', how='left')

full_merge_dataset = pd.merge(rating_movie, df_user_movie, on='user_id', how='left')

full_merge_dataset.to_csv("merge_dataset.csv", index =False) 

print("csv file successfully created")


# Load environment variables from .env file
load_dotenv()

secret_key = os.getenv("MY_SECRET_KEY")
access_key = os.getenv("MY_ACCESS_KEY")


# s3 Bucket details
s3_bucket = 'movie-rating-project'
s3_folder = 'movie_rating'
s3_filename = 'merge_dataset'

s3_path = f"s3://{s3_bucket}/{s3_folder}/{s3_filename}"


# read merge file
df_movie_dataset = pd.read_csv("merge_dataset.csv")

session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# Upload dataframe as a parquet file to S3
wr.s3.to_parquet(
    df = df_movie_dataset, 
    path=s3_path,
    dataset=True,
    mode='overwrite',
    index=False,
    boto3_session=session)

print(f"Data successfully uploaded to {s3_path}")


