import gdown
import os

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv

# Download user_movie_lens"
url ="https://drive.google.com/file/d/1_wAww5beF2K7dpx-SU_gUUddNWeaeZqv/view"
output = "user_movie_lens.csv"
gdown.download(url, output, quiet=False , fuzzy=True)

# Download item_movie_lens"
url = "https://drive.google.com/file/d/188tIKLJKek62rGmzj1Ylc03fe4Pgb5co/view"
output = "item_movie_lens.csv"
gdown.download(url, output, quiet=False, fuzzy=True)

# Download data_movie_lens"
url = "https://drive.google.com/file/d/1-3S-XOgZyo9D3sVoXtjPvmFdsihjfQhN/view"
output = "data_movie_lens.csv"
gdown.download(url, output, quiet=False, fuzzy=True)


# Load environment variables from .env file
load_dotenv()

secret_key = os.getenv("MY_SECRET_KEY")
access_key = os.getenv("MY_ACCESS_KEY")
region_key = os.getenv("REGION_KEY")

# s3 Bucket details
s3_bucket = 'movie-rating-project'
s3_folder = 'movie_rating'
s3_filename_user = 'user_movie_lens'
s3_filename_item = 'item_movie_lens'
s3_filename_data =  'data_movie_lens'

s3_path_user = f"s3://{s3_bucket}/{s3_folder}/{s3_filename_user}"
s3_path_item = f"s3://{s3_bucket}/{s3_folder}/{s3_filename_item}"
s3_path_data = f"s3://{s3_bucket}/{s3_folder}/{s3_filename_data}"

# read csv files
df_user_movie = pd.read_csv("user_movie_lens.csv")
df_item_movie = pd.read_csv("item_movie_lens.csv")
df_data_movie = pd.read_csv("data_movie_lens.csv")

session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# Upload dataframe as a parquet file to S3
wr.s3.to_parquet(
    df = df_user_movie, 
    path=s3_path_user,
    dataset=True,
    mode='overwrite',
    index=False,
    boto3_session=session)

wr.s3.to_parquet(
    df = df_item_movie, 
    path=s3_path_item,
    dataset=True,
    mode='overwrite',
    index=False,
    boto3_session=session)

wr.s3.to_parquet(
    df = df_data_movie, 
    path=s3_path_data,
    dataset=True,
    mode='overwrite',
    index=False,
    boto3_session=session)


print(f"Data successfully uploaded to {s3_path_user}")
print(f"Data successfully uploaded to {s3_path_item}")
print(f"Data successfully uploaded to {s3_path_data}")

