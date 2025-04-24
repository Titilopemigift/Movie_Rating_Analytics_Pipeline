import boto3
import psycopg2
import getpass
import os
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

secret_key = os.getenv("MY_SECRET_KEY")
access_key = os.getenv("MY_ACCESS_KEY")
region_key = os.getenv("REGION_KEY")


# create S3 client

s3 = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key= secret_key
)

# download the Parquet file from S3
bucket_name = 'movie-rating-project'
parquet_file= 'movie_rating/transformed/24fcb420d72a42dcb8bf5c89fd785677.snappy.parquet'   
local_parquet_file = 'temp_transformed.parquet'


s3.download_file(bucket_name, parquet_file, local_parquet_file)
print("Parquet file downloaded from S3.")

# convert to DataFrame
df = pd.read_parquet(local_parquet_file)
print("Parquet file loaded into DataFrame.")


# save as CSV
csv_file = 'temp_transformed.csv'
df.to_csv(csv_file, index=False)
print(f"Data saved as {csv_file}")


