import awswrangler as wr
import pandas as pd
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

access_key = os.getenv("MY_ACCESS_KEY")
secret_key = os.getenv("MY_SECRET_KEY")

# s3 Bucket detail
bucket = "movie-rating-project"
folder = "movie_rating"
filename = "transformed_dataset"

# Read transformed data
df = pd.read_csv('transformed_files/transformed_data.csv')

# Session
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Upload to S3 as Parquet
s3_path = f"s3://{bucket}/{folder}/{filename}"
wr.s3.to_parquet(
    df=df,
    path=s3_path,
    dataset=True,
    mode="overwrite",
    boto3_session=session
)

print(f"Data successfully uploaded to {s3_path}")
