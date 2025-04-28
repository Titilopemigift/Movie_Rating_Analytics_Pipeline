import pandas as pd
import csv
from pathlib import Path
 
 # Read raw CSVs
data_movie = pd.read_csv('raw_csv_files/data_movie_lens.csv')
item_movie = pd.read_csv('raw_csv_files/item_movie_lens.csv')
user_movie = pd.read_csv('raw_csv_files/user_movie_lens.csv')
 
# Merge csv files
item_movie['release_date'] = pd.to_datetime(item_movie['release_date'], errors='coerce')
full_merge_dataset = data_movie.merge(item_movie, on='item_id', how='left')
full_merge_dataset = full_merge_dataset.merge(user_movie, on='user_id', how='left')
 
# Extract release year and month
full_merge_dataset['release_year'] = full_merge_dataset['release_date'].dt.year.fillna(0).astype(int)
full_merge_dataset['release_month'] =full_merge_dataset['release_date'].dt.month.fillna(0).astype(int)
 
# Save transformed file
Path('transformed_files').mkdir(exist_ok=True)
full_merge_dataset.to_csv('transformed_files/transformed_data.csv', index=False, quoting=csv.QUOTE_ALL)
 
print(" Transformed CSV file successfully created")