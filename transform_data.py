import pandas as pd
from extract_files_to_s3 import df_data_movie, df_user_movie, df_item_movie


# merge csv files
rating_movie = pd.merge(df_data_movie, df_item_movie, on='item_id', how='left')

full_merge_dataset = pd.merge(rating_movie, df_user_movie, on='user_id', how='left')

full_merge_dataset.to_csv("movie_dataset.csv", index =False) 

print("csv file successfully created")

df_movie_dataset = pd.read_csv("movie_dataset.csv")

