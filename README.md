## Movie Rating Analytics Pipeline


### **Overview**

This project replicates a real-world data engineering scenario for a media analytics company aiming to track and analyze movie ratings to gain insights into audience preferences. The data is collected in CSV format from Google Drive and processed through a robust ETL pipeline using modern tools.

### **Project Goals**

- Ingest, clean, and transform movie ratings data

- Download source data from Google Drive

- Implement incremental loading using release_date

- Store processed data in PostgreSQL

- Visualize insights with Power BI

- Automate the entire pipeline using Apache Airflow


### **Dataset Description**

The dataset consists of three main CSV files (hosted on Google Drive):

data_movie_lens.csv → Ratings per user per movie

item_movie_lens.csv → Movie metadata (including release_date)

user_movie_lens.csv → User demographic info


- *Incremental loading is based on the release_date column from item_movie_lens.csv.*


|Tool                    | Purpose|
| ------                 | -------|
| Google Drive         | Cloud source for raw CSV data|
| Pandas                | Data wrangling & transformation|
| S3 Bucket             |Raw & cleaned data storage|
| PostgreSQL            |  Final data warehouse for analytics|
| Power BI              |Visualizations & summary statistics|
|Apache Airflow         | ETL orchestration and automation|