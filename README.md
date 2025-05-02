## Movie Rating Analytics Pipeline


### **Overview**

This project simulates a real-world data engineering workflow for a media analytics company aiming to analyze movie ratings and understand audience behavior. The pipeline ingests raw CSV data from Google Drive, transforms it using Python, stores the cleaned data in PostgreSQL, and visualizes insights with Power BI. The entire workflow is automated using Apache Airflow in a Dockerized environment.

### **Project Goals**

- Ingest raw data from Google Drive

- Clean and merge datasets using Pandas

- Load **transformed data** to S3 (Parquet format)

- Store the final cleaned data in a PostgreSQL database

- Implement incremental data loading using `release_date`

- Visualize insights with Power BI
- Automate the entire pipeline with Apache Airflow and Docker


### **Dataset Description**

The dataset consists of three main CSV files (hosted on Google Drive):

data_movie_lens.csv → Ratings per user per movie

item_movie_lens.csv → Movie metadata (including release_date)

user_movie_lens.csv → User demographic info


- *Incremental loading is based on the release_date column from item_movie_lens.csv.*


|Tool                    | Purpose|
| ------                 | -------|
| **Google Drive + Gdown**         | Raw data source|
| **Pandas**                | Data cleaning, transformation and merging|
| **S3 Bucket**             |Stores transformed data|
| **PostgreSQL**            |  Final data warehouse for analytics|
| **Power BI**             |Visualizations & summary statistics|
|**Apache Airflow**         | ETL orchestration and automation|

## Pipeline Breakdown

1. **Extraction**
   - Downloads all CSV files from Google Drive using `gdown`

2. **Transformation**
   - Merges movie, user, and rating data
   - Converts `release_date` to datetime
   - Adds `release_year` and `release_month`
   - Drops duplicates and handles nulls

3. **Loading**
   - Transformed data is saved as a Parquet file to **AWS S3**
   - Also loaded into **PostgreSQL** for dashboard use
   - **Incremental logic** ensures only new records (based on `release_date`) are added

4. **Visualization**
   - Power BI connects to PostgreSQL
   - Dashboard includes:
     - KPIs: Total Ratings, Average Rating, Total Users, Highest Rating
     - Charts: Top 10 Movies, Most rated Genres, Ratings by Gender, Top 5 Movies
     - Slicers: Genre, Gender, Date

5. **Automation**
   - Airflow orchestrates all steps via DAGs
   - Docker used to run Airflow and Postgres locally

---

