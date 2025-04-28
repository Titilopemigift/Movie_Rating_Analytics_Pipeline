import psycopg2
import getpass
import pandas as pd
from io import StringIO
from pathlib import Path

# PostgreSQL connection
my_password = getpass.getpass()
print('password secured')

conn = psycopg2.connect(
    database="movie_ratings",
    user="postgres",
    password=my_password,
    host="localhost",
    port="5432"
)

# Read transformed CSV
df = pd.read_csv('transformed_files/transformed_data.csv')

# Handle checkpoint
checkpoint_path = Path('last_release_date.txt')
if checkpoint_path.exists():
    with open(checkpoint_path, 'r') as f:
        last_date = pd.to_datetime(f.read().strip())
else:
    last_date = pd.to_datetime('1900-01-01')

# Filter new records
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
df_new_data = df[df['release_date'] > last_date]

# If new data found, load it
if not df_new_data.empty:
    cursor = conn.cursor()

    # Prepare CSV buffer for bulk insert
    buffer = StringIO()
    df_new_data.to_csv(buffer, index=False, header=True, quoting=1)  # quoting=1 to enforce double quotes
    buffer.seek(0)

    # Copy data into Postgres
    cursor.copy_expert(
        sql="""
        COPY movie_ratings
        FROM STDIN
        WITH CSV HEADER
        DELIMITER ','
        QUOTE '"'
        """,
        file=buffer
    )

    conn.commit()

    # Update checkpoint
    new_last_date = df_new_data['release_date'].max()
    with open('last_release_date.txt', 'w') as f:
        f.write(str(new_last_date))

    print(f"{len(df_new_data)} new rows loaded. Updated checkpoint to {new_last_date}")
    cursor.close()
    conn.close()

else:
    print("No new data to load")
    conn.close()






