import gdown
from pathlib import Path

# Create raw folder
Path("raw_csv_files").mkdir(exist_ok=True)

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


print("Extracted raw CSV files successfully.")

