import gdown


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
