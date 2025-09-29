# Import libraries
import pandas as pd
import numpy as np

# ----------------------------
# Create test data
# ----------------------------
data = {
    "nominee": [
        "Abigail Breslin", "Abigail Breslin", "Abigail Breslin",
        "Leonardo DiCaprio", "Leonardo DiCaprio",
        "Meryl Streep"
    ],
    "movie": [
        "Little Miss Sunshine", "Zombieland", "August: Osage County",
        "The Revenant", "Inception",
        "The Iron Lady"
    ],
    "year": [2006, 2009, 2013, 2015, 2010, 2011]
}

oscar_nominees = pd.DataFrame(data)

# ----------------------------
# Filter and compute result
# ----------------------------
nominee = oscar_nominees[oscar_nominees['nominee'] == 'Abigail Breslin']
result = nominee.movie.nunique()

print("Number of unique movies for Abigail Breslin:", result)
