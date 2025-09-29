# Import your libraries
import pandas as pd
from datetime import datetime

def compute_actor_differences(actor_rating_shift: pd.DataFrame) -> pd.DataFrame:
    """
    For each actor, compute:
      - avg_lifetime_rating: average rating across all films
      - latest_rating: rating of the most recent film (by release_date)
      - rating_difference: latest_rating - average(previous films), rounded to 2 decimals
        * If actor has only one film, difference = 0, and avg_lifetime_rating = latest_rating

    Tie-break rule when multiple films share the latest date:
      - pick the one with the higher rating; if still tied, pick lexicographically smaller film_title.
    """
    df = actor_rating_shift.copy()

    # Ensure release_date is datetime
    df["release_date"] = pd.to_datetime(df["release_date"])

    # --- lifetime average across ALL films ---
    lifetime_avg = (
        df.groupby("actor_name", as_index=False)
        .agg(avg_lifetime_rating=("film_rating", "mean"))
    )

    # --- pick exactly one "latest" film per actor with tie-breakers ---
    # Sort so the desired "latest" row becomes the first row per actor
    df_sorted = df.sort_values(
        by=["actor_name", "release_date", "film_rating", "film_title"],
        ascending=[True, False, False, True]   # latest date desc, higher rating first, then title asc
    )
    # Mark first row per actor as the "latest"
    df_sorted["rn"] = df_sorted.groupby("actor_name").cumcount() + 1
    latest_rows = df_sorted.loc[df_sorted["rn"] == 1, ["actor_name", "film_rating"]].copy()
    latest_rows = latest_rows.rename(columns={"film_rating": "latest_rating"})

    # --- previous films average (exclude the chosen latest row) ---
    # To exclude exactly the chosen latest row, drop rn==1 from df_sorted before averaging
    prev_df = df_sorted.loc[df_sorted["rn"] != 1, ["actor_name", "film_rating"]].copy()
    prev_avg = (
        prev_df.groupby("actor_name", as_index=False)
        .agg(avg_previous_rating=("film_rating", "mean"))
    )

    # Merge pieces
    out = latest_rows.merge(lifetime_avg, on="actor_name", how="left")
    out = out.merge(prev_avg, on="actor_name", how="left")

    # Single-film actors: avg_previous_rating is NaN -> set diff to 0,
    # and per requirement: lifetime avg = only film’s rating (already true), latest = that rating (already true)
    # Compute difference; if no previous, fill with 0
    out["rating_difference"] = (out["latest_rating"] - out["avg_previous_rating"]).round(2)
    out["rating_difference"] = out["rating_difference"].fillna(0.00)

    # Nice ordering
    out = out[["actor_name", "avg_lifetime_rating", "latest_rating", "rating_difference"]]
    # Optional: sort by actor_name for readability
    out = out.sort_values("actor_name").reset_index(drop=True)
    return out


def build_test_data() -> pd.DataFrame:
    """
    Create a robust test set with:
      - Actors with multiple films
      - Single-film actor
      - Ties on latest date to exercise the tie-break rule
    """
    data = [
        # Actor A: clear increasing timeline
        {"actor_name": "Alice Anders", "film_title": "Rising Dawn",     "release_date": "2018-05-10", "film_rating": 6.8},
        {"actor_name": "Alice Anders", "film_title": "Silent Echoes",   "release_date": "2020-08-01", "film_rating": 7.5},
        {"actor_name": "Alice Anders", "film_title": "Last Horizon",    "release_date": "2023-09-15", "film_rating": 8.9},

        # Actor B: single film
        {"actor_name": "Ben Brooks",   "film_title": "Lone Star",       "release_date": "2019-03-22", "film_rating": 6.2},

        # Actor C: latest date tie; higher rating should win as latest
        {"actor_name": "Cara Chen",    "film_title": "Neon River",      "release_date": "2021-02-11", "film_rating": 7.1},
        {"actor_name": "Cara Chen",    "film_title": "Neon River II",   "release_date": "2024-11-09", "film_rating": 8.0},
        {"actor_name": "Cara Chen",    "film_title": "Neon River III",  "release_date": "2024-11-09", "film_rating": 8.4},  # same date, higher rating
        {"actor_name": "Cara Chen",    "film_title": "Neon Prelude",    "release_date": "2019-01-04", "film_rating": 6.5},

        # Actor D: latest date tie with same rating; title tiebreak (alphabetical)
        {"actor_name": "Diego Diaz",   "film_title": "Twin Skies",      "release_date": "2022-07-07", "film_rating": 7.9},
        {"actor_name": "Diego Diaz",   "film_title": "Mirror Gate A",   "release_date": "2025-06-01", "film_rating": 8.3},
        {"actor_name": "Diego Diaz",   "film_title": "Mirror Gate B",   "release_date": "2025-06-01", "film_rating": 8.3},  # same date & rating, 'A' beats 'B'
    ]
    return pd.DataFrame(data)


if __name__ == "__main__":
    # Build sample data frame named like your code expects
    actor_rating_shift = build_test_data()

    # Show input (optional)
    print("Input data:")
    print(actor_rating_shift.sort_values(["actor_name", "release_date"]).to_string(index=False))
    print("\nResults:")

    result_df = compute_actor_differences(actor_rating_shift)
    print(result_df.to_string(index=False))
