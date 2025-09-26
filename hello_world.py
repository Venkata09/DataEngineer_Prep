import pandas as pd

def main():
    # -----------------------
    # Sample Data
    # -----------------------
    oscar_nominees = pd.DataFrame({
        "nominee": ["Actor A", "Actor B", "Actor C", "Actor A", "Actor B", "Actor D"],
        "winner":  [True, False, True, True, True, False]
    })

    nominee_information = pd.DataFrame({
        "name": ["Actor A", "Actor B", "Actor C", "Actor D"],
        "top_genre": ["Drama", "Comedy", "Action", "Drama"]
    })

    print("Oscar Nominees:")
    print(oscar_nominees, "\n")

    print("Nominee Information:")
    print(nominee_information, "\n")

    # -----------------------
    # Step 1: Filter only winners
    # -----------------------
    winner = oscar_nominees[oscar_nominees['winner'] == True]

    # -----------------------
    # Step 2: Count wins per nominee
    # to_frame() -> converts Series to DataFrame with a column name
    # reset_index() -> brings index back as a column (so 'nominee' stays a column)
    # sort_values() -> sort by number of wins
    # -----------------------
    n_winnings = (
        winner.groupby('nominee')
        .size()
        .to_frame('n_win')
        .reset_index()
        .sort_values('n_win', ascending=False)
    )

    print("Number of Wins per Nominee:")
    print(n_winnings, "\n")

    # -----------------------
    # Step 3: Merge with nominee_information
    # -----------------------
    merged = pd.merge(
        n_winnings,
        nominee_information,
        left_on="nominee",
        right_on="name",
        how="inner"
    )

    print("Merged Data:")
    print(merged, "\n")

    # -----------------------
    # Step 4: Find the nominee with most wins per top_genre
    # nlargest(1, 'n_win') -> picks top 1 row by n_win in each group
    # -----------------------
    result = (
        merged.groupby(['top_genre'])
        .apply(lambda x: x.nlargest(1, 'n_win'))
        .reset_index(drop=True)
        .sort_values(['n_win', 'name'], ascending=[False, True])
        .head(1)[['top_genre']]
    )

    print("Final Result (Top Genre with Most Wins):")
    print(result)


if __name__ == "__main__":
    main()
