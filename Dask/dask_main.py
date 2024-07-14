import math

import dask.dataframe as dd


def calculate_woba(sum_numerator, sum_denominator):
    if sum_denominator == 0:
        return 0.0
    woba = sum_numerator / sum_denominator
    return float(woba)


def calculate_percentage(players_beaten, players):
    if players == 0:
        return 0.0
    percentage = (players_beaten / players) * 100
    return float(percentage)


# Read the CSV files using Dask DataFrames
woba_ddf = dd.read_csv("woba.csv")
hof_ddf = dd.read_csv("HallOfFame.csv", dtype={'ballots': 'float64',
                                               'needed': 'float64',
                                               'needed_note': 'object',
                                               'votes': 'float64'})

batting_ddf = dd.read_csv("Batting.csv", dtype={
    'CS': 'float64',
    'GIDP': 'float64',
    'RBI': 'float64',
    'SB': 'float64',
    'SO': 'float64',
    'lgID': 'object'
})

appearances_ddf = dd.read_csv("Appearances.csv", dtype={
    'GS': 'float64',
    'G_defense': 'float64',
    'G_dh': 'float64',
    'G_ph': 'float64',
    'G_pr': 'float64',
    'lgID': 'object'
})

# Drop used columns
columns_to_drop = ['runSB', 'runCS', 'R/PA', 'R/W', 'cFIP', 'wOBA', 'wOBAScale']
woba_ddf = woba_ddf.drop(columns_to_drop, axis=1)

columns_to_drop = ['GS', 'G_defense', 'G_ph', 'G_pr', 'lgID', 'teamID', 'G_all', 'G_batting']
appearances_ddf = appearances_ddf.drop(columns_to_drop, axis=1)

columns_to_drop = ['stint', 'teamID', 'lgID', 'G', 'R', 'RBI', 'SB', 'CS', 'SO', 'GIDP', 'SH']
batting_ddf = batting_ddf.drop(columns_to_drop, axis=1)

columns_to_drop = ['yearID', 'votedBy', 'ballots', 'needed', 'votes', 'needed_note']
hof_ddf = hof_ddf.drop(columns_to_drop, axis=1)

# Filter and preprocess the data using Dask DataFrames
hof_filtered_ddf = hof_ddf[(hof_ddf["inducted"] == "Y")]


# Group by playerID and calculate the sum for each player
# Define custom aggregation functions
aggregations = {col: 'sum' for col in appearances_ddf.columns if col != 'playerID' and col != 'yearID'}
aggregations['yearID'] = 'max'

# Group by playerID and apply the custom aggregations
appearances_grouped_ddf = appearances_ddf.groupby("playerID").agg(aggregations)

# Reset the index to include playerID in the output
appearances_grouped_ddf = appearances_grouped_ddf.reset_index()

# Calculate the most_played_position for each player
appearances_grouped_ddf["most_played_position"] = appearances_grouped_ddf[
    ["G_c", "G_p", "G_1b", "G_2b", "G_3b", "G_ss", "G_of", "G_dh"]].idxmax(axis=1).str.replace("G_", "")

# Drop all columns except "most_played_position", "yearID", and "playerID"
columns_to_drop = [col for col in appearances_grouped_ddf.columns if
                   col not in ["playerID", "yearID", "most_played_position"]]
appearances_grouped_ddf = appearances_grouped_ddf.drop(columns=columns_to_drop)

# Calculate wOBA for each player
batting_ddf = batting_ddf.merge(woba_ddf, left_on="yearID", right_on="YearID")
batting_ddf["woba_numerator"] = (
        batting_ddf["wBB"] * (batting_ddf["BB"] - batting_ddf["IBB"].fillna(0)) + batting_ddf["wHBP"] * batting_ddf[
    "HBP"].fillna(0) + batting_ddf["w1B"] * (
                batting_ddf["H"] - batting_ddf["2B"] - batting_ddf["3B"] - batting_ddf["HR"]) + batting_ddf[
            "w2B"] * batting_ddf["2B"] + batting_ddf["w3B"] * batting_ddf["3B"] + batting_ddf["wHR"] * batting_ddf[
            "HR"])
batting_ddf["woba_denominator"] = (
        batting_ddf["AB"] + batting_ddf["BB"] - batting_ddf["IBB"].fillna(0) + batting_ddf["HBP"].fillna(0) +
        batting_ddf["SF"].fillna(0))
batting_grouped_ddf = batting_ddf.groupby("playerID").agg(
    {"woba_numerator": "sum", "woba_denominator": "sum", "AB": "sum"})
batting_grouped_ddf["woba"] = batting_grouped_ddf.apply(
    lambda x: calculate_woba(x["woba_numerator"], x["woba_denominator"]), axis=1, meta='float')
batting_grouped_ddf = batting_grouped_ddf.reset_index()

# Drop all columns except "most_played_position", "yearID", and "playerID"
columns_to_drop = [col for col in batting_grouped_ddf.columns if col not in ["playerID", "AB", "woba"]]
batting_grouped_ddf = batting_grouped_ddf.drop(columns=columns_to_drop)

# Merge DataFrames and filter the data
player_stats_ddf = appearances_grouped_ddf.merge(batting_grouped_ddf, on="playerID")
player_stats_ddf = player_stats_ddf.merge(hof_filtered_ddf[["playerID", "inducted", "category"]], on="playerID", how="left")
player_stats_ddf["inducted"] = player_stats_ddf["inducted"].fillna("N")

player_stats_ddf = player_stats_ddf[(player_stats_ddf["yearID"] < 2017)]

# Calculate the results for each position
position_list = ["c", "of", "1b", "2b", "3b", "ss", "dh"]
combined_results = []

for position in position_list:
    total_players = len(player_stats_ddf[(player_stats_ddf["most_played_position"] == position) & (
            player_stats_ddf["inducted"] == "Y") & (player_stats_ddf["category"] == "Player")])
    inducted_players = player_stats_ddf[(player_stats_ddf["most_played_position"] == position) & (
            player_stats_ddf["inducted"] == "Y") & (player_stats_ddf["category"] == "Player")].compute().sort_values(by="woba")
    percentile_index = math.ceil(len(inducted_players) * 0.25) - 1
    percentile_woba = inducted_players.iloc[percentile_index]["woba"]
    eligible_players = player_stats_ddf[
        (player_stats_ddf["most_played_position"] == position) & (player_stats_ddf["woba"] > percentile_woba) & (
                player_stats_ddf["inducted"] == "N") & (player_stats_ddf["AB"] > 4000)].compute()
    eligible_players["players_beaten"] = eligible_players["woba"].apply(lambda x: sum(inducted_players["woba"] < x))
    eligible_players["percentage"] = eligible_players.apply(
        lambda x: calculate_percentage(x["players_beaten"], total_players), axis=1)

    # Drop all columns except needed ones
    columns_to_drop = [col for col in eligible_players.columns if
                       col not in ["playerID", "most_played_position", "woba", "percentage"]]
    eligible_players = eligible_players.drop(columns=columns_to_drop)


    combined_results.append(eligible_players)

# Concatenate the results for each position
all_results = dd.concat(combined_results)

# Write the combined DataFrame to a single CSV file
output_path = "orojo.as4"
all_results.to_csv(output_path, index=False, single_file=True)
