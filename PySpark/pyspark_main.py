import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


# Define a Python function to calculate wOBA
def calculate_woba(sum_numerator, sum_denominator):
    """
        Calculate the wOBA (Weighted On-Base Average) given the sum of numerators and denominators.

        :param sum_numerator: float, the sum of wOBA numerators
        :param sum_denominator: float, the sum of wOBA denominators
        :return: float, the wOBA value
    """
    if sum_denominator == 0:
        return 0.0
    woba = sum_numerator / sum_denominator
    return float(woba)


# Define a Python function to calculate percentage beaten
def calculate_percentage(players_beaten, players):
    """
        Calculate the percentage of players beaten.

        :param players_beaten: float, number of players beaten
        :param players: float, total number of players
        :return: float, the percentage of players beaten
    """
    if players == 0:
        return 0.0
    percentage = (players_beaten / players) * 100
    return float(percentage)


# create a SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("baseball-analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Register the Python function as a UDF
calculate_woba_udf = udf(calculate_woba, FloatType())
spark.udf.register("calculate_woba", calculate_woba_udf)

calculate_percentage_udf = udf(calculate_percentage, FloatType())
spark.udf.register("calculate_percentage", calculate_percentage_udf)

# read CSV files
woba_df = spark.read.csv("woba.csv", header=True, inferSchema=True)
hof_df = spark.read.csv("HallOfFame.csv", header=True, inferSchema=True)
batting_df = spark.read.csv("Batting.csv", header=True, inferSchema=True)
appearances_df = spark.read.csv("Appearances.csv", header=True, inferSchema=True)

# register DataFrame as SQL table
woba_df.createOrReplaceTempView("woba")
hof_df.createOrReplaceTempView("halloffame")
batting_df.createOrReplaceTempView("batting")
appearances_df.createOrReplaceTempView("appearances")

# SQL query to generate player stats table
playerStatsTable = """SELECT p.playerID, p.most_played_position, p.max_yearid, p.woba, p.total_ab, CASE WHEN 
h.inducted = 'Y' THEN 'Y' ELSE 'N' END AS inducted FROM ( SELECT t.playerID, t.most_played_position, r.max_yearid, 
r.woba, r.total_ab FROM ( SELECT playerID, CASE WHEN total_G_c >= GREATEST( total_G_p, total_G_1b, total_G_2b, 
total_G_3b, total_G_ss, total_G_of, total_G_dh ) THEN 'C' WHEN total_G_p >= GREATEST( total_G_c, total_G_1b, 
total_G_2b, total_G_3b, total_G_ss, total_G_of, total_G_dh ) THEN 'P' WHEN total_G_1b >= GREATEST( total_G_c, 
total_G_p, total_G_2b, total_G_3b, total_G_ss, total_G_of, total_G_dh ) THEN '1B' WHEN total_G_2b >= GREATEST( 
total_G_c, total_G_p, total_G_1b, total_G_3b, total_G_ss, total_G_of, total_G_dh ) THEN '2B' WHEN total_G_3b >= 
GREATEST( total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_ss, total_G_of, total_G_dh ) THEN '3B' WHEN 
total_G_ss >= GREATEST( total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_3b, total_G_of, total_G_dh ) THEN 'SS' 
WHEN total_G_dh >= GREATEST( total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_3b, total_G_of, total_G_ss, 
total_G_dh ) THEN 'DH' ELSE 'OF' END AS most_played_position FROM ( SELECT playerID, SUM(G_c) AS total_G_c, 
SUM(G_p) AS total_G_p, SUM(G_1b) AS total_G_1b, SUM(G_2b) AS total_G_2b, SUM(G_3b) AS total_G_3b, SUM(G_ss) AS 
total_G_ss, SUM(G_lf + G_cf + G_rf) AS total_G_of FROM appearances GROUP BY playerID ) t ) t JOIN ( SELECT 
b.playerid, MAX(b.yearid) as max_yearid, calculate_woba(SUM(w.wBB * (b.BB - COALESCE(b.IBB, 0)) + w.wHBP * COALESCE(
b.HBP, 0) + w.w1B * (b.H - b.2B - b.3B - b.HR) + w.w2B * b.2B + w.w3B * b.3B + w.wHR * b.HR), SUM(b.AB + b.BB - 
COALESCE(b.IBB, 0) + COALESCE(b.HBP, 0) + COALESCE(b.SF, 0))) as woba, SUM(b.AB) as total_ab FROM batting b JOIN woba 
w ON b.yearid = w.YearID GROUP BY b.playerid ) r ON t.playerID = r.playerid ) p LEFT JOIN halloffame h ON p.playerID 
= h.playerID GROUP BY p.playerID, p.most_played_position, p.max_yearid, p.woba, p.total_ab, h.inducted
"""

result = spark.sql(playerStatsTable)
result.createOrReplaceTempView("playerStats")

# Loop through each position and gather data on eligible players
positionList = ["C", "OF", "1B", "2B", "3B", "SS"]

combined_results = []

# Loop through each position and gather data on eligible players
for position in positionList:
    # Query to get the total number of players inducted into the Hall of Fame for each position
    total_players_query = f"""
            SELECT COUNT(*) as total_players
            FROM playerStats
            WHERE most_played_position = '{position}' AND max_yearid < 2017 AND inducted = 'Y'
        """
    total_players_result = spark.sql(total_players_query)
    total_players = total_players_result.collect()[0]["total_players"]

    # Query to get Hall of Fame inducted players for each position, ordered by wOBA
    posQuery = f"""
        SELECT *
        FROM playerStats
        WHERE most_played_position = '{position}' AND max_yearid < 2017 AND inducted = 'Y'
        ORDER BY woba ASC
    """

    posResults = spark.sql(posQuery)
    posResultsList = posResults.collect()
    length = len(posResultsList)

    # Calculate the 25th percentile index of wOBA for inducted players
    percentile_index = math.ceil(length * 0.25) - 1
    percentileWoba = posResultsList[percentile_index]["woba"]

    # Query to get non-inducted players who beat the 25th percentile wOBA of inducted players for each position
    percentileCheckQuery = F"""
            SELECT p.playerID, p.most_played_position, p.woba,
                   calculate_percentage(COUNT(p2.playerID), {total_players}) as percentage
            FROM playerStats p
            LEFT JOIN playerStats p2 ON p2.most_played_position = p.most_played_position
                                       AND p2.max_yearid < 2017
                                       AND p2.woba < p.woba
                                       AND p2.inducted = 'Y'
            WHERE p.most_played_position = '{position}' AND p.max_yearid < 2017 AND p.woba > '{percentileWoba}' AND 
            p.total_ab > 4000 AND p.inducted = 'N' AND p.playerID NOT IN (SELECT playerID FROM ({posQuery}) AS subquery)
            GROUP BY p.playerID, p.most_played_position, p.woba
            ORDER BY p.playerID ASC
    """

    percentileCheckQueryResults = spark.sql(percentileCheckQuery)
    combined_results.append(percentileCheckQueryResults)

# Combine all DataFrames in combined_results using union()
if combined_results:
    all_results = combined_results[0]
    for i in range(1, len(combined_results)):
        all_results = all_results.union(combined_results[i])

    # Write the combined DataFrame to a single CSV file
    output_path = "orojo.as3"
    all_results.coalesce(1).write.csv(output_path, mode="overwrite", header=True)

# stop SparkSession
spark.stop()
