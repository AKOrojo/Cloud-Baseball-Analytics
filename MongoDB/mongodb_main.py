import csv
import math
from pymongo import MongoClient
from as5_cfg import mongo

# Establish a connection to the MongoDB database
client = MongoClient(username=mongo['username'], password=mongo['password'])
db = client[mongo['database']]


# Function to print documents in a MongoDB collection
def print_documents_in_collection(collection_name):
    """
    Print documents in the specified MongoDB collection.

    :param collection_name: The name of the MongoDB collection to print the documents from.
    """
    cursor = db[collection_name].find()
    for document in cursor:
        print(document)


# Function to drop a MongoDB collection if it exists
def drop_collection_if_exists(database, collection_name):
    """
    Drop the specified collection from the database if it exists.

    :param database: A MongoDB database instance.
    :param collection_name: The name of the collection to be dropped.
    """
    if collection_name in database.list_collection_names():
        database[collection_name].drop()


# Function to count the number of players beaten by a given player's wOBA
def count_beaten_players(player_woba, inducted_players_eligible):
    """
        Count the number of inducted players that have a lower career wOBA than the given player.

        :param player_woba: The weighted On-Base Average (wOBA) of the player to compare. :type player_woba: float
        :param inducted_players_eligible: A list of dictionaries containing inducted players' data, including
        'career_woba' key. :type inducted_players_eligible: list :return: The number of inducted players with lower
        career wOBA than the given player. :rtype: int
    """
    beaten_count = 0
    for inducted_player in inducted_players_eligible:
        if player_woba > inducted_player['career_woba']:
            beaten_count += 1
    return beaten_count


# Check if the 'inducted_players' collection exists and drop it if it does
drop_collection_if_exists(db, 'inducted_players')

# Pipeline to aggregate data from 'halloffame' collection and create a new 'inducted_players' collection
pipeline = [
    {
        "$lookup": {
            "from": "halloffame",
            "localField": "playerID",
            "foreignField": "playerID",
            "as": "hof_data"
        }
    },
    {
        "$unwind": "$hof_data"
    },
    {
        "$match": {
            "hof_data.inducted": "Y"
        }
    },
    {
        "$project": {
            "_id": 0,
            "playerid": "$playerID",
            "category": "$hof_data.category",
            "inducted": "$hof_data.inducted"
        }
    },
    {
        "$out": "inducted_players"
    }
]

db.halloffame.aggregate(pipeline)
inducted_players_eligible = db.inducted_players.find()

# Check if the 'player_position' collection exists and drop it if it does
drop_collection_if_exists(db, 'player_position')

# Pipeline to aggregate data from 'appearances' collection and create a new 'player_position' collection
pipeline = [
    {
        "$group": {
            "_id": "$playerID",
            "total_G_ss": {"$sum": {"$cond": [{"$eq": ["$G_ss", ""]}, 0, {"$toInt": "$G_ss"}]}},
            "total_G_1b": {"$sum": {"$cond": [{"$eq": ["$G_1b", ""]}, 0, {"$toInt": "$G_1b"}]}},
            "total_G_c": {"$sum": {"$cond": [{"$eq": ["$G_c", ""]}, 0, {"$toInt": "$G_c"}]}},
            "total_G_p": {"$sum": {"$cond": [{"$eq": ["$G_p", ""]}, 0, {"$toInt": "$G_p"}]}},
            "total_G_2b": {"$sum": {"$cond": [{"$eq": ["$G_2b", ""]}, 0, {"$toInt": "$G_2b"}]}},
            "total_G_3b": {"$sum": {"$cond": [{"$eq": ["$G_3b", ""]}, 0, {"$toInt": "$G_3b"}]}},
            "total_G_dh": {"$sum": {"$cond": [{"$eq": ["$G_dh", ""]}, 0, {"$toInt": "$G_dh"}]}},
            "total_G_of": {"$sum": {"$cond": [{"$eq": ["$G_of", ""]}, 0, {"$toInt": "$G_of"}]}},
        }
    },
    {
        "$project": {
            "_id": 0,
            "playerID": "$_id",
            "most_played_position": {
                "$arrayElemAt": [
                    {"$filter": {
                        "input": {"$objectToArray": {
                            "C": "$total_G_c",
                            "P": "$total_G_p",
                            "1B": "$total_G_1b",
                            "2B": "$total_G_2b",
                            "3B": "$total_G_3b",
                            "SS": "$total_G_ss",
                            "DH": "$total_G_dh",
                            "OF": "$total_G_of"
                        }},
                        "as": "item",
                        "cond": {"$eq": ["$$item.v", {"$max": [
                            "$total_G_c", "$total_G_p", "$total_G_1b", "$total_G_2b",
                            "$total_G_3b", "$total_G_ss", "$total_G_dh", "$total_G_of"
                        ]}]},
                    }},
                    0
                ]}
        }
    },
    {
        "$out": "player_position"
    }
]

db.appearances.aggregate(pipeline)
player_position = db.player_position.find()

# Check if the 'career_woba' collection exists and drop it if it does
drop_collection_if_exists(db, 'career_woba')

# Pipeline to aggregate data from 'batting' collection and create a new 'career_woba' collection
combined_pipeline = [
    {
        "$lookup": {
            "from": "woba",
            "localField": "yearID",
            "foreignField": "Season",
            "as": "woba_data"
        }
    },
    {
        "$unwind": "$woba_data"
    },
    {
        "$group": {
            "_id": "$playerID",
            "weighted_total": {
                "$sum": {
                    "$add": [
                        {"$multiply": [{"$toDouble": "$woba_data.wBB"}, {
                            "$subtract": [{"$convert": {"input": "$BB", "to": "double", "onError": 0}},
                                          {"$convert": {"input": "$IBB", "to": "double", "onError": 0}}]}]},
                        {"$multiply": [{"$toDouble": "$woba_data.wHBP"},
                                       {"$convert": {"input": "$HBP", "to": "double", "onError": 0}}]},
                        {"$multiply": [{"$toDouble": "$woba_data.w1B"}, {"$subtract": [{"$subtract": [{"$subtract": [
                            {"$convert": {"input": "$H", "to": "double", "onError": 0}},
                            {"$convert": {"input": "$2B", "to": "double", "onError": 0}}]}, {"$convert": {
                            "input": "$3B", "to": "double", "onError": 0}}]}, {"$convert": {"input": "$HR",
                                                                                            "to": "double",
                                                                                            "onError": 0}}]}]},
                        {"$multiply": [{"$toDouble": "$woba_data.w2B"},
                                       {"$convert": {"input": "$2B", "to": "double", "onError": 0}}]},
                        {"$multiply": [{"$toDouble": "$woba_data.w3B"},
                                       {"$convert": {"input": "$3B", "to": "double", "onError": 0}}]},
                        {"$multiply": [{"$toDouble": "$woba_data.wHR"},
                                       {"$convert": {"input": "$HR", "to": "double", "onError": 0}}]}
                    ]
                }
            },
            "total_denominator": {
                "$sum": {
                    "$add": [
                        {"$convert": {"input": "$AB", "to": "double", "onError": 0}},
                        {"$subtract": [{"$convert": {"input": "$BB", "to": "double", "onError": 0}},
                                       {"$convert": {"input": "$IBB", "to": "double", "onError": 0}}]},
                        {"$convert": {"input": "$HBP", "to": "double", "onError": 0}},
                        {"$convert": {"input": "$SF", "to": "double", "onError": 0}}
                    ]
                }
            },
            "retirement_year": {"$max": "$yearID"},
            "total_AB": {"$sum": {"$cond": [{"$eq": ["$AB", ""]}, 0, {"$toInt": "$AB"}]}}
        }
    },
    {
        "$project": {
            "_id": 0,
            "playerID": "$_id",
            "career_woba": {
                "$cond": [
                    {"$eq": ["$total_denominator", 0]},
                    None,  # Replace 'None' with 'null'
                    {"$divide": ["$weighted_total", "$total_denominator"]}
                ]
            },
            "retirement_year": 1,
            "total_AB": 1
        }
    },
    {
        "$out": "career_woba"
    }
]

db.batting.aggregate(combined_pipeline)
career_woba = db.career_woba.find()

# Check if the 'player_stats' collection exists and drop it if it does
drop_collection_if_exists(db, 'player_stats')

# Pipeline to aggregate data from 'career_woba', 'player_position', and 'inducted_players' collections and create a
# new 'player_stats' collection
combined_pipeline = [
    {
        "$lookup": {
            "from": "player_position",
            "localField": "playerID",
            "foreignField": "playerID",
            "as": "position_data"
        }
    },
    {
        "$lookup": {
            "from": "inducted_players",
            "localField": "playerID",
            "foreignField": "playerid",
            "as": "inducted_data"
        }
    },
    {
        "$project": {
            "_id": 0,
            "playerID": 1,
            "career_woba": 1,
            "retirement_year": 1,
            "total_AB": 1,
            "most_played_position": {"$arrayElemAt": ["$position_data.most_played_position", 0]},
            "inducted": {
                "$ifNull": [
                    {"$arrayElemAt": ["$inducted_data.inducted", 0]},
                    "N"
                ]
            },
            "category": {
                "$ifNull": [
                    {"$arrayElemAt": ["$inducted_data.category", 0]},
                    "player"
                ]
            }
        }
    },
    {
        "$out": "player_stats"
    }
]

db.career_woba.aggregate(combined_pipeline)
player_stats = db.player_stats.find()

position_list = ["C", "OF", "1B", "2B", "3B", "SS", 'DH']

# Initialize a list to store combined results
combined_results = []

for position in position_list:
    # Query inducted_players_eligible and eligible_players
    total_players = db.player_stats.count_documents({
        "most_played_position.k": position,
        "retirement_year": {"$lt": "2017"},
        "inducted": "Y",
        "category": "Player"
    })

    inducted_players_eligible = list(db.player_stats.find({
        "most_played_position.k": position,
        "retirement_year": {"$lt": "2017"},
        "inducted": "Y",
        "category": "Player"
    }).sort("career_woba", 1))

    length = len(inducted_players_eligible)
    percentile_index = math.ceil(length * 0.25) - 1
    percentile_woba = inducted_players_eligible[percentile_index]["career_woba"]

    eligible_players = list(db.player_stats.aggregate([
        {"$match": {
            "category": "player",
            "most_played_position.k": position,
            "retirement_year": {"$lt": "2017"},
            "career_woba": {"$gt": percentile_woba},
            "total_AB": {"$gt": 4000},
            "inducted": "N",
            "playerID": {"$nin": [player["playerID"] for player in inducted_players_eligible]}
        }},
        {"$lookup": {
            "from": "player_stats",
            "let": {"player_woba": "$career_woba"},
            "pipeline": [
                {"$match": {
                    "category": "player",
                    "most_played_position.k": position,
                    "retirement_year": {"$lt": "2017"},
                    "inducted": "Y",
                    "$expr": {"$lt": ["$career_woba", "$$player_woba"]}
                }}
            ],
            "as": "players_beaten"
        }},
        {"$project": {
            "_id": 0,
            "playerID": 1,
            "most_played_position": "$most_played_position.k",
            "career_woba": 1
        }},
        {"$sort": {"playerID": 1}}
    ]))

    inducted_players_count = len(inducted_players_eligible)

    # Calculate players_beaten_percentage for each eligible player
    for player in eligible_players:
        beaten_count = count_beaten_players(player['career_woba'], inducted_players_eligible)
        player['players_beaten_percentage'] = (beaten_count / inducted_players_count) * 100

    combined_results.extend(eligible_players)

# Save combined_results to a CSV file
output_path = "orojo.as5"
with open(output_path, mode="w", newline='') as output_file:
    writer = csv.DictWriter(output_file,
                            fieldnames=["playerID", "most_played_position", "career_woba", "players_beaten_percentage"])
    writer.writeheader()
    for row in combined_results:
        writer.writerow(row)
