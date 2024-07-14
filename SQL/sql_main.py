import math
import csv
import mysql.connector
from mysql.connector import Error


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


def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='baseball',
            user='your_username',
            password='your_password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return None


def execute_query(connection, query, params=None):
    cursor = connection.cursor(dictionary=True)
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()


def main():
    connection = create_connection()
    if connection is None:
        return

    try:
        # Register user-defined functions
        connection.create_function("calculate_woba", 2, calculate_woba)
        connection.create_function("calculate_percentage", 2, calculate_percentage)

        # SQL query to generate player stats table
        player_stats_query = """
        CREATE TEMPORARY TABLE playerStats AS
        SELECT p.playerID, p.most_played_position, p.max_yearid, p.woba, p.total_ab, 
               CASE WHEN h.inducted = 'Y' THEN 'Y' ELSE 'N' END AS inducted 
        FROM (
            SELECT t.playerID, t.most_played_position, r.max_yearid, r.woba, r.total_ab 
            FROM (
                SELECT playerID, 
                CASE 
                    WHEN total_G_c >= GREATEST(total_G_p, total_G_1b, total_G_2b, total_G_3b, total_G_ss, total_G_of, total_G_dh) THEN 'C'
                    WHEN total_G_p >= GREATEST(total_G_c, total_G_1b, total_G_2b, total_G_3b, total_G_ss, total_G_of, total_G_dh) THEN 'P'
                    WHEN total_G_1b >= GREATEST(total_G_c, total_G_p, total_G_2b, total_G_3b, total_G_ss, total_G_of, total_G_dh) THEN '1B'
                    WHEN total_G_2b >= GREATEST(total_G_c, total_G_p, total_G_1b, total_G_3b, total_G_ss, total_G_of, total_G_dh) THEN '2B'
                    WHEN total_G_3b >= GREATEST(total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_ss, total_G_of, total_G_dh) THEN '3B'
                    WHEN total_G_ss >= GREATEST(total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_3b, total_G_of, total_G_dh) THEN 'SS'
                    WHEN total_G_dh >= GREATEST(total_G_c, total_G_p, total_G_1b, total_G_2b, total_G_3b, total_G_of, total_G_ss) THEN 'DH'
                    ELSE 'OF'
                END AS most_played_position
                FROM (
                    SELECT playerID, 
                           SUM(G_c) AS total_G_c, SUM(G_p) AS total_G_p, 
                           SUM(G_1b) AS total_G_1b, SUM(G_2b) AS total_G_2b, 
                           SUM(G_3b) AS total_G_3b, SUM(G_ss) AS total_G_ss, 
                           SUM(G_lf + G_cf + G_rf) AS total_G_of, SUM(G_dh) AS total_G_dh
                    FROM appearances 
                    GROUP BY playerID
                ) t
            ) t
            JOIN (
                SELECT b.playerid, MAX(b.yearid) as max_yearid,
                       calculate_woba(SUM(w.wBB * (b.BB - IFNULL(b.IBB, 0)) + w.wHBP * IFNULL(b.HBP, 0) + 
                                          w.w1B * (b.H - b.2B - b.3B - b.HR) + w.w2B * b.2B + w.w3B * b.3B + w.wHR * b.HR), 
                                      SUM(b.AB + b.BB - IFNULL(b.IBB, 0) + IFNULL(b.HBP, 0) + IFNULL(b.SF, 0))) as woba,
                       SUM(b.AB) as total_ab
                FROM batting b
                JOIN woba w ON b.yearid = w.YearID
                GROUP BY b.playerid
            ) r ON t.playerID = r.playerid
        ) p
        LEFT JOIN halloffame h ON p.playerID = h.playerID
        GROUP BY p.playerID, p.most_played_position, p.max_yearid, p.woba, p.total_ab, h.inducted
        """

        # Execute the query to create the temporary table
        execute_query(connection, player_stats_query)

        # Process results for each position
        position_list = ["C", "OF", "1B", "2B", "3B", "SS"]
        combined_results = []

        for position in position_list:
            total_players_query = """
            SELECT COUNT(*) as total_players
            FROM playerStats
            WHERE most_played_position = %s AND max_yearid < 2017 AND inducted = 'Y'
            """
            total_players_result = execute_query(connection, total_players_query, (position,))
            total_players = total_players_result[0]['total_players']

            pos_query = """
            SELECT *
            FROM playerStats
            WHERE most_played_position = %s AND max_yearid < 2017 AND inducted = 'Y'
            ORDER BY woba ASC
            """
            pos_results = execute_query(connection, pos_query, (position,))

            length = len(pos_results)
            percentile_index = math.ceil(length * 0.25) - 1
            percentile_woba = pos_results[percentile_index]['woba']

            percentile_check_query = """
            SELECT p.playerID, p.most_played_position, p.woba,
                   calculate_percentage(COUNT(p2.playerID), %s) as percentage
            FROM playerStats p
            LEFT JOIN playerStats p2 ON p2.most_played_position = p.most_played_position
                                       AND p2.max_yearid < 2017
                                       AND p2.woba < p.woba
                                       AND p2.inducted = 'Y'
            WHERE p.most_played_position = %s AND p.max_yearid < 2017 AND p.woba > %s 
                  AND p.total_ab > 4000 AND p.inducted = 'N'
            GROUP BY p.playerID, p.most_played_position, p.woba
            ORDER BY p.playerID ASC
            """
            percentile_check_results = execute_query(connection, percentile_check_query,
                                                     (total_players, position, percentile_woba))
            combined_results.extend(percentile_check_results)

        # Write results to CSV
        output_path = "orojo.as3"
        with open(output_path, 'w', newline='') as csvfile:
            if combined_results:
                fieldnames = combined_results[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in combined_results:
                    writer.writerow(row)
            else:
                print("No results to write to CSV.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()


if __name__ == "__main__":
    main()
