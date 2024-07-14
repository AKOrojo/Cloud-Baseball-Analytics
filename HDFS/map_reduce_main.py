from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import math


class BaseballAnalysis(MRJob):

    def configure_args(self):
        super(BaseballAnalysis, self).configure_args()
        self.add_file_arg('--woba', help='Path to woba.csv')
        self.add_file_arg('--hof', help='Path to HallOfFame.csv')
        self.add_file_arg('--batting', help='Path to Batting.csv')
        self.add_file_arg('--appearances', help='Path to Appearances.csv')

    def load_woba(self):
        self.woba = {}
        with open(self.options.woba, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                year = int(row['YearID'])
                self.woba[year] = {
                    'wBB': float(row['wBB']),
                    'wHBP': float(row['wHBP']),
                    'w1B': float(row['w1B']),
                    'w2B': float(row['w2B']),
                    'w3B': float(row['w3B']),
                    'wHR': float(row['wHR'])
                }

    def load_hof(self):
        self.hof = set()
        with open(self.options.hof, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['inducted'] == 'Y':
                    self.hof.add(row['playerID'])

    def calculate_woba(self, stats, year):
        w = self.woba[year]
        numerator = (
                w['wBB'] * (stats['BB'] - stats['IBB']) +
                w['wHBP'] * stats['HBP'] +
                w['w1B'] * (stats['H'] - stats['2B'] - stats['3B'] - stats['HR']) +
                w['w2B'] * stats['2B'] +
                w['w3B'] * stats['3B'] +
                w['wHR'] * stats['HR']
        )
        denominator = stats['AB'] + stats['BB'] - stats['IBB'] + stats['HBP'] + stats['SF']
        return numerator / denominator if denominator != 0 else 0

    def mapper_init(self):
        self.load_woba()
        self.load_hof()

    def mapper(self, _, line):
        if line.startswith('playerID'):  # skip header
            return

        parts = line.strip().split(',')
        if len(parts) == 7:  # Batting.csv
            playerID, yearID, AB, H, _2B, _3B, HR = parts
            yield playerID, ('batting', int(yearID), {
                'AB': int(AB),
                'H': int(H),
                '2B': int(_2B),
                '3B': int(_3B),
                'HR': int(HR),
                'BB': 0,
                'IBB': 0,
                'HBP': 0,
                'SF': 0
            })
        elif len(parts) == 22:  # Appearances.csv
            playerID, yearID, G_all, G_batting, G_defense, G_p, G_c, G_1b, G_2b, G_3b, G_ss, G_lf, G_cf, G_rf, G_of, G_dh = parts[
                                                                                                                            :16]
            yield playerID, ('appearances', int(yearID), {
                'G_c': int(G_c),
                'G_p': int(G_p),
                'G_1b': int(G_1b),
                'G_2b': int(G_2b),
                'G_3b': int(G_3b),
                'G_ss': int(G_ss),
                'G_of': int(G_lf) + int(G_cf) + int(G_rf),
                'G_dh': int(G_dh)
            })

    def reducer(self, playerID, values):
        batting_stats = {}
        appearances = {}
        max_year = 0
        total_ab = 0

        for value_type, year, stats in values:
            if value_type == 'batting':
                if year not in batting_stats:
                    batting_stats[year] = stats
                else:
                    for k, v in stats.items():
                        batting_stats[year][k] += v
                total_ab += stats['AB']
                max_year = max(max_year, year)
            elif value_type == 'appearances':
                if year not in appearances:
                    appearances[year] = stats
                else:
                    for k, v in stats.items():
                        appearances[year][k] += v

        if not batting_stats or not appearances:
            return

        # Calculate wOBA
        woba_numerator = 0
        woba_denominator = 0
        for year, stats in batting_stats.items():
            if year in self.woba:
                woba_numerator += self.calculate_woba(stats, year) * (
                            stats['AB'] + stats['BB'] - stats['IBB'] + stats['HBP'] + stats['SF'])
                woba_denominator += stats['AB'] + stats['BB'] - stats['IBB'] + stats['HBP'] + stats['SF']

        woba = woba_numerator / woba_denominator if woba_denominator != 0 else 0

        # Determine most played position
        position_totals = {
            'C': 0, 'P': 0, '1B': 0, '2B': 0, '3B': 0, 'SS': 0, 'OF': 0, 'DH': 0
        }
        for year_stats in appearances.values():
            for pos, games in year_stats.items():
                position = pos[2:]  # Remove 'G_' prefix
                position_totals[position] += games

        most_played_position = max(position_totals, key=position_totals.get)

        # Check if inducted
        inducted = 'Y' if playerID in self.hof else 'N'

        yield None, (playerID, most_played_position, max_year, woba, total_ab, inducted)

    def reducer_final(self, _, player_stats):
        player_stats = list(player_stats)
        positions = ['C', 'OF', '1B', '2B', '3B', 'SS']

        for position in positions:
            inducted_players = [p for p in player_stats if p[1] == position and p[2] < 2017 and p[5] == 'Y']
            inducted_players.sort(key=lambda x: x[3])  # Sort by wOBA

            if len(inducted_players) == 0:
                continue

            total_players = len(inducted_players)
            percentile_index = math.ceil(total_players * 0.25) - 1
            percentile_woba = inducted_players[percentile_index][3]

            eligible_players = [p for p in player_stats if
                                p[1] == position and p[2] < 2017 and p[3] > percentile_woba and p[4] > 4000 and p[
                                    5] == 'N']

            for player in eligible_players:
                players_beaten = sum(1 for p in inducted_players if p[3] < player[3])
                percentage = (players_beaten / total_players) * 100
                yield None, f"{player[0]},{player[1]},{player[3]:.6f},{percentage:.2f}"

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_final)
        ]


if __name__ == '__main__':
    BaseballAnalysis.run()