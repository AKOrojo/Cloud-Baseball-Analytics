# Baseball Analysis Project

## Overview

This project performs an in-depth analysis of baseball statistics to identify potential Hall of Fame candidates who may have been overlooked. The analysis is implemented using various big data technologies and frameworks to demonstrate different approaches to handling and processing large datasets.

## Implementations

The project has been implemented using the following technologies:

1. MapReduce (HDFS)
2. Dask
3. MongoDB
4. PySpark
5. SQL

Each implementation achieves the same result but leverages different tools and paradigms for big data processing.

## Analysis Process

The analysis follows these general steps across all implementations:

1. Load and process data from multiple CSV files:
   - `woba.csv`: Weighted On-Base Average (wOBA) coefficients by year
   - `HallOfFame.csv`: Information about players inducted into the Hall of Fame
   - `Batting.csv`: Players' batting statistics
   - `Appearances.csv`: Players' game appearance data

2. Calculate each player's career wOBA and determine their primary playing position.

3. For each position (Catcher, Outfielder, First Base, Second Base, Third Base, Shortstop):
   - Identify players already inducted into the Hall of Fame
   - Calculate the 25th percentile wOBA for inducted players
   - Find non-inducted players who exceed this 25th percentile wOBA
   - Calculate the percentage of inducted players that each candidate's wOBA exceeds

4. Output a list of potential Hall of Fame candidates, including their playerID, position, wOBA, and the percentage of inducted players they outperform.

## Requirements

- Python 3.7+

## Future Work

- Implement additional statistical analyses
- Create visualizations of the results
- Expand the dataset to include more recent years
- Optimize performance for each implementation