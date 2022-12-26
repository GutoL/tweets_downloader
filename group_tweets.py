import pandas as pd
import os
import sys

# If you want to group all CSV files that were downloaded by the tool, you can use this script
# You need to copy this file to the folder where the tweets were downloaded and run it passing 
# as parameter the output filename: python group_tweets.py output.csv

if len(sys.argv) == 1:
    raise Exception("Please, provide the output filename, e.g.: python group_tweets.py output.csv")

df = pd.DataFrame()
sep = '|'

for filename in os.listdir():
    if '.csv' in filename:
        temp_df = pd.read_csv(filename, sep=sep)

        df = pd.concat([df, temp_df])

df.sort_values(by='created_at', inplace=True)

filter_columns = {'lang':'en'}

if filter_columns:
    for column in filter_columns:
        df = df[df[column] == filter_columns[column]]

df.to_csv(sys.argv[1], sep=sep)