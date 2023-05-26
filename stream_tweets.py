import twarc
import yaml
import threading
import pandas as pd
from pandas import json_normalize
import glob
import os
import re
import sys
import json

class TwitterStreamming():

    def __init__(self, config) -> None:
        self.credentials = self.read_credentials(config['keys_file'], config['key_set'])
        self.results_path = config['results_path']
        self.result_batch_size = config['result_batch_size']
        self.terms_file = config['terms_file']
        self.separator = config['separator']

    def read_credentials(self, keys_filename, key_set):
        
        with open(keys_filename) as file:
            documents = yaml.full_load(file)
            return documents[key_set]

    def create_rules(self, terms):
        rules_list = []

        for i, term in enumerate(terms):

            if ('#' not in term) and ('@' not in term) and (len(term.split()) > 1):
                term = '"'+term+'"'

            rules_list.append({'value':term+' -is:retweet', 'tag': 'term-'+str(i)})
        
        return rules_list

    def collect_tweets_streaming(self):

        T = twarc.Twarc2(bearer_token=self.credentials['bearer_token'], consumer_secret=self.credentials['consumer_secret'])

        # remove any active stream rules
        rules = T.get_stream_rules()

        if "data" in rules and len(rules["data"]) > 0:
            rule_ids = [r["id"] for r in rules["data"]]
            T.delete_stream_rule_ids(rule_ids)

        # make sure they are empty
        rules = T.get_stream_rules()
        assert "data" not in rules

        fp = open(self.terms_file, 'r')
        terms = [line.strip() for line in fp.readlines()]
        fp.close()

        my_rules = self.create_rules(terms)

        print(my_rules)
        
        # add rules
        rules = T.add_stream_rules(
            my_rules
            # [{"value": "(manchester united) -is:retweet", "tag": "my-teams"}]
        )

        # make sure they are there
        rules = T.get_stream_rules()

        # collect some data
        event = threading.Event()

        result_files_name = glob.glob(self.results_path+'*.csv')
        result_files_name = [os.path.basename(filename) for filename in result_files_name]

        if len(result_files_name) == 0:
            batch_df = pd.DataFrame()
            result_file_name_to_save = '0.csv'

        else:
            result_files_name.sort()
            batch_df = pd.read_csv(self.results_path+result_files_name[-1])
            result_file_name_to_save = result_files_name[-1]

        # for count, result in enumerate(T.stream(event=event)):
        for result in T.stream(event=event):
            batch_df = pd.concat([batch_df, json_normalize(result["data"])])

            if batch_df.shape[0] > self.result_batch_size:
                print('Total tweets collected:', batch_df.shape[0], 'in', self.results_path+result_file_name_to_save)
                batch_df, result_file_name_to_save = self.save_file(batch_df, self.results_path, result_file_name_to_save)

        '''
        # delete the rules
        rule_ids = [r["id"] for r in rules["data"]]
        T.delete_stream_rule_ids(rule_ids)

        # make sure they are gone
        rules = T.get_stream_rules()
        assert "data" not in rules # '''

    def save_file(self, df, path, file_name):

        if not os.path.exists(path):
            os.makedirs(path)

        df.to_csv(path+file_name, sep=self.separator, index=False)

        file_size = (os.stat(path+file_name).st_size)

        if file_size/1000000000 > 1: # if the file size is bigger that 1GB
            df = pd.DataFrame() # '''
            idx = int(re.findall(r'\d+', file_name)[0])+1
            file_name = str(idx)+'.csv'
            
        return df, file_name

if len(sys.argv) == 1:
    print('Please, call the script: python twarc_stream_tweets.py config_file.json')
    exit()

config_fp = open(sys.argv[1])

twitter_streamming = TwitterStreamming(json.load(config_fp))
twitter_streamming.collect_tweets_streaming()