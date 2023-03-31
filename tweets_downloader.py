
from google_cloud_storage_manager import GoogleCloudStorageManager

import yaml
import glob
import pandas as pd

import json
import os
from datetime import datetime, timedelta
import time

from twarc.client2 import Twarc2
from twarc.expansions import ensure_flattened
from twarc_csv import DataFrameConverter, CSVConverter


# import sys
# print(sys.maxsize)
import csv
csv.field_size_limit(1000000*6)

class TweetsDownloader:
    def __init__(self, keys_filename, key_set, path_to_google_cloud_private_key_file=None, results_path=None,
                academic_privilege=False) -> None:

        self.read_credentials(keys_filename, key_set)

        self.tweet_date_format =  "%Y-%m-%d %H:%M:%S" # "%Y-%m-%dT%H:%M:%S.%fZ"
        self.results_path = results_path

        if academic_privilege:
            self.search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from
            self.maximum_query_size = 1024
        else:
            self.search_url = 'https://api.twitter.com/2/tweets/search/recent'
            self.maximum_query_size = 512

        if path_to_google_cloud_private_key_file:
          self.gcs_manager = GoogleCloudStorageManager(path_to_google_cloud_private_key_file)

        
        self.expansions=['author_id','referenced_tweets.id','referenced_tweets.id.author_id','entities.mentions.username',
                    'attachments.poll_ids','attachments.media_keys','in_reply_to_user_id','geo.place_id,edit_history_tweet_ids']
                    
        self.media_fields=['media_key', 'type', 'url', 'public_metrics', 'duration_ms', 'height', 'alt_text', 'width', 'variants', 'preview_image_url']

        self.tweet_fields=['attachments','author_id','context_annotations','conversation_id','created_at','edit_controls',
                        'edit_history_tweet_ids','entities','geo','id','in_reply_to_user_id','lang',
                        'possibly_sensitive','public_metrics','referenced_tweets',
                        'reply_settings','source','text','withheld']

        self.poll_fields=['duration_minutes','end_datetime','id','options','voting_status']

        self.place_fields=['contained_within','country','country_code','full_name','geo','id','name','place_type']

        self.user_fields=['created_at','description','entities','id','location','name','pinned_tweet_id','profile_image_url',
                    'protected','public_metrics','url', 'username','verified','withheld']
        

    def read_credentials(self, keys_filename, key_set):
        with open(keys_filename) as file:
          documents = yaml.full_load(file)
          
          self.endpoint = documents[key_set]['endpoint']
          self.consumer_key = documents[key_set]['consumer_key']
          self.consumer_secret = documents[key_set]['consumer_secret']
          self.access_token = documents[key_set]['access_token']
          self.access_token_secret = documents[key_set]['access_token_secret']
          self.bearer_token = documents[key_set]['bearer_token']

    # https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a
    def auth(self):
        # os.environ['TOKEN'] = bearer_token
        # return os.getenv('TOKEN')
        return self.bearer_token

    def create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def data_convert(self, src):
        
        print(src)

        data = pd.to_datetime(src)
        # print('date of latest tweet downloaded:', data)

        return data

        
    def datetime_range(self, start, end, delta):
      current = start
      while current < end:
          yield current
          current += delta

    def create_query(self, hashtags_file):
        hashtags_list = []

        if (len(hashtags_file) > 0) and (hashtags_file is not None):
            fp = open(hashtags_file, 'r', encoding="utf8")

            temp_hashtags_list = [hashtag.replace('\t', '').strip() for hashtag in fp.readlines()]
            
            fp.close()
            
            for hashtag in temp_hashtags_list:
                if len(hashtag) > 0:
                    if 'AND' in hashtag:
                        hashtag = '('+hashtag+')'
                    else:
                        hashtag = '"'+hashtag+'"'
                    
                    hashtags_list.append(hashtag)
                        
        return hashtags_list


    # Your queries will be limited depending on which access level you are using. 
    # If you have Essential or Elevated access, your query can be 512 characters long.
    # If you have Academic Research access, your query can be 1024 characters long. 
    def break_query(self, query, language=None, usernames_file=None, from_users=True, exclude_retweets=False):
      
      # Dealing with the language
      if language is None or len(language) == 0:
        language = ''
      else:
        language = 'lang:'+language
      
      # Dealing with the usernames

      if usernames_file is None or len(usernames_file) == 0:
        usernames = ''
      else:
        usernames_list = []
        usernames = '('

        fp = open(usernames_file, 'r')
        lines = fp.readlines()
        fp.close()

        for i, user in enumerate(lines):
            user = user.strip()
            
            if from_users:
                user = user.replace('@','')
                usernames += 'from:'+user
                usernames_list.append('from:'+user)
            else:
                usernames += user
                usernames_list.append(user)

            
            if i == len(lines)-1:
                usernames += ') '                
            else:
                usernames += ' OR '
      
      # Creating the query or the list of queries
      if len(self.create_temp_query(query, language, usernames, exclude_retweets)) > self.maximum_query_size:

        if len(query) > 0:
            hashtags = query.split('OR')
        else:
            hashtags = []
        
        query_chunks = []

        # Creating list of queries based on the hashtags and terms
        if len(hashtags) > 0:
            temp_query = self.create_temp_query('', language, usernames, exclude_retweets)

            for i, hashtag in enumerate(hashtags):
                if len(temp_query+' OR '+hashtag) <= self.maximum_query_size:
                    if i == 0:
                        temp_query += hashtag
                    else:
                        temp_query += ' OR '+hashtag

                else:
                    query_chunks.append(temp_query)
                    temp_query = self.create_temp_query(hashtag, language, usernames, exclude_retweets)

        # if the list of hashtags is empty, create the queries based on the list of usernames...
        elif len(hashtags) == 0 and len(usernames_list) > 0:
            temp_query = self.create_temp_query('', language, '', exclude_retweets)
            
            for i, username in enumerate(usernames_list):
                if len(temp_query+' OR '+username) <= self.maximum_query_size:
                    if i == 0:
                        temp_query += username
                    else:
                        temp_query += ' OR '+username

                else:
                    query_chunks.append(temp_query)
                    temp_query = self.create_temp_query(username, language, '', exclude_retweets)
                
        return query_chunks
      
      else:
        return [self.create_temp_query(query, language, usernames, exclude_retweets)]

    def create_temp_query(self, query, language, usernames, exclude_retweets, query_first=False):
        
        if exclude_retweets:
            temp_query = '-is:retweet '
        else:
            temp_query = ''
        
        if query_first:
            if len(query) == 0:
                temp_query += ''
            else:
                temp_query += '('+query+ ') '

        if len(usernames) > 0:
            temp_query += usernames

        if len(language) > 0:
            temp_query += ' '+language
        
        if len(temp_query) > 0:
            temp_query += ' '
        
        if not query_first:
            if len(query) == 0:
                temp_query += ''
            else:
                temp_query += '('+query+ ') '

        return temp_query


    def download_tweets(self, hashtags_file, usernames_from_file, start_date_list, end_date_list, time_interval_break, 
                        limit_tweets=100, chunck_size_to_save=1000, total_of_tweets=None, language=None, file_extension='csv', separator=',', 
                        save_on_disk=True, from_users=True, exclude_retweets=False, save_using_twarc_chunk=False,
                        generate_query=False):
        
        if not total_of_tweets:
            total_of_tweets = float('inf')

        if not generate_query:
            fp = open(hashtags_file, 'r')
            query_list = [fp.read()]
            fp.close()

            if len(query_list[0]) > self.maximum_query_size:
                Exception('Your query is too long. The maximum size is', self.maximum_query_size, 'but yout query has the size', len(query_list[0]))

        else:
            query = self.create_query(hashtags_file)
            
            if len(query) > 0:
                processed_query = query[0] # getting the firt term to create the query

                for term in query[1:]: # iterating over all terms except the first one
                    processed_query += ' OR ' + term
            else:
                processed_query = query
            
            query_list = self.break_query(processed_query, language=language, usernames_file=usernames_from_file, from_users=from_users, 
                                        exclude_retweets=exclude_retweets)
            # query_list = [re.sub(' +', ' ', t_query) for t_query in query_list] # removing duplicate spaces
        
        if 'csv' in file_extension:
            file_extension = '.csv'
        else:
            file_extension = '.xlsx'

        for start_date, end_date in zip(start_date_list, end_date_list):

            start_date = datetime.strptime(start_date, self.tweet_date_format).isoformat('T')+'Z'
            end_date = datetime.strptime(end_date, self.tweet_date_format).isoformat('T')+'Z'

            start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
            end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

            
            if time_interval_break:
                if list(time_interval_break.keys())[0] == 'days':
                    variation = timedelta(days=time_interval_break['days'])

                elif list(time_interval_break.keys())[0] == 'hours':
                    variation = timedelta(hours=time_interval_break['hours'])

                elif list(time_interval_break.keys())[0] == 'minutes':
                    variation = timedelta(minutes=time_interval_break['minutes'])

                # end_date += variation

                dts = [dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for dt in self.datetime_range(start_date, end_date, variation)]

            else:
                dts = [start_date, end_date]

            for j in range(len(dts)-1, 0 , -1):
                temp_start_date = dts[j-1]
                temp_end_date = dts[j]

                
                for x in range(len(query_list)):

                    newpath = self.results_path+str(x)+'/'

                    if not os.path.exists(newpath) and save_on_disk:
                        os.makedirs(newpath)
                        with open(newpath+'hashtags.txt', 'w', encoding="utf8") as fp:
                            fp.write(''.join(query_list[x]))

                    tweets_file_name, temp_start_date, temp_end_date = self.get_last_period_tweet_file(newpath, file_extension, x, temp_start_date, temp_end_date)
                                        
                    # print(tweets_file_name, 'temp_start_date',temp_start_date, 'temp_end_date',temp_end_date)

                    if tweets_file_name == False: # if you don't get the filename from the folder, create the name of the new file
                        tweets_file_name = self.results_path+str(x)+'/tweets_'+temp_start_date.replace(':','_')+'_'+temp_end_date.replace(':','_')+'_group_'+str(x)+file_extension

                    
                    if os.path.isfile(tweets_file_name):
                        
                        if 'csv' in tweets_file_name:
                            
                            # df_date_temp = pd.read_csv(tweets_file_name, sep=separator, encoding='utf-8', engine='c', low_memory=False)

                            with open(tweets_file_name, "rb") as file:
                                
                                date_col_idx = 0
                                for j, col in enumerate(file.readline().decode().split(separator)):
                                    if col == 'created_at':
                                        date_col_idx = j
                                        break

                                try:
                                    file.seek(-2, os.SEEK_END)
                                    while file.read(1) != b'\n':
                                        file.seek(-2, os.SEEK_CUR)
                                except OSError:
                                    file.seek(0)
                                last_line = file.readline().decode()

                                last_date = last_line.split(separator)[date_col_idx]

                        # else:
                        #     df_date_temp = pd.read_excel(tweets_file_name, sheet_name='tweets')

                        # print(df_date_temp['created_at'].iloc[-1])
                        # new_end_date = datetime.strptime(df_date_temp['created_at'].iloc[-1], "%Y-%m-%dT%H:%M:%S.%fZ")
                        new_end_date = last_date # df_date_temp['created_at'].iloc[-1]

                        # print('temp_end_date',temp_end_date)
                        # print('new_end_date', new_end_date, type(new_end_date))
                        # print(new_end_date < temp_end_date)
                        
                        if new_end_date < temp_end_date: #datetime.strptime(temp_end_date, "%Y-%m-%dT%H:%M:%S.%fZ"):
                            temp_end_date = new_end_date # datetime.strptime(new_end_date, "%Y-%m-%dT%H:%M:%S.%fZ")

                            

                    # All tweets for this time interval were downloaded
                    
                    if datetime.strptime(temp_start_date, "%Y-%m-%dT%H:%M:%S.%fZ") == datetime.strptime(temp_end_date, "%Y-%m-%dT%H:%M:%S.%fZ"):
                        continue
                    
                    print('Downloading tweets between:', temp_start_date, 'and', temp_end_date)
                    print('QUERY:', query_list[x])
                    print('-------------------------------------')
                    

                    self.download_tweets_with_twarc(query=query_list[x], start_time=temp_start_date, end_time=temp_end_date,
                                                        max_number_of_tweets=limit_tweets, total_of_tweets=total_of_tweets, 
                                                        tweets_file_name=tweets_file_name, separator=separator,
                                                        chunck_size_to_save=chunck_size_to_save, 
                                                        save_using_twarc_chunk=save_using_twarc_chunk)
                    
                    time.sleep(2) # '''

    # https://twarc-project.readthedocs.io/en/latest/api/library/
    def download_tweets_with_twarc(self, query, start_time, end_time, max_number_of_tweets, total_of_tweets,
                                    tweets_file_name, separator, chunck_size_to_save, save_using_twarc_chunk=False):
        

        t = Twarc2(bearer_token=self.bearer_token)

        start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ") # self.tweet_date_format
        end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

        search_results = t.search_all(query=query, start_time=start_time, end_time=end_time, max_results=max_number_of_tweets)

        tweets_pool = []

        # Get all results page by page:
        for page in search_results:
            # Do something with the whole page of results:
            # or alternatively, "flatten" results returning 1 tweet at a time, with expansions inline:
            
            # Do something with the page of results:
            if save_using_twarc_chunk:
                self.save_tweets_with_twarc_chunck(tweets_file_name=tweets_file_name, page=page) # batch_size=chunck_size_to_save

            else:
                for tweet in ensure_flattened(page):
                    # Do something with the tweet
                    tweets_pool.append(tweet)
                    # print(tweet['text'])
                
                if len(tweets_pool) > chunck_size_to_save:
                    self.save_tweets_on_disk(tweets_file_name, tweets_pool, separator, using_twarc=True)
                    tweets_pool = []
            
        if len(tweets_pool) > 0:
            self.save_tweets_on_disk(tweets_file_name, tweets_pool, separator, using_twarc=True)

    def save_tweets_with_twarc_chunck(self, tweets_file_name, page): # batch_size=1000
        
        # Get all results page by page:
        print('Saving more',len(page),'tweets...')
        with open(tweets_file_name.replace('csv','jsonl'), 'w+', encoding="utf-8") as f:
            f.write(json.dumps(page) + "\n")
        
        # This assumes `results.jsonl` is finished writing.
        with open(tweets_file_name.replace('.csv','.jsonl'), "r", encoding="utf-8") as infile:
            with open(tweets_file_name, 'w', encoding="utf-8") as outfile:
                converter = CSVConverter(infile, outfile) # batch_size=batch_size
                converter.process()


    def convert_string_to_twitter_date(self, string):
        return datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")

    def find_between(self, s, first, last ):
        try:
            start = s.index( first ) + len( first )
            end = s.index( last, start )
            return s[start:end]
        except ValueError:
            return ""

    def get_last_period_tweet_file(self, path, file_extension, group, start_date, end_date):

        files = [self.find_between(name, 'tweets_', '_group') for name in glob.glob(path+'*') if file_extension in name]
        
        if len(files) == 0: # if there are no CSV files, return the same start and end date. The file name will be created in the another function
            return False, start_date, end_date

        new_start_date = start_date
        new_end_date = end_date
        latest_file = ''

        files.sort(reverse=True)

        for filename in files:
            start_date_from_file = filename[0:filename.find('Z')+1].replace('_',':')
            end_date_from_file = filename[filename.find('Z')+2: len(filename)].replace('_',':')
            
            if end_date_from_file <= new_end_date and start_date_from_file <= new_start_date:
                new_end_date = end_date_from_file
                new_start_date = start_date_from_file

                latest_file = filename
                
        # if the current end date is lower than all end dates from files, then consider the current end and start date, 
        # and the filename will be created in the another function
        if len(latest_file) == 0:
            return False, start_date, end_date

        return path+'tweets_'+latest_file+'_group_'+str(group)+file_extension, new_start_date, new_end_date
                
    def save_tweets_on_disk(self, tweets_file_name, tweets_pool, separator, using_twarc=False):
        
        if using_twarc:
            # Default options for Dataframe converter
            converter = DataFrameConverter()
            new_df = converter.process(tweets_pool) 
            
        else:
            new_df = pd.json_normalize(tweets_pool)
            
        columns = new_df.columns            

        if os.path.isfile(tweets_file_name):
            if 'csv' in tweets_file_name:
                file_size = os.stat(tweets_file_name).st_size / (1024 ** 3) # get the filesize

                if file_size < 1: # if the file size is lower than 1gb
                    df = pd.read_csv(tweets_file_name, sep=separator, encoding='utf-8', engine='c', low_memory=False)
                else:
                    df = pd.DataFrame(columns=columns)

                    head, tail = os.path.split(tweets_file_name)

                    try:
                        idx = int(tweets_file_name[0])
                        tweets_file_name = head+'/'+str(idx+1)+'_'+tail

                    except:
                        tweets_file_name = head+'/1_'+tail

            else:
                df = pd.read_excel(tweets_file_name, sheet_name='tweets')
        
        else:
            df = pd.DataFrame(columns=columns)

        print('Saving more', len(tweets_pool), 'tweets at this time. Total number of tweets collected:', df.shape[0])
        
        tweets_df = pd.concat([df, new_df], ignore_index=True)

        # print('Saving:')
        # print(tweets_df)        
        
        # tweets_df.drop_duplicates(inplace=True)

        if '.csv' in tweets_file_name:
            tweets_df.to_csv(tweets_file_name, index=False, sep=separator, columns=columns, escapechar='\\')                

        elif '.xlsx' in tweets_file_name:
            with pd.ExcelWriter(tweets_file_name) as writer:  
                tweets_df.to_excel(writer, sheet_name='tweets', columns=columns)
              
    def upload_to_cloud_storage(self, bucket_name):
        # bucket_name = 'guto_test_bucket'

        # path_to_private_key_file = path+'config_files/stable-plasma-690-686708fe9d44.json'

        # gcs_manager = GoogleCloudStorageManager(path_to_private_key_file)

        # self.gcs_manager.get_all_buckets_names()
        self.gcs_manager.download_data(bucket_name)
        self.gcs_manager.upload_file(self.results_path, bucket_name)
