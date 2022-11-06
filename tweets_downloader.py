
from google_cloud_storage_manager import GoogleCloudStorageManager
from tweet_information_handler import TweetInformationHandler

import yaml
import requests
import dateutil
import pandas as pd
from pathlib import Path

import os
# import datetime
from datetime import datetime, timedelta
import time
import re

class TweetsDownloader:
    def __init__(self, keys_filename, key_set, path_to_google_cloud_private_key_file=None, results_path=None,
                academic_privilege=False) -> None:

        self.read_credentials(keys_filename, key_set)

        self.tweet_date_format = "%Y-%m-%d %H:%M:%S"
        self.results_path = results_path

        if academic_privilege:
            self.search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from
            self.maximum_query_size = 1024
        else:
            self.search_url = 'https://api.twitter.com/2/tweets/search/recent'
            self.maximum_query_size = 512

        if path_to_google_cloud_private_key_file:
          self.gcs_manager = GoogleCloudStorageManager(path_to_google_cloud_private_key_file)

    def read_credentials(self, keys_filename, key_set):
        with open(keys_filename) as file:
          documents = yaml.full_load(file)
          
          self.endpoint = documents[key_set]['endpoint']
          self.consumer_key = documents[key_set]['consumer_key']
          self.consumer_secret = documents[key_set]['consumer_secret']
          self.bearer_token = documents[key_set]['bearer_token']

    # https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a
    def auth(self):
        # os.environ['TOKEN'] = bearer_token
        # return os.getenv('TOKEN')
        return self.bearer_token

    def create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def create_url(self, keyword, start_date, end_date, max_results=10):
        
        #change params based on the endpoint you are using
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'end_time': end_date,
                        'max_results': max_results,

                        # 'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        # 'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        # 'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        # 'place.fields': 'full_name,id,country,country_code,geo,name,place_type',


                        'expansions': ('author_id,referenced_tweets.id,edit_history_tweet_ids,'+
                                      'in_reply_to_user_id,attachments.media_keys,attachments.poll_ids,'+
                                      'geo.place_id,entities.mentions.username,referenced_tweets.id.author_id'),

                        'tweet.fields': ('attachments,author_id,context_annotations,conversation_id,'+
                                         'created_at,edit_controls,edit_history_tweet_ids,entities,geo,'+
                                         'id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,'+
                                         'referenced_tweets,reply_settings,source,text,withheld'),
                        
                        'user.fields': ('created_at,description,entities,id,location,name,pinned_tweet_id,'+
                                        'profile_image_url,protected,public_metrics,url,username,verified,withheld'),
                    
                        'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
                        
                        'poll.fields': 'id,options,duration_minutes,end_datetime,voting_status',
                        
                        'media.fields': 'media_key,type,url,preview_image_url,public_metrics,width,alt_text,variants',
                        
                        'next_token': {}
                      }

        return (self.search_url, query_params)

    def connect_to_endpoint(self, url, headers, params, next_token=None):
        params['next_token'] = next_token   #params object received from create_url function
        response = requests.request("GET", url, headers=headers, params=params)
        # print("Endpoint Response Code: " + str(response.status_code))
        
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        
        # print(response.json())
        return response.json()
    
    def get_tweets_information_simplified(self, json_response):
        counter = 0
        new_rows = []

        #Loop through each tweet
        for tweet in json_response['data']:

            row = {}
            
            # We will create a variable for each since some of the keys might not exist for some tweets
            # So we will account for that

            # 1. Author ID
            row['author_id'] = tweet['author_id']

            # 2. Time created
            created_at = tweet['created_at']
            if len(created_at) > 19:
                created_at = created_at[:19]

            row['created_at'] = dateutil.parser.parse(created_at)

            # 3. Geolocation
            if ('geo' in tweet):   
                row['geo'] = tweet['geo']['place_id']
            else:
                row['geo'] = " "

            # 4. Tweet ID
            row['tweet_id'] = tweet['id']

            # 5. Language
            row['lang'] = tweet['lang']

            # 6. Tweet metrics
            row['retweet_count'] = tweet['public_metrics']['retweet_count']
            row['reply_count'] = tweet['public_metrics']['reply_count']
            row['like_count'] = tweet['public_metrics']['like_count']
            row['quote_count'] = tweet['public_metrics']['quote_count']

            # 7. source
            row['source'] = tweet['source']

            # 8. Tweet text
            row['text'] = tweet['text']

            new_rows.append(row)

            counter += 1

    def data_convert(self, src):
        return pd.to_datetime(src)

    def append_to_csv(self, json_response, tweets_file_name, save_on_disk=True):
        tweets_file_name = Path(tweets_file_name)

        #A counter variable
        counter = 0

        # csvFile['created_at'] = csvFile.created_at.apply(self.data_convert)
        
        #Open OR create the tweets CSV file
        if os.path.isfile(tweets_file_name):
            if 'csv' in tweets_file_name.name:
                tweets_df = pd.read_csv(tweets_file_name)
            elif 'xlsx' in tweets_file_name.name:
                tweets_df = pd.read_excel(tweets_file_name, sheet_name='tweets')
            
            if 'Unnamed: 0' in tweets_df.columns:
                tweets_df.drop('Unnamed: 0', axis=1, inplace=True)
                
        else:
            tweets_df = pd.DataFrame()
            
        # print('json_response\n',json.dumps(json_response, indent=4))
        # sys.exit()

        tweet_information_handler = TweetInformationHandler(json_response)

        new_rows = tweet_information_handler.get_full_information_from_tweets()
        
        counter += len(new_rows)                

        # Append the result to the CSV file
        # tweets_df = tweets_df.append(new_rows, ignore_index=True, sort=False)
        # print('___________________________________________')
        pd.DataFrame(new_rows)
        
        tweets_df = pd.concat([tweets_df, pd.DataFrame(new_rows)], ignore_index=True, sort=False)
        
        tweets_df['created_at'] = tweets_df['created_at'].apply(self.data_convert)        
        
        tweets_df['created_at'] = tweets_df['created_at'].dt.strftime(self.tweet_date_format)

        tweets_df.sort_values(by=['created_at'], inplace=True, ascending=True)

        if save_on_disk:
            if '.csv' in tweets_file_name.name:
                tweets_df.to_csv(tweets_file_name, index=False)                

            elif '.xlsx' in tweets_file_name.name:
                with pd.ExcelWriter(tweets_file_name) as writer:  
                    tweets_df.to_excel(writer, sheet_name='tweets')                    
        
        # Print the number of tweets for this iteration
        print("# of Tweets added from this response: ", counter)

    
    def datetime_range(self, start, end, delta):
      current = start
      while current < end:
          yield current
          current += delta

    def get_historical_tweets_from_hashtags(self, query, bearer_token, start_date, end_date, 
                                            number_of_tweets_per_call, max_count, limit_tweets_per_period, 
                                            tweets_file_name, save_on_disk, time_interval_break):
      #Inputs for tweets
      headers = self.create_headers(bearer_token)

      #Total number of tweets we collected from the loop
      total_tweets = 0

      # Inputs
      count = 0 # Counting tweets per time period
      flag = True
      next_token = None
      
      
      start_date = datetime.strptime(start_date, self.tweet_date_format)
      end_date = datetime.strptime(end_date, self.tweet_date_format) # "%Y-%m-%dT%H:%M:%S.%fZ"

      if list(time_interval_break.keys())[0] == 'days':
          variation = timedelta(days=time_interval_break['days'])

      elif list(time_interval_break.keys())[0] == 'hours':
          variation = timedelta(hours=time_interval_break['hours'])

      elif list(time_interval_break.keys())[0] == 'minutes':
          variation = timedelta(minutes=time_interval_break['minutes'])
            
      end_date += variation

      dts = [dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for dt in self.datetime_range(start_date, end_date, variation)]
      
      for x in range(len(dts)-1):
        first_date = dts[x]
        second_date = dts[x+1]
        
        # Check if flag is true
        while flag:
          ## Check if max_count reached
          if count >= max_count and limit_tweets_per_period == True:
              break

          url = self.create_url(query, first_date, second_date, number_of_tweets_per_call)

          
          json_response = self.connect_to_endpoint(url[0], headers, url[1], next_token) # Calling the Twitter API
          result_count = json_response['meta']['result_count']

          if result_count is not None and result_count > 0:
            self.append_to_csv(json_response, tweets_file_name, save_on_disk=save_on_disk)

            count += result_count
            total_tweets += result_count

            print("Start Date: ", first_date)
            print("End Date: ", second_date)
            print("Total # of Tweets added: ", total_tweets)
            print("-------------------")
            time.sleep(3) 

          if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']
            
          # If no next token exists
          else:
            #Since this is the final request, turn flag to false to move to the next time period.
            flag = False
            next_token = None

      print("Total number of results: ", total_tweets)

    def create_query(self, hashtags_file):
      fp = open(hashtags_file, 'r', encoding="utf8")

      hashtags_list = [hashtag.replace('\n', '').replace('\t', '').strip() for hashtag in fp.readlines()]
      hashtags_list = [hashtag for hashtag in hashtags_list if len(hashtag) > 0]

      fp.close()

      return ' '.join(hashtags_list)


    # Your queries will be limited depending on which access level you are using. 
    # If you have Essential or Elevated access, your query can be 512 characters long.
    # If you have Academic Research access, your query can be 1024 characters long. 
    def break_query(self, query, language=None):
    
      if language is None or len(language) == 0:
        language = ''
      else:
        language = 'lang:'+language

      if len(query+' lang:'+language) > self.maximum_query_size:

        hashtags = query.split('OR')
        hashtags_chunks = []

        temp_query = language+' '

        for i, hashtag in enumerate(hashtags):
          if len(temp_query+' OR '+hashtag) <= self.maximum_query_size:
            if i == 0:
              temp_query += hashtag
            else:
              temp_query += ' OR '+hashtag

          else:
            hashtags_chunks.append(temp_query)

            temp_query = language+' '+hashtag

        return hashtags_chunks
      
      else:
        return [query+' '+language]


    def download_tweets(self, hashtags_file, start_date_list, end_date_list, limit_tweets_per_period, 
                        time_interval_break, language=None, date_format='%Y-%m-%d %H:%M:%S', 
                        file_extension='csv', number_of_tweets_per_call=100, save_on_disk=True):    
      
      # Max tweets per time period. If you define that there is no limit, 
      # this variable will be not considered
      max_count = 10  

      # query = "xbox lang:en"
      query = self.create_query(hashtags_file)

      query = ' OR '.join(query.split())

      query_list = self.break_query(query, language=language)

      query_list = [re.sub(' +', ' ', t_query) for t_query in query_list] # removing duplicate spaces
            
      bearer_token = self.auth()

      for start_date, end_date in zip(start_date_list, end_date_list):
          for x in range(len(query_list)):
            
              newpath = self.results_path+str(x)+'/'

              if not os.path.exists(newpath) and save_on_disk:
                  os.makedirs(newpath)
                  with open(newpath+'hashtags.txt', 'w', encoding="utf8") as fp:
                      fp.write(''.join(query_list[x]))

              
              if 'csv' in file_extension:
                  file_extension = '.csv'
              else:
                  file_extension = '.xlsx'

              tweets_file_name = self.results_path+str(x)+'/tweets_'+start_date.replace(':','_')+'_'+end_date.replace(':','_')+'_group_'+str(x)+file_extension
              
              
              if os.path.isfile(tweets_file_name):
                  if 'csv' in tweets_file_name:
                      df_date_temp = pd.read_csv(tweets_file_name)
                  else:
                      df_date_temp = pd.read_excel(tweets_file_name, sheet_name='tweets')

                  new_start_date = df_date_temp['created_at'].iloc[-1]

                  if len(new_start_date) > 19:
                      new_start_date = new_start_date[:19]
                  
                  if len(start_date) > 19:
                      start_date = start_date[:19]
                    
                  if datetime.strptime(new_start_date, date_format) > datetime.strptime(start_date, date_format):
                      start_date = new_start_date
              
              self.get_historical_tweets_from_hashtags(query_list[x], bearer_token, start_date, end_date, 
                                                       number_of_tweets_per_call, max_count, 
                                                       limit_tweets_per_period, tweets_file_name, save_on_disk,
                                                       time_interval_break=time_interval_break)
              
    def upload_to_cloud_storage(self, bucket_name):
        # bucket_name = 'guto_test_bucket'

        # path_to_private_key_file = path+'config_files/stable-plasma-690-686708fe9d44.json'

        # gcs_manager = GoogleCloudStorageManager(path_to_private_key_file)

        # self.gcs_manager.get_all_buckets_names()
        self.gcs_manager.download_data(bucket_name)
        self.gcs_manager.upload_file(self.results_path, bucket_name)