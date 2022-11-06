from tweets_downloader import TweetsDownloader
import sys
import json
# import pathlib

# path = str(pathlib.Path(__file__).parent.resolve())+'/' 

if len(sys.argv) <= 1:
    print('Please, run: pythton 3 main.py config_file_name.json')
    exit(1)


config = json.load(open(sys.argv[1]))

tweets_downloader = TweetsDownloader(keys_filename=config['keys_filename'], key_set=config['key_set'],
                                     path_to_google_cloud_private_key_file=config['path_to_google_cloud_private_key_file'],
                                     results_path=config['results_path'],
                                     academic_privilege=config['academic_privilege'])

# start_list =   ['2022-10-30 22:00:00'] # ['2022-10-18T22:00:00.000Z',]
# end_list =     ['2022-10-30 23:59:00'] # ['2022-10-18T23:59:00.000Z',]



# json_response = json.load(open(path+'json_response_example.json'))

# # tweets_downloader.get_users_information_tweet(json_response)
# # tweets_downloader.get_media_information_tweet(json_response)
# # tweets_downloader.get_places_information_tweet(json_response)

# tweet_information_handler = TweetInformationHandler(json_response)

# tweets = tweet_information_handler.get_full_information_from_tweets()

# print(json.dumps(tweets, indent=4))

# columns_names  = tweets[0].keys()
# df = pd.DataFrame(columns=columns_names)

# for tweet in tweets:
#   df = df.append(tweet, ignore_index=True)

# df.to_csv('tweets.csv', index=False)

if len(config['start_list']) != len(config['end_list']):
    print('start_list and end_list must have the same size!')
    exit(1)

tweets_downloader.download_tweets(hashtags_file=config['query_file'], 
                                  start_date_list=config['start_list'], 
                                  end_date_list=config['end_list'], 
                                  language=config['language'],
                                  file_extension=config['file_extension'],
                                  number_of_tweets_per_call=config['number_of_tweets_per_call'],
                                  time_interval_break=config["time_interval_break"],
                                  limit_tweets_per_period=False,
                                  save_on_disk=True)