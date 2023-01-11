from tweets_downloader import TweetsDownloader
import sys
import json
# import pathlib

# path = str(pathlib.Path(__file__).parent.resolve())+'/' 

if len(sys.argv) <= 1:
    print('Please, run: pythton main.py config_file_name.json')
    exit(1)


config = json.load(open(sys.argv[1]))

tweets_downloader = TweetsDownloader(keys_filename=config['keys_filename'], key_set=config['key_set'],
                                     path_to_google_cloud_private_key_file=config['path_to_google_cloud_private_key_file'],
                                     results_path=config['results_path'],
                                     academic_privilege=config['academic_privilege'])

if len(config['start_list']) != len(config['end_list']):
    print('start_list and end_list must have the same size!')
    exit(1)

# tweets_downloader.download_tweets(hashtags_file=config['query_file'], 
#                                   start_date_list=config['start_list'], 
#                                   end_date_list=config['end_list'], 
#                                   language=config['language'],
#                                   file_extension=config['file_extension'],
#                                   separator=config['separator'],
#                                   number_of_tweets_per_call=config['number_of_tweets_per_call'],
#                                   time_interval_break=config["time_interval_break"],
#                                   limit_tweets_per_period=False,
#                                   save_on_disk=True)

tweets_downloader.download_tweets_tweepy(hashtags_file=config['query_file'], 
                                  usernames_from_file=config['usernames_file'],
                                  start_date_list=config['start_list'], 
                                  end_date_list=config['end_list'], 
                                  language=config['language'],
                                  file_extension=config['file_extension'],
                                  separator=config['separator'],
                                  time_interval_break=config["time_interval_break"],
                                  limit_tweets=config['number_of_tweets_per_call'],
                                  chunck_size_to_save=5000,
                                  total_of_tweets= None,
                                  save_on_disk=True,
                                  from_users=False)