import sys
sys.path.append("..")

from tweets_downloader.tweets_downloader import TweetsDownloader
import sys
import json

if len(sys.argv) <= 1:
    print('Please, run: pythton main.py config_file_name.json')
    exit(1)

config = json.load(open(sys.argv[1]))

tweets_downloader = TweetsDownloader(keys_filename=config['keys_file'], key_set=config['key_set'],
                                     path_to_google_cloud_private_key_file=config['path_to_google_cloud_private_key_file'],
                                     results_path=config['results_path'],
                                     academic_privilege=config['academic_privilege'])


start_date = '2024-02-22T00:00:00.000Z'
end_date = '2024-02-22T21:00:00.000Z'
tweets_file_name = 'results/'


conversation_id_list = ['1760650930700079361']

for conversation_id in conversation_id_list:

    tweets_downloader.download_replies_tweepy(conversation_id=conversation_id, 
                                            tweets_file_name=tweets_file_name+'main_tweet_'+conversation_id+'.csv', 
                                            start_date=start_date, end_date=end_date)

    tweets_with_replies = tweets_downloader.expand_replies(replies_filename=tweets_file_name+'main_tweet_'+conversation_id+'.csv', 
                                                            start_date=start_date,end_date=end_date)
    
    tweets_with_replies.to_csv(tweets_file_name+'second_replies_'+conversation_id+'.csv', sep=config['separator'])
    
    # https://dev.to/suhemparack/understanding-the-entire-conversation-around-a-tweet-with-the-twitter-api-v2-3ce6
    tweets_with_replies = tweets_downloader.download_quotes_tweepy(tweet_id=conversation_id, separator=config['separator'],
                                                                   tweets_file_name=tweets_file_name+'quotes_'+conversation_id+'.csv',
                                                                   exclude_rule='retweets', start_date=start_date,end_date=end_date,
                                                                   chunck_size_to_save=500, save_on_disk=True)
