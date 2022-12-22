from tweets_downloader import TweetsDownloader
import sys
import json

if len(sys.argv) <= 1:
    print('Please, run: pythton main.py config_file_name.json')
    exit(1)

config = json.load(open(sys.argv[1]))

tweets_downloader = TweetsDownloader(keys_filename=config['keys_filename'], key_set=config['key_set'],
                                     path_to_google_cloud_private_key_file=config['path_to_google_cloud_private_key_file'],
                                     results_path=config['results_path'],
                                     academic_privilege=config['academic_privilege'])

conversation_id = '1222968733829865477' # Covid -> IT WORKS
start_date = '2020-01-29T00:00:00.000Z'
end_date = '2022-12-22T12:00:00.000Z'
tweets_file_name = 'results/covid_status.csv'

conversation_id = '1597253819804844034' # mpox rename
start_date = '2022-11-27T00:00:00.000Z'
end_date = '2022-12-22T12:00:00.000Z'
tweets_file_name = 'results/mpox_rename.csv'

conversation_id = '1550847224963371010' # announcing Monkeypox status
start_date = '2022-07-22T00:00:00.000Z'
end_date = '2022-12-22T12:00:00.000Z'
tweets_file_name = 'results/announcing_monkeypox_status.csv'

# tweets_downloader.download_replies_tweepy(conversation_id=conversation_id, tweets_file_name=tweets_file_name, start_date=start_date, end_date=end_date)

tweets_with_replies = tweets_downloader.expand_replies(tweets_file_name, start_date, end_date)

print(tweets_with_replies)
print(tweets_with_replies.shape)