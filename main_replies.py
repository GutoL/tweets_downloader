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


## COVID-19 Announcement
conversation_id_list = [
    '1222967082733559808', # - 1,257 replies
    '1222967978293178369', # - 108 replies
    '1222968023876763649', # - 180 replies
    '1222968107020488706', # - 38 replies
    '1222968207524450308', # - 94 replies
    '1222968349719760896', # - 13 replies 
    '1222968472444993542', # - 19 replies
    '1222968567303409664', # - 24 replies
    '1222968618515865600', # - 11 replies
    '1222968678595076096', # - 19 replies
    '1222968733829865477', # (the one we're examining) - 346 replies
    '1222968794269745153', # - 31 replies
    '1222968882555768834', # - 18 replies
    '1222968973291114498', # - 58 replies
    '1222969091482361856', # - 121 replies
    '1222969326707380230', # - 18 replies
    '1222969409175785472', # - 6 replies
    '1222969511801950210', # - 23 replies
    '1222969618505093121', # - 63 replies
    '1222969858574430217', # - 196 replies
    '1222969949175656448', # - 51 replies
] 
start_date = '2020-01-29T00:00:00.000Z'
end_date = '2022-12-22T12:00:00.000Z'
tweets_file_name = 'results/covid_status/'


## Monkeypox rename
# conversation_id_list = [
#     '1597253819804844034', #(the one we're examining) - 308 replies
#     '1597256946633281537' #- 68 replies
# ]
# start_date = '2022-11-27T00:00:00.000Z'
# end_date = '2022-12-22T12:00:00.000Z'
# tweets_file_name = 'results/mpox_rename/'

# Monkeypox Annoucement
# conversation_id_list = [
#     '1550844182473805824', #- 330 replies
#     '1550845488148144128', #- 20 replies
#     '1550845693820026883', #- 20 replies
#     '1550845759687385088', #- 9  replies
#     '1550845936909385728', #- 4 replies
#     '1550846084196536320', #- 25 replies
#     '1550846162034462720', #- 14 replies
#     '1550846761006235648', #- 6 replies
#     '1550846763338186752', #- 5 replies
#     '1550846765397573635', #- 9 replies
#     '1550846767213805569', #- 4 replies
#     '1550846769143185408', #- 5 replies
#     '1550846873577181185', #- 11 replies
#     '1550847007765454848', #- 8 replies
#     '1550847099293540353', #- 39 replies
#     '1550847224963371010', #(the one we're examining) - 1,991 replies
#     '1550847560948105216', #- 30 replies
#     '1550847562978131968', #- 21 replies
#     '1550848120757665793', #- 11 replies
#     '1550848123127439361', #- 20 replies
#     '1550848379684589569', #- 12 replies
#     '1550848382612213760', #- 12 replies
#     '1550848384801579013', #- 6 replies
#     '1550848705447792641', #- 10 replies
#     '1550848707737931777', #- 12 replies
#     '1550848811941220354', #- 374 replies
#     '1550849017453678594', #- 22 replies
#     '1550849165927845888', #- 68 replies
#     '1550849439182495744', #- 114 replies
#     '1550849619122421760', #- 25 replies
#     '1550849620779081730', #- 54 replies
#     '1550853208330964992', #- 110 replies
#     '1550854012634898433' #- 185 replies
# ]
# start_date = '2022-07-22T00:00:00.000Z'
# end_date = '2022-12-22T12:00:00.000Z'
# tweets_file_name = 'results/announcing_monkeypox_status/'

for conversation_id in conversation_id_list:

    tweets_downloader.download_replies_tweepy(conversation_id=conversation_id, 
                                            tweets_file_name=tweets_file_name+'main_tweet_'+conversation_id+'.csv', 
                                            start_date=start_date, end_date=end_date)

    tweets_with_replies = tweets_downloader.expand_replies(replies_filename=tweets_file_name+'main_tweet_'+conversation_id+'.csv', 
                                                            start_date=start_date,end_date=end_date)

    print(tweets_with_replies)
    print(tweets_with_replies.shape)

    tweets_with_replies.to_csv(tweets_file_name+'second_replies_'+conversation_id+'.csv', sep='|')