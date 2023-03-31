# Tweets Downloader in Python

Python project to download tweets using the Twitter API. You can download tweets using common API (limited to 7 days) or using API V2 (allows you to download tweets since Twitter was created). This project uses the library [twarc2](https://twarc-project.readthedocs.io/en/latest/api/library/) to simplify the Twitter access, but you don't need to worry about that =D

Firstly, you need to install all dependencies: pip3 install -r requirements.txt

Our project allows to upload the tweets data into the Google Cloud Storage. All dependencies to do this are installed in the previous command.
All configuration is done in the file **config.json**, which is located at the **setup_files** folder. You need to specify some information such as:

- **query_file**: the name of the file where you will specify which terms and hashtags will be used to download the tweets. You need to create a list of terms separated by enter. Example: "config_files/hashtags.txt". You can also define the query that you want to use to collect the tweets following [this tutorial](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#build), but without specify the dates, as this will be done in other fields of this file.
- **usernames_file**: if you want to collect tweets that were published for specific accounts, you can define a list of user accounts. The value of each user account can be either the username (including or excluding the @ character) or the user’s numeric user ID. The list of user accounts can be defined in a file (usually located at the folder **config_files**) and must be separated by enter.
- **generate_query**: it's a boolean value to define if the software will generate the query from the **query_file** or not.
- **keys_filename**: Name of the file where you need to store your twitter developer account keys. It must be a YAML file. To check how to create an application and generate the necessary keys, please see [this link](https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a). Example: "config_files/twitter_keys.yaml".
- **key_set**: The key set name specified in the keys file, You can define more than one keys set, and specify which one you want to use to download the tweets. Example: "search_tweets_v1".
- **path_to_google_cloud_private_key_file**: Filename of the keys for Google Cloud. Please, see [this link](https://www.skytowner.com/explore/guide_on_creating_a_service_account_and_private_keys_in_google_cloud_platform) to learn how to create your keys in the Google Cloud. If you don't need to use the Google Cloud Storage, set None. Example: "config_files/keys.json".
- **start_list**: List of start dates to retrieve the tweets. You can specify more than one interval. Example: ["2022-10-30 22:00:00"].
- **end_list**: List of start dates to retrieve the tweets. You can specify more than one interval. This list must have the same size of start_list. Example["2022-10-30 23:59:00"].
- **results_path**: The path where the tweets will be stored. Example: "tweets/".
- **time_interval_break**: It is a variable that you can use to break the downloads of tweets. For instance, if you will download tweets by a period of one month, you can break this collection in days. Then the script will collect the tweets for each day, and save then in different files. You can break your tweets collection in minutes, hours, and days. Example: {"days":1}.
- **language**: Language filter to download tweets. If you don't want to specify a language, set None or ''. Example: "pt".
- **exclude_retweets**: boolean value that specify if you want to collect retweets or not.
- **file_extension**: Extension of the output file where the tweets will be saved. Can be "csv" or "xlsx". Example: "csv".
- **separator**: Delimiter used to create the CSV files. Example: ','.
- **number_of_tweets_per_call**: Number of tweets that will be downloaded per API call. The maximum is 100. However, the script will download all tweets by the [paginator mechanism](https://developer.twitter.com/en/docs/twitter-api/pagination). Example: 100.
- **academic_privilege**: This field defines if you have academic privilege (true or false). Example: true.
- **save_using_twarc_chunk**: boolean that defines whether you want to use the twarc2 engine to save the tweets. It is no longer required as the script has been optimized to save tweets with a small memory footprint. This project saves all tweets from a time range (defined by the field **time_interval_break**) to a file. However, depending on the terms and hashtags you are collecting, each file can become large and consume a lot of memory. In this case, our script checks if the size of the resulting files is greater than 1GB and generates another file to continue saving the tweets. The new file has the same name as the previous one with a +1 counter at the end of the file. 

Since the config file is specified, you can run: python3 main.py config.json. The script will create a folder and save all tweets. If you create a big query (check [here](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#limits) the query limitation size according to the different access levels), the script you break your query into chunks and download the tweets separately.
