# Tweets Downloader in Python

Python project to download tweets using the Twitter API. You can download tweets using common API (limited to 7 days) or using API V2 (allows you to download tweets since Twitter was created). This project was based on in the wonderfull tutorial presented [here](https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a).

Firstly, you need to install all dependencies: pip3 install -r requirements.txt

Our project allows to upload the tweets data into the Google Cloud Storage. All dependencies to do this are installed in the previous command.
All configuration is done in the file **config.json**. You need to specify some information such as:

- **query_file**: the name of the file where you will specify which terms and hashtags will be used to download the tweets. You need to create a list of terms separated by enter or space. Example: "config_files/hashtags.txt".
- **keys_filename**: Name of the file where you need to store your twitter developer account keys. It must be a YAML file. To check how to create an application and generate the necessary keys, please see [this link](https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a). Example: "config_files/twitter_keys.yaml".
- **key_set**: The key set name specified in the keys file, You can define more than one keys set, and specify which one you want to use to download the tweets. Example: "search_tweets_v1".
- **path_to_google_cloud_private_key_file**: Filename of the keys for Google Cloud. Please, see [this link](https://www.skytowner.com/explore/guide_on_creating_a_service_account_and_private_keys_in_google_cloud_platform) to learn how to create your keys in the Google Cloud. If you don't need to use the Google Cloud Storage, set None. Example: "config_files/keys.json".
- **start_list**: List of start dates to retrieve the tweets. You can specify more than one interval. Example: ["2022-10-30 22:00:00"].
- **end_list**: List of start dates to retrieve the tweets. You can specify more than one interval. This list must have the same size of start_list. Example["2022-10-30 23:59:00"].
- **results_path**: The path where the tweets will be stored. Example: "tweets/".
- **time_interval_break**: It is a variable that you can use to break the downloads of tweets. For instance, if you will download tweets by a period of one month, you can break this collection in days. Then the script will collect the tweets for each day, and save then in different files. You can break your tweets collection in minutes, hours, and days. Example: {"days":1}.
- **language**: Language filter to download tweets. If you don't want to specify a language, set None or ''. Example: "pt".
- **file_extension**: Extension of the output file where the tweets will be saved. Can be "csv" or "xlsx". Example: "csv".
- **separator**: Delimiter used to create the CSV files. Example: ','.
- **number_of_tweets_per_call**: Number of tweets that will be downloaded per API call. The maximum is 100. However, the script will download all tweets by the [paginator mechanism](https://developer.twitter.com/en/docs/twitter-api/pagination). Example: 100.
- **academic_privilege**: This field defines if you have academic privilege (true or false). Example: true.

Since the config file is specified, you can run: python3 main.py config.json. The script will create a folder and save all tweets. If you create a big query (check [here](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#limits) the query limitation size according to the different access levels), the script you break your query into chunks and download the tweets separately.
