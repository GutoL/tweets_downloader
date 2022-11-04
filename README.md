# Tweets Downloader in Python

Python project to download tweets using the Twitter API. You can download tweets using common API (limited to 7 days) or using API V2 (allows you to download tweets since Twitter was created).

Firstly, you need to install all dependencies: pip3 install -r requirements.txt

Our project allows to upload the tweets data into the Google Cloud Storage. All dependencies to do this are installed in the previous command.
All configuration is done in the file **config.json**. You need to specify some information such as:

* query_file: the name of the file where you will specify which terms and hashtags will be used to download the tweets. You need to create a list of terms separated by enter or space. Example: "config_files/hashtags.txt"
* keys_filename: Name of the file where you need to store your twitter developer account keys. "config_files/twitter_keys.yaml",
* key_set: "search_tweets_v1",
* path_to_private_key_file":"config_files/keys.json",
* start_list":["2022-10-30 22:00:00"],
* end_list":["2022-10-30 23:59:00"],
* results_path": "results/",
* language": "pt",
* file_extension": "csv",
* number_of_tweets_per_call": 11,
* save_ond_disk": true
