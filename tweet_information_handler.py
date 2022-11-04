#@title Data Tweets Handler & Data Models
from copy import deepcopy

class User:
  def __init__(self) -> None:
     self.id = None
     self.name = None
     self.username  = None
     self.created_at  = None
     self.description  = None
     self.location  = None
     self.location  = None
     self.pinned_tweet_id = None
     self.profile_image_url = None
     self.url = None
     self.verified = None
     
     self.public_metrics_followers_count = None
     self.public_metrics_following_count = None
     self.public_metrics_tweet_count = None
     self.public_metrics_listed_count = None
     
class Place:
  def __init__(self) -> None:
     self.full_name = None
     self.id = None
     self.contained_within = None
     self.country = None
     self.country_code = None
     self.name = None
     self.place_type = None
     
     self.geo_type = None
     self.geo_bbox = None
     self.geo_properties = None
     
class Media:
  def __init__(self) -> None:
    self.media_key = None
    self.type = None
    self.url = None
    self.duration_ms = None
    self.height = None
    self.preview_image_url = None
    self.width = None
    self.alt_text = None

    self.public_metrics_view_count = None
    
    self.variants_bit_rate = []
    self.variants_content_type = []
    self.variants_url = []
     
class Tweet:
  def __init__(self) -> None:
    self.id = None
    self.author_id = None
    self.created_at = None
    self.lang = None
    self.text = None
    self.conversation_id = None
    self.possibly_sensitive = None
    self.reply_settings = None
    self.geo_place_id = None

    self.data_attachments_media_keys = None
    self.data_entities_hashtags_start = None
    self.data_entities_hashtags_end = None
    self.data_entities_hashtags_tag = None
    self.data_entities_cashtags_start = None
    self.data_entities_cashtags_end = None
    self.data_entities_cashtags_tag = None
    self.data_entities_annotations_start = None
    self.data_entities_annotations_end = None
    self.data_entities_annotations_probability = None
    self.data_entities_annotations_type = None
    self.data_entities_annotations_normalized_text = None
    self.data_entities_urls_start = None
    self.data_entities_urls_end = None
    self.data_entities_urls_url = None
    self.data_entities_urls_expanded_url = None
    self.data_entities_urls_display_url = None
    self.data_entities_urls_media_key = None
    self.data_entities_mentions_start = None
    self.data_entities_mentions_end = None
    self.data_entities_mentions_username = None
    self.data_referenced_tweets_type = None
    self.data_referenced_tweets_id = None
    self.data_edit_history_tweet_ids = None
    self.data_public_metrics_retweet_count = None
    self.data_public_metrics_reply_count = None
    self.data_public_metrics_like_count = None
    self.data_public_metrics_quote_count = None
    self.data_edit_controls_edits_remaining = None
    self.data_edit_controls_is_edit_eligible = None
    self.data_edit_controls_editable_until = None

    self.author_name = None
    self.author_username = None
    self.author_created_at = None
    self.author_description = None
    self.author_location = None
    self.author_pinned_tweet_id = None
    self.author_profile_image_url = None
    self.author_url = None
    self.author_verified = None
    self.author_public_metrics_followers_count = None
    self.author_public_metrics_following_count = None
    self.author_public_metrics_tweet_count = None
    self.author_public_metrics_listed_count = None
    self.author_protected = None

    self.media_key = None
    self.media_type = None
    self.media_url = None
    self.media_public_metrics = None
    self.media_duration_ms = None
    self.media_height = None
    self.media_alt_text = None
    self.media_width = None
    self.media_variants = None
    self.media_preview_image_url = None

    self.place_full_name = None
    self.place_contained_within = None
    self.place_country = None
    self.place_country_code = None
    self.place_name = None
    self.place_place_type = None
    self.place_geo_type = None
    self.place_geo_bbox = None
    self.place_geo_properties = None

class TweetInformationHandler():
    def __init__(self, json_response) -> None:
       self.json_response = json_response 

    def get_entities_information_tweet(self, row, tweet_data, main_tag, basic_tag, tags_list):
        # main_tag = 'entities'

        if main_tag in tweet_data:
            if basic_tag in tweet_data[main_tag]:
                
                for element in tweet_data[main_tag][basic_tag]:
                    for tag in tags_list:
                        if tag in element:
                          if row['data_'+main_tag+'_'+basic_tag+'_'+tag] is not None:
                              row['data_'+main_tag+'_'+basic_tag+'_'+tag].append(element[tag])
                          else:
                              row['data_'+main_tag+'_'+basic_tag+'_'+tag] = [element[tag]]
        return row
    
    def get_attachments_information_tweet(self, row, tweet_data, main_tag, tags_list):
        # main_tag='attachments'
        
        if main_tag in tweet_data:
            for basic_tag in tags_list:
                if basic_tag in tweet_data[main_tag]:
                    for element in tweet_data[main_tag][basic_tag]:
                        if row['data_'+main_tag+'_'+basic_tag] is not None:
                            row['data_'+main_tag+'_'+basic_tag].append(element)
                        else:
                            row['data_'+main_tag+'_'+basic_tag] = [element]
        
        return row
    
    def get_referenced_tweets_information_tweet(self, row, tweet_data, main_tag, tags_list):

        if main_tag in tweet_data:
            for element in tweet_data[main_tag]:
                for tag in tags_list:
                    if row['data_'+main_tag+'_'+tag ] is not None:
                        row['data_'+main_tag+'_'+tag].append(element[tag])
                    else:
                      row['data_'+main_tag+'_'+tag] = [element[tag]]
        return row

    def get_edit_history_tweet_ids_information_tweet(self, row, tweet_data, main_tag):
        
        if main_tag in tweet_data:
            for element in tweet_data[main_tag]:
                if row['data_'+main_tag] is not None:
                    row['data_'+main_tag].append(element)
                else:
                    row['data_'+main_tag] = [element]
        
        return row
    
    def get_public_metrics_information_tweet(self, row, tweet_data, main_tag, tags_list):
        
        if main_tag in tweet_data:
            for tag in tags_list:
                row['data_'+main_tag+'_'+tag] = tweet_data[main_tag][tag]
        else:
            for tag in tags_list:
                row['data_'+main_tag+'_'+tag] = None

        return row

    def get_context_annotations_information_tweet(self, row, tweet_data, main_tag):
        
        if main_tag in tweet_data:
            row['data_'+main_tag] = tweet_data[main_tag]
        else:
            tweet_data = None
        
        return row

    
    def get_data_information_tweets(self):
        new_rows = []
        
        # print('==================================')
        # print(json.dumps(self.json_response['includes']['media'], indent=4))

        for tweet_response in self.json_response['data']:
            row = Tweet().__dict__            
            
            row['id'] = tweet_response['id']
            row['author_id'] = tweet_response['author_id']

            
            if len(tweet_response['created_at']) > 19:
                row['created_at'] = tweet_response['created_at'][:19]
            else:
                row['created_at'] = tweet_response['created_at']

            row['lang'] = tweet_response['lang']
            row['text'] = tweet_response['text']
            
            row['conversation_id'] = tweet_response['conversation_id']
            
            row['possibly_sensitive'] = tweet_response['possibly_sensitive']
            row['reply_settings'] = tweet_response['reply_settings']

            if 'geo' in tweet_response:   
                row['geo_place_id'] = tweet_response['geo']['place_id']
            else:
                row['geo_place_id'] = None

            # Getting information about attachments
            row = self.get_attachments_information_tweet(row=row, tweet_data=tweet_response, main_tag='attachments', tags_list=['media_keys'])

            # Getting information about entities
            row = self.get_entities_information_tweet(row=row, tweet_data=tweet_response, main_tag='entities', basic_tag='hashtags', tags_list=['start','end','tag'])
            row = self.get_entities_information_tweet(row=row, tweet_data=tweet_response, main_tag='entities', basic_tag='cashtags', tags_list=['start','end','tag'])
            row = self.get_entities_information_tweet(row=row, tweet_data=tweet_response, main_tag='entities', basic_tag='annotations', tags_list=['start','end','probability', 'type', 'normalized_text'])
            row = self.get_entities_information_tweet(row=row, tweet_data=tweet_response, main_tag='entities', basic_tag='urls', tags_list=['start','end','url', 'expanded_url', 'display_url', 'media_key'])
            row = self.get_entities_information_tweet(row=row, tweet_data=tweet_response, main_tag='entities', basic_tag='mentions', tags_list=['start','end','username'])

            # Getting information about referenced_tweets
            row = self.get_referenced_tweets_information_tweet(row=row, tweet_data=tweet_response, main_tag='referenced_tweets', tags_list=['type', 'id'])
            
            # Getting information about edit_history_tweet_ids
            row = self.get_edit_history_tweet_ids_information_tweet(row=row, tweet_data=tweet_response, main_tag='edit_history_tweet_ids')

            # Getting information about public_metrics
            row = self.get_public_metrics_information_tweet(row=row, tweet_data=tweet_response, main_tag='public_metrics', tags_list=['retweet_count', 'reply_count', 'like_count', 'quote_count'])
            
            # Getting information about context_annotations
            # row = self.get_context_annotations_information_tweet(row=row, tweet_data=tweet, main_tag='context_annotations')

            # Getting information about edit_controls (is the same format of public metrics)
            row = self.get_public_metrics_information_tweet(row=row, tweet_data=tweet_response, main_tag='edit_controls', tags_list=['edits_remaining', 'is_edit_eligible', 'editable_until'])
            
            new_rows.append(row)
        
        return new_rows
    
    def get_media_information_tweet(self):
        media_list = []
            
        media_keys = ['media_key', 'type', 'url', 'public_metrics', 'duration_ms', 'height', 'alt_text', 'width', 'variants', 'preview_image_url']

        if 'media' in self.json_response['includes']:
          for media in self.json_response['includes']['media']:

            media_temp = Media().__dict__ # creating an 'empty' media dict

            for key in media_keys:
              if key == 'public_metrics':
                if key in media:
                  media_temp['public_metrics_view_count'] = media['public_metrics']['view_count']

              elif key == 'variants':
                if key in media:
                  for variant in media['variants']:
                    if 'bit_rate' in variant:
                      media_temp['variants_bit_rate'].append(variant['bit_rate'])
                    else:
                      media_temp['variants_bit_rate'].append(None)
                    
                    if 'content_type' in variant:
                      media_temp['variants_content_type'].append(variant['content_type'])
                    else:
                      media_temp['variants_content_type'].append(None)
                    
                    if 'url' in variant:
                      media_temp['variants_url'].append(variant['url'])
                    else:
                      media_temp['variants_url'].append(None)

              else:
                if key in media:
                  media_temp[key] = media[key]

            media_list.append(media_temp)

        return media_list

    def get_places_information_tweet(self):
      places_list = []
            
      places_keys = ['full_name', 'id', 'contained_within', 'country', 'country_code', 'name', 'place_type', 'geo']

      if 'places' in self.json_response['includes']:

        for place in self.json_response['includes']['places']:
          place_temp = Place().__dict__

          for key in places_keys:
            if key == 'geo':
              if 'type' in place['geo']:
                place_temp['geo_type'] = place['geo'][ 'type']
            
            if key == 'geo':
              if 'bbox' in place['geo']:
                place_temp['geo_bbox'] = place['geo'][ 'bbox']

            if key == 'geo':
              if 'properties' in place['geo']:
                place_temp['geo_properties'] = place['geo'][ 'properties']

            else:
              if key in place:
                place_temp[key] = place[key]

          places_list.append(place_temp)
        
      return places_list

    def get_users_information_tweet(self):
        users_keys = ['description', 'public_metrics', 'created_at', 'pinned_tweet_id', 'profile_image_url', 'url', 'verified', 'username', 'location', 'name', 'id', 'protected'] # 'entities'
        users = []

        if 'users' in self.json_response['includes']:
            
            for user in self.json_response['includes']['users']:
                # print(user)
                # print('-----------------------------')
                user_temp = User().__dict__

                for u_key in users_keys:
                    if u_key in user:
                        if u_key == 'public_metrics':
                          for u_key_2 in user['public_metrics']:
                                user_temp['public_metrics_'+u_key_2] = user['public_metrics'][u_key_2]
                        elif u_key == 'created_at' and len(user[u_key]) > 19:
                          user_temp[u_key] = user[u_key][:19]                          
                        else:
                            # print(u_key, user[u_key])
                            user_temp[u_key] = user[u_key]
                    
                users.append(deepcopy(user_temp))
        
        return users
      
    def get_full_information_from_tweets(self):
      rows = self.get_data_information_tweets()

      users = self.get_users_information_tweet()
      medias = self.get_media_information_tweet()
      places = self.get_places_information_tweet()
      
      for row in rows:

        if row['author_id'] is not None:
          user = next((user for user in users if user['id'] == row['author_id']), None)
        
          for key in user:
            row['author_'+key] = user[key]
        
        if row['data_attachments_media_keys'] is not None:
          media = next((media for media in medias if media['media_key'] == row['data_attachments_media_keys'][0]), None)

          row.pop('data_attachments_media_keys')

          for key in media:
            if key == 'media_key':
              row[key] = media[key]
            else:  
              row['media_'+key] = media[key]            
        
        if row['geo_place_id'] is not None:
          place = next((place for place in places if place['id'] == row['geo_place_id']), None)
          
          for key in place:
            if key == 'id':
              continue
            row['place_'+key] = place[key]

          # print(json.dumps(place, indent=4))
          # print(json.dumps(row, indent=4))
    
      return rows
