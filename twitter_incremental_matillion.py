import tweepy
import pandas as pd
import time
import json
from google.cloud import storage, bigquery
import mimetypes
from datetime import datetime, timedelta
import requests
import itertools


bigquery_client = bigquery.Client()
storage_client = storage.Client()

#FOR LOCAL TESTING

consumer_key = twitter_api_key
consumer_secret = api_key_secret
access_token = access_token
access_token_secret = access_token_secret


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
#---------

#Function to return all current teams and associated twitter ID's 
def get_teams():
	team_arr = [] 
	twitter_id_query = (
    """ SELECT team_name, twitter_user_id, team_key  FROM `trajektory-dev.dimensions.team_dim` a 
    WHERE twitter_handle is not null """
    )
	results = bigquery_client.query(twitter_id_query)
	#only collect teams that have a twitter ID
	for result in results:
		if result['twitter_user_id'] is not None:
			team_arr.append({
				"team_name": result['team_name'],
				"twitter_user_id": result['twitter_user_id'],
				"team_key":result['team_key']
				})
	
	return team_arr

def main():
	#tweets = tweepy.Cursor(api.user_timeline,id=username, tweet_mode ="extended").items(count)
	get_recent_tweets()

def get_recent_tweets():
	
	team_arr = get_teams()
	#team_name
	#FOR TESTING
	#team_arr=[]
	#team_arr.append({
	#"team_name": "Baltimore Orioles",
	#"twitter_user_id": "39389304"
	#})
	
	
	#Lookback date decides for how long to track engagement of a tweet
	lookback_date = datetime.utcnow() - timedelta(days= 11)
	lookback_date = str(lookback_date.date())


	get_tweets_query = (""" Select tweet_id, af.team_key 
		From `trajektory-dev.facts.twitter_accumulating_fact` af INNER JOIN(
		SELECT min(created_at) as created_at, team_key
		FROM `trajektory-dev.facts.twitter_accumulating_fact`
		WHERE created_at > @lookback_date
		GROUP BY team_key
		) t ON af.team_key=t.team_key and af.created_at = t.created_at 
		""")

	job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("lookback_date", "STRING", lookback_date),
    ])
    
	query_job = bigquery_client.query(get_tweets_query, job_config=job_config)
	recent_tweet_results = query_job.result()
	for tweet in recent_tweet_results:
		for team in team_arr:
			if tweet['team_key'] == team['team_key']:
				team['recent_tweet_id'] = tweet['tweet_id']

	get_tweet_data(team_arr)


def get_tweet_data(team_arr): 

	for team in team_arr:
		print(team)
		latest_tweet_id = team['recent_tweet_id']
		twitter_user_id = team['twitter_user_id']
		tweets = tweepy.Cursor(api.user_timeline,id=twitter_user_id, 
			tweet_mode ="extended", 
			wait_on_rate_limit=True, 
			since_id = latest_tweet_id).items()
		tweets1, tweets2 = itertools.tee(tweets, 2)
		#Gets existing TWEETS found in DB

		#Determine if the tweet is a new tweet in our database
		new_tweets = get_new_tweet_ids(tweets1, team)
		for tweet in tweets2:
			tweet_object ={
				"created_at": getattr(tweet, 'created_at', ''),
				"tweet_id": getattr(tweet, 'id', ''),
				"full_text": getattr(tweet, 'full_text', ''),
				"retweet_count": getattr(tweet, 'retweet_count', ''),
				"favorite_count": getattr(tweet, 'favorite_count', ''),
				"source": getattr(tweet, 'source', ''),
				"language": getattr(tweet, 'lang', ''),
				"in_reply_to_status_id": getattr(tweet, 'in_reply_to_status_id', ''),
				"in_reply_to_user_id": getattr(tweet, 'in_reply_to_user_id', ''),
				"in_reply_to_screen_name": getattr(tweet, 'in_reply_to_screen_name', ''),
				"team_name": team['team_name']
			}
			# if tweet_object['tweet_id'] in new_tweets:
			# 	print("New TWEET")
			# 	print(tweet_object['tweet_id'])
			# else:
			# 	print('Existing TWEET')
			# 	print(tweet_object['tweet_id'])

			created_time_UTC = str(tweet_object['created_at']) + ' UTC'
			tweet_object['created_at'] = created_time_UTC
			tweet_entities = tweet.entities
			#Only collecting a max of 5 hashtags per tweet
			for val, items in enumerate(tweet_entities['hashtags']):
				if val <6:
					val = val+1
					hashtag_name = 'hashtag'+str(val)
					tweet_object[hashtag_name] = items['text']
				else:
					pass

			if tweet_object['full_text'].startswith('RT @'):
				tweet_object['retweeted'] = True
			else:
				tweet_object['retweeted'] = False

			try: 
				coords = tweet["coordinates"]
				tweet_object['coordinates'] = coords['coordinates']
			except:
				pass

			media_elements = []
			try:
				extended_entities = tweet.extended_entities
				media_elements = extended_entities['media']
			except: 
				pass

			if len(media_elements) > 0:
				for val, media_element in enumerate(media_elements):
					#print(str(val))
					val = val+1
					media_url_name = 'media_url'+str(val)
					media_url_https_name = 'media_url_https'+str(val)
					media_type_name = 'media_type'+str(val)
					media_id_name = 'media_id'+str(val)
					tweet_object[media_url_name] = media_element['media_url']
					tweet_object[media_url_https_name] = media_element['media_url_https']
					tweet_object[media_type_name] = media_element['type']
					tweet_object[media_id_name] = media_element['id']
					url_to_storage = media_element['media_url']
					gcs_upload_type = media_element['type']
					#type_to_storage = media_element['type']
					if media_element['type'] == 'video':
						#There can only be one video per tweet
						video_variants = media_element['video_info']['variants']
						max_url_element = find_max(video_variants)				
						tweet_object['video_url'] = media_element['media_url']
						tweet_object['video_duration_millis'] = media_element['video_info']['duration_millis']
						url_to_storage = max_url_element['url']
						gcs_upload_type = max_url_element['content_type']


					blob_name = team['team_name'] +'/'+ media_element['id_str']
					if tweet_object['tweet_id'] in new_tweets:
						tweet_object['media_upload_status']=upload_media_GCS(url_to_storage, blob_name, gcs_upload_type)
					else:
						pass
						
					
			else:
				#print('either or')
				tweet_object['media_type1'] = 'text'
			#tweets_list.append(tweet_object)
			upload_data_bigquery(tweet_object, team)
	
#Function to determine if the tweet ID is a new tweet in our DB
def get_new_tweet_ids(tweet_arr, team):
	print(team)
	team_name = team['team_name']
	tweet_ids_arr=[]
	for tweet in tweet_arr:
		tweet_ids_arr.append(getattr(tweet, 'id', ''))
	#print(tweet_ids_arr)

	twitter_id_query = (
    """ SELECT *  
    FROM UNNEST(@ids) as v
    where not exists (select tweet_id
    from `trajektory-dev.facts.twitter_accumulating_fact` a
                  where a.tweet_id = v);
    """
    )
	job_config = bigquery.QueryJobConfig(
		query_parameters=[
		bigquery.ArrayQueryParameter('ids', 'INT64', tweet_ids_arr)
    ])
	results = bigquery_client.query(twitter_id_query, job_config=job_config)
	#print(results)
	#print(len(tweet_ids_arr))
	new_tweets = []
	for result in results:
		new_tweets.append((result['v']))
	#print("New Tweets")
	#print(len(new_tweets))
	return new_tweets



def upload_media_GCS(image_url, destination_blob_name, media_type):
	bucket = storage_client.get_bucket('social_dev')
	blob = bucket.blob(destination_blob_name)
	blob.chunk_size = 5 * 1024 * 1024 #SET Chunk Size to 5MB
	#image = urlopen(image_url)
	media = requests.get(image_url, stream = True)
	media_response = media.raw.read()

	#use below code if you need exact file type added to 'content_type' for GCS upload
	#response = requests.get(image_url)
	#content_type = response.headers['content-type']
	#extension = mimetypes.guess_extension(content_type)
	#print(extension)

	#blob.upload_from_string(image.read(), content_type = media_type)
	blob.upload_from_string(media_response, content_type = media_type)

def upload_data_bigquery(tweet_object, team):
	#team = team['team_name']
	table_id = "trajektory-dev.landing.twitter_landing"
	rows_to_insert=[tweet_object]
	current_time = str(datetime.utcnow()) +' UTC'
	tweet_object['upload_time'] = current_time
	errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
	if errors == []:
		test = 0
	else:
		print("Encountered errors while inserting rows: {}".format(errors))	
	

def find_max(video_variants):
	max_bitrate= 0
	max_elment = video_variants[0]
	for video_variant in video_variants:
		#print(video_variant)
		if 'bitrate' in video_variant.keys() and video_variant['bitrate'] > max_bitrate:
			maximum_elment = video_variant
		else:
			pass
	return maximum_elment




if __name__ == '__main__':
    main()

