from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
from requests_aws4auth import AWS4Auth
import tweepy
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
aws_id =''
aws_key= ''

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

host = ''

auth = AWSRequestsAuth(aws_access_key=aws_id,
                       aws_secret_access_key=aws_key,
                       aws_host=host,
                       aws_region='us-east-2',
                       aws_service='es')

client = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    use_ssl=True,
    http_auth=auth,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
class StdOutListener(tweepy.StreamListener):

    def on_data(self, data):
        data_json = json.loads(data)
        try:
            coordinates = data_json['place']['bounding_box']['coordinates']
            tweet = json.dumps(data_json['text'])
            place = json.dumps(data_json['place'])

            if place is not None:
                if coordinates[0] is not None and len(coordinates[0]) > 0:
                    lat_x = 0
                    long_y = 0
                    for c in coordinates[0]:
                        lat_x = (lat_x + c[0])
                        long_y = (long_y + c[1])
                    lat_x /= len(coordinates[0])
                    long_y /= len(coordinates[0])
                    coordinates = [lat_x, long_y]
                data_string = str(coordinates) + ","+ str(tweet.encode('utf-8')) + "\n"
                #print data_string

                with open('fetched_tweets', 'a') as f:
                    f.write(data_string)

                try:
                    client.index(index='cloud_tweet', doc_type='twitter', body={
                    'content': tweet,
                    'coordinates': coordinates
                    })
                except:
                    print('ElasticSearch indexing failed')

        except (KeyError, TypeError):
            pass
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, StdOutListener())
    stream.filter(track=['trump','giveaway','messi','ronaldo','kangana','thursday','josh brown','bollywood','samsung','apple',
                         'location','amazon','dollar','lenovo','hugh','pizza','snapchat','money','hrithik','vodka',
                         'election','https','lol','instagram','twitter','fb','facebook','nba','birthday',
                         'technology', 'hillary', 'food', 'travel','vote','nintendo', 'fashion', 'soccer',
                         'sports','modi','debate','america','india','obama','song','punjab','new york','bernie',
                         'news','logan','usa','london','health','Dangal','spain','music','travel','skyline','bigboss',
                         'foodie','sharktank','neverkock','puerto','october','rodgers','harvey','menwillbemen','mondaymotivation'])
