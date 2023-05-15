

import streamlit as st
import pymongo

import googleapiclient.discovery
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi









st.title('Youtube Data Scraping app:chart_with_upwards_trend:')

scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]

@st.cache_data
def youtube_authenticate():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    #os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    #client_secrets_file = r"C:\Users\SVR\Python vs code\Guvi_Projects\credentials.json"
    dev_key = "AIzaSyAJSNdqINYUD9nzb39D4MUPYrWw-s6rb9c"
    return googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = dev_key)

@st.cache_data
def get_channel_details(_youtube, **kwargs):
    return youtube.channels().list(
        part="statistics,snippet,contentDetails",
        **kwargs
    ).execute()

@st.cache_data
def get_playlist_details(_youtube, **kwargs):
    return youtube.playlists().list(
        part="snippet,contentDetails",
        **kwargs,
        maxResults=3
        ).execute()

@st.cache_data
def get_playlistitems_details(_youtube, pl_id):

    return youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=3,
        playlistId=pl_id
        ).execute() 

@st.cache_data
def get_video_details(_youtube, **kwargs):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        maxResults=3,
        **kwargs
        ).execute()

@st.cache_data
def get_comment_details(_youtube, video_id):
    return youtube.commentThreads().list(
        part="snippet,replies",
        maxResults=3,
        videoId=video_id
        ).execute()    

def channel_details_to_mongo_db(data):
    
    ch_details = {
        "Channel_Details": {
            "Channel_Name": data['items'][0]['snippet']['title'],
            "Channel_Id": data['items'][0]['id'],
            "Subscription_Count": data['items'][0]['statistics']['subscriberCount'],
            "Channel_Views": data['items'][0]['statistics']['viewCount'],
            "Channel_Description": data['items'][0]['snippet']['description'],
            "Number_of_Videos": data['items'][0]['statistics']['videoCount']
        }
    }
    #st.write(channel_details_data)
    channel_db.insert_one(ch_details)

def playlist_details_to_mongo_db(pl_of_each_id):
    
    for i in pl_of_each_id['items']:
    
        pl_details = {
                'playlist_details':{
                    'playlist_id' :i['id'],
                    'channelId' :i['snippet']['channelId'],
                    'playlist_title' :i['snippet']['title'],
                    'playlist_description' :i['snippet']['description'],
                    'Playlist_video_count' :i['contentDetails']['itemCount']
                                    }
                                }
        playlist_db.insert_one(pl_details)

def playlistitem_details_to_mongo_db(pl_item_of_each_pl_id):
    for i in pl_item_of_each_pl_id['items']:
        #st.write(i['snippet']['thumbnails']['default']['url'])
        pl_items_details = {
                        'playlistitem_details':{ 'channelId':i['snippet']['channelId'],
                        'channelTitle':i['snippet']['channelTitle'],
                        'playlist_id':i['snippet']['playlistId'],
                        'Video_id':i['contentDetails']['videoId']
                                                }
                            }
        playlistitems_db.insert_one(pl_items_details)

def video_details_to_mongo_db(vid_list):
    for each_item in vid_list:
        #st.write(i['id'])
        #st.write(each_item)
        vid_details = {'video_details':{
                    'Video_id' :each_item['id'],
                    'video_publishedAt':each_item['snippet']['publishedAt'],
                    'channelId':each_item['snippet']['channelId'],
                    'video_title':each_item['snippet']['title'],
                    'description':each_item['snippet']['description'],
                    'thumbnail_url':each_item['snippet']['thumbnails']['default']['url'],
                    'channelTitle':each_item['snippet']['channelTitle'],
                    # 'tags':i['snippet']['tags'],
                    'duration':each_item['contentDetails']['duration'],
                    'viewCount':each_item['statistics']['viewCount'],
                    'likeCount':each_item['statistics']['likeCount'],
                    'favoriteCount':each_item['statistics']['favoriteCount'],
                    
                    'commentCount':   
                        -1 if (each_item['statistics']['commentCount']==KeyError)  else each_item['statistics']['commentCount']
                                        
                    
                 }

                  }
        # break
        video_db.insert_one(vid_details)
def comment_details_to_mongo_db(comments_list):
    for i in comments_list:
        #st.write(i['snippet']['topLevelComment']['id'])
        #st.write(i)
        comment_det = {'Comment_details':{
            'comment_id':i['snippet']['topLevelComment']['id'],
            'video_id':i['snippet']['topLevelComment']['snippet']['videoId'],
            'textDisplay':i['snippet']['topLevelComment']['snippet']['textDisplay'],
            'authorDisplayName':i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'publishedAt': i['snippet']['topLevelComment']['snippet']['publishedAt']
                                            }
                        }
        
        comment_db.insert_one(comment_det)
  
if __name__ == "__main__":
    youtube = youtube_authenticate()
    #st.write(youtube)
    uri = "mongodb+srv://svrdb:svrnoobs@ytdatabysvr.0dp48ba.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        st.write("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        st.write(e)

    yt_dbs = client['yt_dbs']

    channel_db = yt_dbs['channels']
    playlist_db = yt_dbs['playlists']
    playlistitems_db = yt_dbs['playlistitems']
    video_db = yt_dbs['videodetails']
    comment_db = yt_dbs['comments']

    channel_db.delete_many({})
    playlist_db.delete_many({})
    playlistitems_db.delete_many({})
    video_db.delete_many({})
    comment_db.delete_many({})

    user_input_channel_ids=[]
    channels = {}
    playlist={}
    playlist_ids={}
    playlistitems={}
    video_details={}
    comment_details={}
    

    number = st.sidebar.number_input(':red[Enter the number of channels you wish to extract]',value=1,min_value=1,max_value=10)        
