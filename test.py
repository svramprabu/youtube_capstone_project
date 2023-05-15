

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
