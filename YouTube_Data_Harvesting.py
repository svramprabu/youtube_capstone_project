import streamlit as st
import googleapiclient.discovery
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

st.title('Youtube Data Scraping app:chart_with_upwards_trend:')

scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]

@st.cache_data
def youtube_authenticate():
    api_service_name = "youtube"
    api_version = "v3"
    return googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = st.secrets["devkey"])

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
        maxResults=10
        ).execute()

@st.cache_data
def get_playlistitems_details(_youtube, pl_id):

    return youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=10,
        playlistId=pl_id
        ).execute() 

@st.cache_data
def get_video_details(_youtube, **kwargs):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        maxResults=5,
        **kwargs
        ).execute()

@st.cache_data
def get_comment_details(_youtube, video_id):
    return youtube.commentThreads().list(
        part="snippet,replies",
        maxResults=5,
        videoId=video_id
        ).execute()    

def channel_details_to_mongo_db(data):

    ch_details = {
            "channelTitle": data['items'][0]['snippet']['title'],
            "channelId": data['items'][0]['id'],
            "subscriberCount": data['items'][0]['statistics']['subscriberCount'],
            "channel_viewCount": data['items'][0]['statistics']['viewCount'],
            "channel_description": data['items'][0]['snippet']['description'],
            "videoCount": data['items'][0]['statistics']['videoCount'],
            "playlist_details": playlist_details_to_mongo_db (data['items'][0]['id'])

                }
    channel_db.insert_one(ch_details)

def playlist_details_to_mongo_db(channel_id):
    
    pl_of_ch_id = get_playlist_details(youtube, channelId =channel_id)
    pl_det = []
    
    for i in pl_of_ch_id['items']:
        pl_id= i['id']
        pl_item_det=get_playlistitems_details(youtube, pl_id)
        # playlistitem_details_to_mongo_db(pl_id)
        pl_items=[]
        for j in pl_item_det['items']:
            pl_items.append({ 'channelId':j['snippet']['channelId'],
                            'channelTitle':j['snippet']['channelTitle'],
                        'playlistId':j['snippet']['playlistId'],
                        'videoId':j['contentDetails']['videoId'],
                        'Video_details': video_details_to_mongo_db(j['contentDetails']['videoId'])
                                                })
        # st.write(pl_items)
        try:
            playlistitems_db.insert_many(pl_items)
        except:
            pass
        pl_det.append({
                    'playlistId' :i['id'],
                    'channelId' :i['snippet']['channelId'],
                    'playlistTitle' :i['snippet']['title'],
                    
                    'Playlist_video_count' :i['contentDetails']['itemCount'],
                    'playlistitem_details' : pl_items
                                    })
    # st.write(pl_det)                                
    playlist_det = playlist_db.insert_many(pl_det)
    return playlist_det.inserted_ids

# def playlistitem_details_to_mongo_db(pl_item_of_each_pl_id):
#     for i in pl_item_of_each_pl_id['items']:
#         pl_items_details = {
#                         'playlistitem_details':{ 'channelId':i['snippet']['channelId'],
#                         'channelTitle':i['snippet']['channelTitle'],
#                         'playlistId':i['snippet']['playlistId'],
#                         'videoId':i['contentDetails']['videoId']
#                                                 }
#                             }
#         playlistitems_db.insert_one(pl_items_details)

def video_details_to_mongo_db(v_id):
    vid_det = get_video_details(youtube, id = v_id)
    
    for each_item in vid_det['items']:
        try:
            
            vid_details = {
                    'videoId' :each_item['id'],
                    'video_publishedAt':each_item['snippet']['publishedAt'],
                    'channelId':each_item['snippet']['channelId'],
                    'video_title':each_item['snippet']['title'],
                    'video_description':each_item['snippet']['description'],
                    'thumbnail_url':each_item['snippet']['thumbnails']['default']['url'],
                    'channelTitle':each_item['snippet']['channelTitle'],
                    'duration':each_item['contentDetails']['duration'],
                    'viewCount':each_item['statistics']['viewCount'],
                    'likeCount':each_item['statistics']['likeCount'],
                    'favoriteCount':each_item['statistics']['favoriteCount'],
                    'commentCount': each_item['statistics']['commentCount'],
                    'commentDetails': comment_details_to_mongo_db(each_item['id']) 

                 }
                  
        except:
            vid_details = {
                    'videoId' :each_item['id'],
                    'video_publishedAt':each_item['snippet']['publishedAt'],
                    'channelId':each_item['snippet']['channelId'],
                    'video_title':each_item['snippet']['title'],
                    'video_description':each_item['snippet']['description'],
                    'thumbnail_url':each_item['snippet']['thumbnails']['default']['url'],
                    'channelTitle':each_item['snippet']['channelTitle'],
                    'duration':each_item['contentDetails']['duration'],
                    'viewCount':each_item['statistics']['viewCount'],
                    'likeCount':each_item['statistics']['likeCount'],
                    'favoriteCount':each_item['statistics']['favoriteCount'],
                    'commentCount': -1,
                    'commentDetails': 'Not Available' 
                 }
                  
        video = video_db.insert_one(vid_details)
        return video.inserted_id
def comment_details_to_mongo_db(vid_id):
    comm_details = get_comment_details(youtube, vid_id)
    comments=[]
    
    
    for i in comm_details['items']:
        comments.append ({
            'commentId': i['id'],
            'videoId':i['snippet']['topLevelComment']['snippet']['videoId'],
            'textDisplay':i['snippet']['topLevelComment']['snippet']['textDisplay'],
            'authorDisplayName':i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'publishedAt': i['snippet']['topLevelComment']['snippet']['publishedAt']
                    })
    comment = comment_db.insert_many(comments)
    return comment.inserted_ids 
 
if __name__ == "__main__":
    youtube = youtube_authenticate()
    
    

    user_input_channel_ids=[]
    channels = {}
    playlist={}
    playlist_ids={}
    playlistitems={}
    video_details={}
    comment_details={}
    

    number = st.sidebar.number_input(':red[Enter the number of channels you wish to extract]',value=1,min_value=1,max_value=10)      
    st.write("you can find the :green[channel ID] of any youtube channel if the share button in the about section is clicked (or)") 
    st.write("go to this link to find it :green[https://commentpicker.com/youtube-channel-id.php] ")
    for i in range(number):
        user_input_channel_ids.append(st.text_input("enter",key=i))
    if st.button("PROCEED"):
            
        st.write("Processing...")
        pwd = st.secrets["mongodb_pwd"]
        url = f"mongodb+srv://svrdb:{pwd}@ytdatabysvr.0dp48ba.mongodb.net/?retryWrites=true&w=majority"

        # Create a new client and connect to the server
        client = MongoClient(url, server_api=ServerApi('1'))

        # Sending a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            st.write("Pinged your deployment. You are successfully connected to MongoDB!")
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

        for each_id in user_input_channel_ids:
            channel_details_to_mongo_db(get_channel_details(youtube,id=each_id))
        st.write(":green[Completed successfully.]")
        st.write("please navigate to next page in the sidebar" )      
    else:
        st.write(":red[click proceed to get data using API and migrate them to Mongo DB Atlas]")     
