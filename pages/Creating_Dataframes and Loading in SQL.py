import streamlit as st
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

#import mysql.connector



if __name__ == "__main__":
    pwd = st.secrets["mongodb_pwd"]
    uri = f"mongodb+srv://svrdb:{pwd}@ytdatabysvr.0dp48ba.mongodb.net/?retryWrites=true&w=majority"

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

    #channel df creation

    for each_channel_id in channel_db.find():
        channels_df = pd.DataFrame(columns = list(each_channel_id['Channel_Details'].keys()))
        break
        
    for each_channel_id in channel_db.find():
        channels_df=pd.concat([channels_df, pd.Series(each_channel_id['Channel_Details']).to_frame().T],ignore_index=True)

    #playlist df creation

    for each_pl in playlist_db.find():
        col_name_pl=list(each_pl['playlist_details'].keys())
        break
    playlist_df = pd.DataFrame(columns=col_name_pl) 

    for each_pl in playlist_db.find():
        playlist_df=pd.concat([playlist_df,pd.Series(each_pl['playlist_details']).to_frame().T],ignore_index=True)

    playlist_df=playlist_df.drop(['playlist_description'],axis=1)

    

    #playlist items data frame creation

    for i in playlistitems_db.find():
            col_pl_item_names = list(i['playlistitem_details'].keys())
            break
    pl_items_df = pd.DataFrame(columns=col_pl_item_names)

    for i in playlistitems_db.find():
        pl_items_df=pd.concat([pl_items_df,pd.Series(i['playlistitem_details']).to_frame().T],ignore_index=True)
        # pl_items_df=pl_items_df.set_index('playlist_id')
        #pl_items_df=pl_items_df.replace(r'^\s*$', 'none', regex=True)
        #pl_items_df['playlist_title']=pl_items_df['playlist_id']


    # for pl_id in playlist_df['playlist_id']:
    #     pl_items_df['playlist_title']=pl_items_df['playlist_title'].replace(pl_id,(playlist_df[playlist_df['playlist_id']==pl_id]['playlist_title'].values[0]))
    
    
    playlist_df = pd.merge(pl_items_df, playlist_df, on=['playlist_id','channelId'])
    playlist_df=playlist_df.drop_duplicates(subset=['playlist_id'])


    # video details to dataframe
    for i in video_db.find():
            vid_col_names=list(i['video_details'].keys())
            break
    video_df=pd.DataFrame(columns=vid_col_names)
    for i in video_db.find():
        video_df=pd.concat([video_df,pd.Series(i['video_details']).to_frame().T],ignore_index=True)

    video_df['video_publishedAt']= pd.to_datetime(video_df['video_publishedAt'])
    video_df['playlist_id']=video_df['Video_id']

    for each_id in playlist_df['Video_id']:
        video_df['playlist_id']=video_df['playlist_id'].replace(each_id,(playlist_df[video_df['playlist_id']==each_id]['playlist_id']).values[0])
    playlist_df=playlist_df.drop(['Video_id'], axis=1)

    for each_item in video_df['duration']:
        try:
            minutes = int(each_item[2:].split('M')[0])
            total_seconds = int(each_item[2:].split('M')[1][:-1]) + (minutes*60)
            video_df.loc[video_df['duration']==each_item,'duration']=str(total_seconds)
        except:
            pass

    
    # comment details to dataframe

    for i in comment_db.find():
        comment_col_names=list(i['Comment_details'].keys())
        break
    comment_df = pd.DataFrame(columns=comment_col_names)

    for i in comment_db.find():
        comment_df=pd.concat([comment_df,pd.Series(i['Comment_details']).to_frame().T],ignore_index=True)
    comment_df['publishedAt']= pd.to_datetime(comment_df['publishedAt'])
    
        

    

    # st.dataframe(channels_df)
    # st.dataframe(playlist_df)
    # st.dataframe(video_df)
    # st.dataframe(comment_df)

    dropdown=[]

    for each_ch_name in channels_df['Channel_Name']:                    # to find the channel names from mongo db
            dropdown.append(each_ch_name)
            #st.write(each_ch_name)
    try:
        options = st.multiselect(                              # create a dropdown of channels searched in streamlit
            'which of these channels do you like to work on?',
            tuple(dropdown))
    

        #st.write(options)
        for option in options:
                
            try:
                a=SQL_channel_details_df.head()
            except:
                SQL_channel_details_df=pd.DataFrame()
            
            SQL_channel_details_df=pd.concat([SQL_channel_details_df,channels_df[channels_df['Channel_Name']==option]])
        
            try:
                b=SQL_plalist_df.head()
            except:
                SQL_plalist_df=pd.DataFrame()

            #CHANNEL_ID=channels_df[channels_df['Channel_Name']==option].index[0]

            SQL_plalist_df = pd.concat([SQL_plalist_df ,playlist_df[playlist_df['channelId']==(channels_df[channels_df['Channel_Name']==option]['Channel_Id'].values[0])]])
            #st.write((channels_df[channels_df['Channel_Name']==option]['Channel_Id'][0]))
            
            try:
                    c=SQL_video_df.head()
            except:
                SQL_video_df=pd.DataFrame()
            try:
                d=SQL_comments_df.head()
            except:
                SQL_comments_df=pd.DataFrame()

            for each_pl_id in set(SQL_plalist_df['playlist_id']):

                SQL_video_df=pd.concat([SQL_video_df,video_df[video_df['playlist_id']==each_pl_id]])

            for each_v_id in set(SQL_video_df['Video_id']):

                SQL_comments_df=pd.concat([SQL_comments_df,comment_df[comment_df['video_id']==each_v_id]])

        SQL_channel_details_df.reset_index(inplace = True, drop = True)
        SQL_plalist_df.reset_index(inplace = True, drop = True)
        SQL_video_df=SQL_video_df.drop_duplicates(subset=['Video_id'])
        SQL_video_df.reset_index(inplace = True, drop = True)
        SQL_comments_df=SQL_comments_df.drop_duplicates(subset=['comment_id'])
        SQL_comments_df.reset_index(inplace = True, drop = True)
        
        st.dataframe(SQL_channel_details_df)
        st.dataframe(SQL_plalist_df)
        st.dataframe(SQL_video_df)
        st.dataframe(SQL_comments_df)
        #st.write(SQL_channel_details_df)
    except:
         
         st.write("choose from options above")

    if st.button("to SQL db"):
        
        import mysql.connector
        from mysql.connector import Error

        try:
            mydb = mysql.connector.connect(host=st.secrets["sqlhost"],
                                                 database='sql12618369',
                                                 user=st.secrets["sqldb_username"],
                                                 password=st.secrets["sqldb_pwd"],
                                                port=3306)
            if mydb.is_connected():
                db_Info = mydb.get_server_info()
                st.write("Connected to MySQL Server version ", db_Info)
                cursor = mydb.cursor()
                #cursor.execute("CREATE DATABASE if not exists yt_details")
                cursor.execute("select database();")
                #cursor.execute("use yt_details")
                record = cursor.fetchone()
                st.write("You're connected to database: ", record)
                cursor.execute("drop table if exists comment_det")
                cursor.execute("drop table if exists video_det")
                cursor.execute("drop table if exists playlist_det")
                cursor.execute("drop table if exists channel_det")

                cursor.execute("create table if not exists channel_det(Channel_Name VARCHAR(255),Channel_Id VARCHAR(255) PRIMARY KEY,Subscription_Count INT,Channel_Views BIGINT,Channel_Description TEXT,Number_of_Videos INT)")
                cursor.execute("create table if not exists playlist_det(Channel_id VARCHAR(255), FOREIGN KEY (Channel_id) REFERENCES channel_det(Channel_Id),Channel_title TEXT,playlist_id VARCHAR(255) PRIMARY KEY, Playlist_title VARCHAR(255), Playlist_video_count INT)")
                cursor.execute("create table if not exists video_det(video_id VARCHAR(255) PRIMARY KEY, video_publishedAt VARCHAR(255), Channel_id VARCHAR(255), video_title TEXT, Video_description TEXT,thumbnail_url VARCHAR(255), channelTitle VARCHAR(255), duration INT, viewCount INT, likeCount INT,favoriteCount INT, commentCount INT, playlist_id VARCHAR(255), FOREIGN KEY (playlist_id) REFERENCES playlist_det(playlist_id))")
                cursor.execute("create table if not exists comment_det(comment_id VARCHAR(255) PRIMARY KEY, video_id VARCHAR(255), FOREIGN KEY (video_id) REFERENCES video_det(video_id),textDisplay TEXT, authorDisplayName VARCHAR(255),publishedAt VARCHAR(255))")    

                #st.dataframe(SQL_channel_details_df)

                for each_row in range(len(SQL_channel_details_df)):
                    val = tuple(SQL_channel_details_df.loc[each_row])
                    sql = "insert into channel_det values (%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,val)
                    mydb.commit()


                for each_row in SQL_plalist_df.index:
                    val=tuple(SQL_plalist_df.values[each_row])
                    sql = "insert into playlist_det values (%s,%s,%s,%s,%s)"
                    cursor.execute(sql,val)
                    mydb.commit()   

                for each_row in SQL_video_df.index:
                    val=tuple(SQL_video_df.values[each_row])
                    sql = "insert into video_det values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,val)
                    mydb.commit()   

                for each_row in SQL_comments_df.index:
                    val=tuple(SQL_comments_df.values[each_row])
                    sql = "insert into comment_det values (%s,%s,%s,%s,%s)"
                    cursor.execute(sql,val)
                    mydb.commit()

                st.write("Finished loading details to SQL ")
                st.write("please navigate to next page")

        except Error as e:
            st.write("Error while connecting to MySQL", e)
        
        
        
       
    else:
         st.write("click 'to SQL' to load the filtered data in SQL db")


    



