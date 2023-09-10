import streamlit as st
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def convert_duration(each_item):
    if each_item.__contains__('H'):
        if each_item.__contains__('M'):
            if each_item.__contains__('S'):
                hours = int(each_item[2:].split('H')[0])
                minutes = int(each_item[2:].split('H')[1].split('M')[0])
                total_seconds = int(each_item[2:].split('M')[1][:-1]) + (minutes*60) + (hours*60*60)
                return total_seconds
            else:
                hours = int(each_item[2:].split('H')[0])
                minutes = int(each_item[2:-1].split('H')[1])
                return (hours*60*60 + minutes*60)
        elif each_item.__contains__('S'):
            hours = int(each_item[2:].split('H')[0])
            total_seconds = int(each_item[2:].split('H')[1][:-1])  + (hours*60*60)
            return total_seconds
        else:
            hours = int(each_item[2:-1])
            return hours*60*60
    elif each_item.__contains__('M'):
        if each_item.__contains__('S'):
            minutes = int(each_item[2:].split('M')[0])
            total_seconds = int(each_item[2:].split('M')[1][:-1]) + (minutes*60) 
            return total_seconds     
        else:
            minutes = int(each_item[2:].split('M')[0])
            return minutes*60
    else:
        return int(each_item[2:-1])


if __name__ == "__main__":
    pwd = st.secrets["mongodb_pwd"]
    uri = f"mongodb+srv://svrdb:{pwd}@ytdatabysvr.0dp48ba.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        st.write("Pinged your deployment. You successfully connected to MongoDB Atlas !")
    except Exception as e:
        st.write(e)
    st.write("Lets retrive Data from Mongo DB for Transformation...")

    yt_dbs = client['yt_dbs']

    channel_db = yt_dbs['channels']
    playlist_db = yt_dbs['playlists']
    playlistitems_db = yt_dbs['playlistitems']
    video_db = yt_dbs['videodetails']
    comment_db = yt_dbs['comments']

            #channel df creation
    channels_df = pd.DataFrame(list(channel_db.find({},{'_id':0,'playlist_details':0})))
    
            #playlist df creation
    playlist_df = pd.DataFrame(list(playlist_db.find({},{'_id':0,'playlistitem_details':0})))
   
            #playlist items data frame creation
    pl_items_df = pd.DataFrame(list(playlistitems_db.find({},{'_id':0,'Video_details':0})))
    
            # video details to dataframe
    video_df = pd.DataFrame(list(video_db.find({},{'_id':0,'commentDetails':0})))    
    video_df['video_publishedAt']= pd.to_datetime(video_df['video_publishedAt'])
    video_df['duration'] = video_df['duration'].apply(lambda row : convert_duration(row))            
    video_df = video_df.merge(pl_items_df,how='inner')
    video_df = video_df.applymap(str)

            # comment details to dataframe
    comment_df = pd.DataFrame(list(comment_db.find({},{'_id':0})))
    comment_df['publishedAt']= pd.to_datetime(comment_df['publishedAt'])
    comment_df = comment_df.applymap(str)


            #merging all details into one table 
    final_df = channels_df.merge(playlist_df,how='left')
    final_df = final_df.merge(video_df,how= 'right',on=['playlistId','channelTitle','channelId'])
    final_df = final_df.merge(comment_df,how='left',on='videoId')
    # st.write(final_df)


    dropdown=[]

    for each_ch_name in channels_df['channelTitle']:                    # to find the channel names from mongo db
            dropdown.append(each_ch_name)
    
    options = st.multiselect(                              # create a dropdown of channels searched in streamlit
        'which of these channels do you like to work on?',
        tuple(dropdown))

    # if st.button('channels chosen from dropdown'):
    st.write(":red[select from options above to get details of those channels from Mongo DB]")
    try:        
        for option in options:
                
            try:
                a=SQL_channel_details_df.head()
            except:
                SQL_channel_details_df=pd.DataFrame()
            
            SQL_channel_details_df=pd.concat([SQL_channel_details_df,channels_df[channels_df['channelTitle']==option]])
        
            try:
                b=SQL_plalist_df.head()
            except:
                SQL_plalist_df=pd.DataFrame()

            #CHANNEL_ID=channels_df[channels_df['Channel_Name']==option].index[0]

            SQL_plalist_df = pd.concat([SQL_plalist_df ,playlist_df[playlist_df['channelId']==(channels_df[channels_df['channelTitle']==option]['channelId'].values[0])]])
            
            try:
                c=SQL_video_df.head()
            except:
                SQL_video_df=pd.DataFrame()
            try:
                d=SQL_comments_df.head()
            except:
                SQL_comments_df=pd.DataFrame()

            for each_pl_id in set(SQL_plalist_df['playlistId']):

                SQL_video_df=pd.concat([SQL_video_df,video_df[video_df['playlistId']==each_pl_id]])

            for each_v_id in set(SQL_video_df['videoId']):

                SQL_comments_df=pd.concat([SQL_comments_df,comment_df[comment_df['videoId']==each_v_id]])

        SQL_channel_details_df.reset_index(inplace = True, drop = True)
        SQL_plalist_df.reset_index(inplace = True, drop = True)
        SQL_video_df=SQL_video_df.drop_duplicates(subset=['videoId'])
        SQL_video_df.reset_index(inplace = True, drop = True)
        SQL_comments_df=SQL_comments_df.drop_duplicates(subset=['commentId'])
        SQL_comments_df.reset_index(inplace = True, drop = True)

    
        st.header(":blue[Channel Details]")
        st.dataframe(SQL_channel_details_df)
        st.header(":blue[Playlist Details]")
        st.dataframe(SQL_plalist_df)
        st.header(":blue[Video Details]")
        st.dataframe(SQL_video_df)
        st.header(":blue[Comments Details]")
        st.dataframe(SQL_comments_df)
    except:
        # pass
        st.write("no channels chosen")


        
    # else:

    # sql_host_name = st.sidebar.text_input("MySql host")
    # sql_username = st.sidebar.text_input("MySql database username")
    # sql_password = st.sidebar.text_input("MySql database password")
    # sql_database = st.sidebar.text_input("MySql database name")
    # sql_port_no = st.sidebar.number_input("enter the MySql db port number",value=3306)


    if st.button("Load to SQLite database"):
        
        # import mysql.connector
        # from mysql.connector import Error
        import sqlite3

        
            # mydb = mysql.connector.connect(host=sql_host_name,
            #                                      database=sql_database,
            #                                      user=sql_username,
            #                                      password=sql_password,
            #                                     port=sql_port_no)
        sql_database = 'ytdetail.db'
        mydb = sqlite3.connect(f'{sql_database}')

        if mydb is not None : # and not mydb.closed:
            print("SQLite connection is open")
        
        # if mydb.is_connected():
            # db_Info = mydb.get_server_info()
            # st.write("Connected to MySQL Server version ", db_Info)
            cursor = mydb.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            st.write("You're connected to database: ", sql_database)
            cursor.execute("drop table if exists comment_det")
            cursor.execute("drop table if exists video_det")
            cursor.execute("drop table if exists playlist_det")
            cursor.execute("drop table if exists channel_det")

            cursor.execute("create table if not exists channel_det(Channel_Name TEXT,\
                           Channel_id TEXT PRIMARY KEY,\
                           Subscription_Count INTEGER,\
                           Channel_Views INTEGER,\
                           Channel_Description TEXT,\
                           Number_of_Videos INTEGER)")
            cursor.execute("create table if not exists playlist_det(playlist_id TEXT PRIMARY KEY, \
                           Channel_id TEXT, \
                           Playlist_title TEXT, \
                           Playlist_video_count INTEGER,\
                           CONSTRAINT fk_channel_det FOREIGN KEY (Channel_id) REFERENCES channel_det(Channel_id))")
            cursor.execute("create table if not exists video_det(video_id TEXT PRIMARY KEY, \
                           video_publishedAt TEXT, \
                           Channel_id TEXT, \
                           video_title TEXT, \
                           Video_description TEXT,\
                           thumbnail_url TEXT, \
                           channelTitle TEXT, \
                           duration INTEGER, \
                           viewCount INTEGER, \
                           likeCount INTEGER,\
                           favoriteCount INTEGER, \
                           commentCount INTEGER, \
                           playlist_id TEXT, \
                           CONSTRAINT fk_playlist_det FOREIGN KEY (playlist_id) REFERENCES playlist_det(playlist_id))")
            cursor.execute("create table if not exists comment_det(comment_id TEXT PRIMARY KEY, \
                           video_id TEXT, \
                           textDisplay TEXT, \
                           authorDisplayName TEXT,\
                           publishedAt TEXT,\
                           CONSTRAINT fk_video_det FOREIGN KEY (video_id) REFERENCES video_det(video_id))")    
            st.write("created tables in database")

            for each_row in range(len(SQL_channel_details_df)):
                val = tuple(SQL_channel_details_df.loc[each_row])
                sql = "insert into channel_det values (?,?,?,?,?,?)"
                cursor.execute(sql,val)
                mydb.commit()


            for each_row in SQL_plalist_df.index:
                val=tuple(SQL_plalist_df.values[each_row])
                # st.write(val)
                sql = "insert into playlist_det values (?,?,?,?)"
                cursor.execute(sql,val)
                mydb.commit()   

            for each_row in SQL_video_df.index:
                val=tuple(SQL_video_df.values[each_row])
                sql = "insert into video_det values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                cursor.execute(sql,val)
                mydb.commit()   

            for each_row in SQL_comments_df.index:
                val=tuple(SQL_comments_df.values[each_row])
                sql = "insert into comment_det values (?,?,?,?,?)"
                cursor.execute(sql,val)
                mydb.commit()

            st.write(":green[Finished loading details to SQL database]")
            st.write("navigate to next page in sidebar to proceed")
            mydb.close()
    
        else:
            print("SQLite connection is closed")
        # except Error as e:
       
      
    # elif st.button("Load to planet scale MySQL database"):
        
    #     import mysql.connector
    #     from mysql.connector import Error

    #     try:
    #         mydb = mysql.connector.connect(host=st.secrets["sqlhost"],
    #                                              database=st.secrets["sqldb"],
    #                                              user=st.secrets["sqldb_username"],
    #                                              password=st.secrets["sqldb_pwd"])
    #                                             # port=sql_port_no)
    #         if mydb.is_connected():
    #             db_Info = mydb.get_server_info()
    #             st.write("Connected to planet scale MySQL Server version ", db_Info)
    #             cursor = mydb.cursor()
    #             cursor.execute("select database();")
    #             record = cursor.fetchone()
    #             st.write("You're connected to database: ", record)

    #             cursor.execute("drop table if exists comment_det")
    #             cursor.execute("drop table if exists video_det")
    #             cursor.execute("drop table if exists playlist_det")
    #             cursor.execute("drop table if exists channel_det")

    #             cursor.execute("create table if not exists channel_det(Channel_Name TEXT,Channel_Id TEXT PRIMARY KEY,Subscription_Count INTEGER,Channel_Views INTEGER,Channel_Description TEXT,Number_of_Videos INTEGER)")
    #             cursor.execute("create table if not exists playlist_det(playlist_id TEXT PRIMARY KEY, Channel_id TEXT,  Playlist_title TEXT, Playlist_video_count INTEGER)")
    #             cursor.execute("create table if not exists video_det(video_id TEXT PRIMARY KEY, video_publishedAt TEXT, Channel_id TEXT, video_title TEXT, Video_description TEXT,thumbnail_url TEXT, channelTitle TEXT, duration INTEGEREGER, viewCount INTEGER, likeCount INTEGER,favoriteCount INTEGER, commentCount INTEGER, playlist_id TEXT)")
    #             cursor.execute("create table if not exists comment_det(comment_id TEXT PRIMARY KEY, video_id TEXT,textDisplay TEXT, authorDisplayName TEXT,publishedAt TEXT)")    

    #             for each_row in range(len(SQL_channel_details_df)):
    #                 val = tuple(SQL_channel_details_df.loc[each_row])
    #                 sql = "insert into channel_det values (%s,%s,%s,%s,%s,%s)"
    #                 cursor.execute(sql,val)
    #                 mydb.commit()


    #             for each_row in SQL_plalist_df.index:
    #                 val=tuple(SQL_plalist_df.values[each_row])
    #                 # st.write(val)
    #                 sql = "insert into playlist_det values (%s,%s,%s,%s)"
    #                 cursor.execute(sql,val)
    #                 mydb.commit()   

    #             for each_row in SQL_video_df.index:
    #                 val=tuple(SQL_video_df.values[each_row])
    #                 sql = "insert into video_det values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #                 cursor.execute(sql,val)
    #                 mydb.commit()   

    #             for each_row in SQL_comments_df.index:
    #                 val=tuple(SQL_comments_df.values[each_row])
    #                 sql = "insert into comment_det values (%s,%s,%s,%s,%s)"
    #                 cursor.execute(sql,val)
    #                 mydb.commit()

    #             st.write(":green[Finished loading details to SQL database]")
    #             st.write("navigate to next page in sidebar to proceed")

    #     except Error as e:
    #         st.write("Error while connecting to planet scale MySQL", e)


    else:
         st.write(":red[click 'Load to SQLite database' to load the filtered data]")


    



