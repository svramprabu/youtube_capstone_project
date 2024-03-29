import streamlit as st
# import mysql.connector
# from mysql.connector import Error
import sqlite3
import pandas as pd

if __name__ == "__main__":

    
   
    # try:
        
    # mydb = mysql.connector.connect(host=st.secrets["sqlhost"],
    #                                         database=st.secrets["sqldb"],
    #                                         user=st.secrets["sqldb_username"],
    #                                         password=st.secrets["sqldb_pwd"])
    sql_database = 'ytdetail.db'
    mydb = sqlite3.connect(f'{sql_database}')
    
    if mydb is not None : # and not mydb.closed:

    # if mydb.is_connected():
        # db_Info = mydb.get_server_info()
        # st.write("Connected to MySQL Server version ", db_Info)
        cursor = mydb.cursor()
        
        # cursor.execute("select database();")
        
        # record = cursor.fetchone()
        st.write(f"You're connected to database:  {sql_database}")

    # sql_database=st.secrets["sqldb"]
    
    if st.button(" Q1 What are the names of all the videos and their corresponding channels?"):
        Q1="SELECT video_det.video_title, video_det.ChannelTitle FROM video_det LEFT JOIN playlist_det ON video_det.playlist_id=playlist_det.playlist_id"
        ans1 = pd.read_sql(Q1,mydb)
        st.write(f"Query: {Q1}")
        st.write(ans1)

    if st.button("Q2 Which channels have the most number of videos, and how many videos do they have?"):
        # Q2=f"SELECT * FROM {sql_database[:-3]}.channel_det ORDER BY Number_of_Videos DESC"
        Q2=f"SELECT * FROM channel_det ORDER BY Number_of_Videos DESC"
        ans2 = pd.read_sql(Q2,mydb)
        st.write(f"Query: {Q2}")
        st.write(ans2[['Channel_Name','Number_of_Videos']].loc[0])

    if st.button("Q3 What are the top 10 most viewed videos and their respective channels?"):
        a = pd.read_sql(f"SELECT Channel_Name FROM channel_det",mydb)
        channels_list = []
        for i in range(len(a)):
            channels_list.append(a.loc[i].values[0])

        ans3= pd.DataFrame()
        for each_channel in channels_list:
            Q3=f"SELECT * FROM video_det WHERE channelTitle='{each_channel}' ORDER BY viewCount DESC LIMIT 10"
            ans3=pd.concat([ans3,pd.read_sql(Q3,mydb)],ignore_index=True)
            st.write(f"Query: {Q3}")
        st.write(ans3[['video_title','channelTitle','viewCount']])

    if st.button("Q4 How many comments were made on each video, and what are their corresponding video names?"):
        Q4 = "SELECT video_det.video_title, video_det.video_id,count(comment_det.comment_id) AS no_of_comments FROM video_det LEFT JOIN comment_det ON video_det.video_id=comment_det.video_id group by video_det.video_id"
        ans4=pd.read_sql(Q4,mydb)
        st.write(f"Query: {Q4}")
        st.write(ans4)

    if st.button("Q5 Which videos have the highest number of likes, and what are their corresponding channel names?"):
        Q5 ="SELECT video_det.video_title, video_det.likeCount,channel_det.Channel_Name FROM video_det LEFT JOIN playlist_det ON video_det.playlist_id=playlist_det.playlist_id LEFT JOIN channel_det ON channel_det.Channel_Id=playlist_det.Channel_id ORDER BY video_det.likeCount DESC"
        ans5 = pd.read_sql(Q5,mydb)
        st.write(f"Query: {Q5}")
        st.write(ans5)

    if st.button("Q6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?"):
        Q6=f"SELECT video_det.video_title,video_det.likeCount FROM video_det ORDER BY video_det.likeCount DESC LIMIT 10"
        ans6 = pd.read_sql(Q6,mydb)
        st.write(f"Query {Q6}")
        st.write(ans6)

    if st.button("Q7 What is the total number of views for each channel, and what are their corresponding channel names?"):
        Q7 = f"SELECT channel_det.Channel_Name,channel_det.Channel_Views FROM channel_det"
        ans7 = pd.read_sql(Q7,mydb)
        st.write(f"Query: {Q7}")
        st.write(ans7)

    if st.button("Q8 What are the names of all the channels that have published videos in the year 2022?"):
        Q8 = "SELECT video_det.video_title,video_det.channelTitle,video_det.video_publishedAt FROM video_det WHERE strftime('%Y',video_publishedAt) = 2022"
        ans8 = pd.read_sql(Q8,mydb)
        st.write(f"Query: {Q8}")
        st.write(ans8)

    if st.button("Q9 What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
        Q9 = f"SELECT video_det.channelTitle, sum(video_det.duration)/count(video_det.video_id) AS average_duration_in_seconds FROM video_det group by video_det.channelTitle"
        ans9=pd.read_sql(Q9,mydb)
        st.write(f"Query {Q9}")
        st.write(ans9)

    if st.button("Q10 Which videos have the highest number of comments, and what are their corresponding channel names?"):
        Q10="SELECT count(comment_det.comment_id) AS no_of_comments,video_det.video_title ,video_det.channelTitle FROM video_det LEFT JOIN comment_det ON comment_det.video_id=video_det.video_id GROUP BY video_det.video_title,video_det.channelTitle ORDER BY no_of_comments DESC"
        ans10 = pd.read_sql(Q10,mydb)
        st.write(f"Query: {Q10}")
        st.write(ans10)
    
    st.write("Thank you this is the end of project scope")

        # except Error as e:
        #     st.write("Error while connecting to MySQL", e)                
            
    


    








