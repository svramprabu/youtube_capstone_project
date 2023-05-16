# youtube_capstone_project

To harvest data from youtube 

Divided the entire project into three pages in streamlit for the ease of access to the user as well as the developer.
1. **Youtube_Data_Harvesting** - Pulling Data from YouTube by providing channel Ids as input and pushing it into Mongo DB Atlas. 
2. **Creating DataFrames and Loading the SQL Database** - Now the data that is pushed to Mongo DB is pulled and transformed to form a Dataframe and then it it loaded into SQL database for further analysis.
3. **Querying SQL Database** - Pulled data from SQL database and answered the questions raised in the project description.

https://www.thepythoncode.com/article/using-youtube-api-in-python

**1. Youtube_Data_Harvesting (Explained)**
    - Created a function to authorize API request. 
    - pinged to Mongo DB atlas for a connection and printed the result of ping.
    - getting an input of number of youtube channels that the user is planning to assess in the sidebar.
    - as per the number input, input boxes are created to get channel ids of the respective channels to be assessed.
    - after typing the channels ids we click get details to proceed.
    - In the next part of the function we get channels details using the channel id provided and save it in a dictionary. Later this dictionary is access to get the required details of the channel and saved in Mongo DB.
    - similar to the above, playlists present in the channel are retrived using the channel id and later moved from dictionary to Mongo DB.
    - playlist ids from the playlist details are extracted and used to get playlist items details which provides details about the playlist like videos inside the playlist. this is also later saved in Mongo DB.
    - video ids present in the playlist items details are used to retrive video details and like before is stored in Mongo DB.
    - once all the above functions are completed with error a completion message is printed and navigation guidelike is printed requesting the user to move to next page from sidebar.

**2. Creating DataFrames and Loading the SQL Database (Explained)**
    - In this function data stored in the Mongo DB is extracted and stored in the form of a DataFrame so that we can transform.
    - Channel details are saved as it is in a dataframe.
    - playlist details and playlist items details are merged on playlist id to form a new dataframe that has all the required fields.
    - video details dataframe is transformed like published date column in converted to datetime, etc.
    - at last comment details dataframe is created as such in Mongo DB.

**3. Querying SQL Database (Explained)**


As the first part of the project is to extract the data from YouTube I created a Google developer account and from there generated API Key and OAuth 2.0 key to get authenticated to pull data from YouTube.

After this used the sample code that is used to find the YouTube Channel details using the Channel ID.

Meanwhile created a text box to input the Channel ID.

The function returned a dictionary file from which I was able locate details like Channel name, Channel ID, Subscription Count, Channel Views, and Description.

Inorder to take multiple Channel IDs as input created a number input box and iterated to store the channel details in a Dictionary.
Migrated this dictionary data to MongoDB.

Using find function in MongoDB filtered to find the channel name that is to be used in the dropdown box list of all the channels given as input so far.

created a dataframe of channel details, playlist details, video details and comment details for the chosen channel in the dropdown. 

later these dataframes are loaded into sql table after some transformation. 

Finally some queries are inserted in the dropdown to answer the questions raised. 


