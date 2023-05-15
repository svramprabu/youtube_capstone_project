# youtube_capstone_project

To harvest data from youtube 

Divided the entire project into three pages in streamlit for the ease of access to the user as well as the developer.
1. **Youtube_Data_Harvesting** - Pulling Data from YouTube by providing channel Ids as input and pushing it into Mongo DB Atlas. 
2. **Creating DataFrames and Loading the SQL Database** - Now the data that is pushed to Mongo DB is pulled and transformed to form a Dataframe and then it it loaded into SQL database for further analysis.
3. **Querying SQL Database** - Pulled data from SQL database and answered the questions raised in the project description.

https://www.thepythoncode.com/article/using-youtube-api-in-python

**1. Youtube_Data_Harvesting (Explained)**
**2. Creating DataFrames and Loading the SQL Database (Explained)**
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


