# youtube_captone

https://www.thepythoncode.com/article/using-youtube-api-in-python

As the first part of the project is to extract the data from YouTube I created a Google developer account and from there generated API Key and OAuth 2.0 key to get authenticated to pull data from YouTube.

After this used the sample code that is used to find the YouTube Channel details using the Channel ID.

Meanwhile created a text box to input the Channel ID.

The function returned a dictionary file from which I was able locate details like Channel name, Channel ID, Subscription Count, Channel Views, and Description.

Inorder to take multiple Channel IDs as input created a number input box and iterated to store the channel details in a Dictionary.
Migrated this dictionary data to MongoDB.

Using find function in MongoDB filtered to find the channel name that is to be used in the dropdown box list of all the channels given as input so far.



