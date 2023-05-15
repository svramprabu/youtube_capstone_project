
import pymongo
import streamlit as st
from google.auth.transport.requests import Request
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

import numpy as np
import pandas as pd
import mysql.connector
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
  st.write(youtube)
