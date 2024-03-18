import os
import requests
import pandas as pd
from datetime import date, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def connect_mongo(uri):    
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client    

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection_game = db[col_name]
    return collection_game


def extract_api_data():    
    league_id = 152 #PremiereLeague
    api_football_key = os.environ.get("API_FOOTBALL_KEY")    
    today = date.today()
    # Extract data from seven days ago up to today
    week_ago = today - timedelta(days=50)
   
    url_base = f"https://apiv3.apifootball.com/?action=get_events&from={week_ago}&to={today}&league_id={league_id}&APIkey={api_football_key}"
    response = requests.get(url_base).json()
    return response
    
def insert_data(col, data):
    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":
    pw_mongo = os.environ.get("PW_MONGODB")
    client = connect_mongo(f"mongodb+srv://orlandobussolo:{pw_mongo}@cluster-pipeline.pmkfr1g.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline")
    db =  create_connect_db(client, "db_games")
    col = create_connect_collection(db, "premiere_league")

    data = extract_api_data()
    print(f"\nTotal Amount of data extracted: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nTotal amount of data inserted: {n_docs}")
    #close connection
    client.close()