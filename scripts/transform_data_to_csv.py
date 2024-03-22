from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd
import os

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def create_dataframe(col):
    list_games = []
    for c in col.find():
        list_games.append(c)
    df_games = pd.DataFrame(list_games)    
    return df_games  

def filter_dataframe(df):
    columns = ['match_id', 'league_id', 'match_date', 'match_time', 'match_hometeam_id',
               'match_hometeam_name', 'match_hometeam_score', 'match_awayteam_name',
               'match_awayteam_id', 'match_awayteam_score', 'match_round', 'match_stadium',
               'match_referee','league_year']

    # Remove the columns
    df_games_filtered = df[columns]
    return df_games_filtered

def save_csv(df, path):
    # Verifica se o diretório de saída existe, caso contrário, cria
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(path, index=False)
    print(f"\nThe file {path} is safe")

if __name__ == "__main__":
    # estabelecendo a conexão e recuperando os dados do MongoDB
    pw_mongo = os.environ.get("PW_MONGODB")
    client = connect_mongo(f"mongodb+srv://orlandobussolo:{pw_mongo}@cluster-pipeline.pmkfr1g.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline")
    db =  create_connect_db(client, "db_games")
    col = create_connect_collection(db, "premiere_league")

    df_games = create_dataframe(col)
    df_games_filtered = filter_dataframe(df_games )
    save_csv(df_games_filtered, "/home/orlando_linux/pipeline-python-mongo-mysql/data_teste/tabela.csv")

   

    
