import mysql.connector
import pandas as pd
import numpy as np
import os

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = pw
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"\nBase de dados {db_name} criada")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

def create_championship_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
        
        match_id INT,
        league_id INT,
        match_date DATE,
        match_time TIME,
        match_hometeam_id INT,
        match_hometeam_name VARCHAR(255),
        match_hometeam_score INT,
        match_awayteam_name VARCHAR(255),
        match_awayteam_id INT,
        match_awayteam_score INT,
        match_round INT,
        match_stadium VARCHAR(255),
        match_referee VARCHAR(255),
        league_year VARCHAR(50),
                
        PRIMARY KEY (match_id)
    )""")
                   
    print(f"\nTabela {tb_name} criada")

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for i, row in df.iterrows()]
    lista = [tuple(None if pd.isna(item) else item for item in tupla) for tupla in lista]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")
    cnx.commit()

if __name__ == "__main__":
    
    # connecting to mysql
    pw_mysql = os.environ.get("PW_MYSQL")    
    cnx = connect_mysql("localhost", "orlando_bussolo", pw_mysql)
    cursor = create_cursor(cnx)

    # creating database
    create_database(cursor, "db_games")
    show_databases(cursor)

    # creating table
    create_championship_table(cursor, "db_games", "premiere_league")
    show_tables(cursor, "db_games")

    # reading and adding data
    df = read_csv("/home/orlando_linux/pipeline-python-mongo-mysql/data_teste/tabela.csv")
    add_data(cnx, cursor, df, "db_games", "premiere_league")

