import mysql.connector
import os

# Connect to database
connection = mysql.connector.connect(
    host="localhost",
    user="orlando_bussolo",
    password= os.environ.get("pw_mysql") ,
    database="db_games"
)

# Create a cursor
cursor = connection.cursor()

# SQL command to create the PL_points table 
create_table_query = """
CREATE TABLE IF NOT EXISTS PL_points (
    team_name VARCHAR(100),
    points INT
)
"""
# Execute the SQL command to create the table
cursor.execute(create_table_query)

#  Execute an SQL query to calculate the teams' points
consulta_sql = """
SELECT team_name, SUM(points)
FROM (
    SELECT match_hometeam_name AS team_name,
           CASE
               WHEN match_hometeam_score > match_awayteam_score THEN 3
               WHEN match_hometeam_score < match_awayteam_score THEN 0
               WHEN match_hometeam_score = match_awayteam_score THEN 1
           END AS points
    FROM premiere_league 
    UNION ALL
    SELECT match_awayteam_name AS team_name,
           CASE
               WHEN match_hometeam_score < match_awayteam_score THEN 3
               WHEN match_hometeam_score > match_awayteam_score THEN 0
               WHEN match_hometeam_score = match_awayteam_score THEN 1
           END AS points
    FROM premiere_league 
) AS UNION_DATA
GROUP BY team_name
ORDER BY SUM(points) DESC

"""
# Execute the SQL query
cursor.execute(consulta_sql)

# Insert the results into the PL_points table

for linha in cursor.fetchall():
    insert_query = "INSERT INTO db_games.PL_points VALUES (%s, %s)"
    cursor.execute(insert_query, linha)

#Confirm the transaction
connection.commit()
   

# Close the cursor and the connection
cursor.close()
connection.close()

