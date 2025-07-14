import gspread
import psycopg2
import pandas as pd 
from google.oauth2.service_account import Credentials

# scopes here
scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
#credentials here
credentials = Credentials.from_service_account_file(
    "C:\\Users\\aditz\\Gym_Data_Tracker\\APIs\\grand-strand-465118-v6-5c8c40adf654.json",
    scopes= scopes
)
#authorize command
gc = gspread.authorize(credentials)
#open sheet by key
spreadsheet = gc.open_by_key('1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ')
worksheet = spreadsheet.worksheet('Exercises')
#pass records to data
exercise_table = worksheet.get_all_records()

#connecting to my db
with open('APIs/pgpass.txt', 'r') as f:
    db_pass = f.read().strip()
conn = psycopg2.connect(
    database = 'gym_data',
    user = 'postgres',
    password = db_pass,
    host = 'localhost',
    port = '5432'
)

cursor = conn.cursor()

df = pd.DataFrame(exercise_table)
# since column names are different i will need to map them
column_mapping = {
    'ID':'exercise_id',
    'Name':'exercise_name',
    'Movement type':'exercise_movement_type',
    'Upper/Lower':'exercise_bodysplit'
}
df.rename(columns= column_mapping, inplace=True)
exercise_df = df [['exercise_id', 'exercise_name', 'exercise_movement_type',
                   'exercise_bodysplit']]

# the insert query 
insert_query = """ 
    INSERT INTO staging_layer.exercises(exercise_id,exercise_name,
    exercise_movement_type, exercise_bodysplit)
    SELECT %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM staging_layer.exercises WHERE exercise_id = '%s'
    )
"""

for row in exercise_df.itertuples(index=False):
    cursor.execute(insert_query,(row.exercise_id, 
                                 row.exercise_name,
                                 row.exercise_movement_type,
                                 row.exercise_bodysplit,
                                 row.exercise_id))
conn.commit()
# adding a comment to test linter 
