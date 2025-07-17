import pandas as pd 
import gspread 
from google.oauth2.service_account import Credentials 
from sqlalchemy import (
    MetaData, create_engine, Table, Column,
    select, insert, String
)
import extract_exercise_muscles as eem
import extract_exercise as ee
import extract_muscles as em 
import extract_resistance_types as ert 
import extract_workouts as ew 
import connect as c

c.connect_sheets()
c.connect_db()
ee.load_exercises()
em.load_muscles()
eem.load_exercise_muscles()
ert.load_resistance_types()
ew.load_workouts()

if __name__ == '__main__':
    main()
