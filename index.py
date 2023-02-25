import pandas as pd
from mysql.connector import connect, Error
import sqlite3
from tqdm import tqdm
from multiprocessing import Process

# read csv
df = pd.read_csv('canada_employment_trend_cycle_dataset.csv')
df.fillna('', inplace=True)
print(df.shape)

# clean csv
df.rename(columns={
    "Data type": "Data_Type",
    "North American Industry Classification System (NAICS)": "NAICS"
    },
    inplace=True
)
df.drop(columns=["STATUS", "SYMBOL", "TERMINATED", "DECIMALS"], inplace=True)

# splitting data for batch insertion
batches = []
jump = 1000
for i in range(0, len(df), jump):
    batches.append(df[i:i+jump])

# connect to mysql
try:
    with connect(
        host="127.0.0.1",
        user="root",
        password="",
        port="3306",
        # database="proconty_test_big_data" ### uncomment this if you run the file more than once
    ) as connection:
        print(connection)
        ### run this just once
        create_db_query = "CREATE DATABASE proconty_test_big_data"
        with connection.cursor() as cursor:
            cursor.execute(create_db_query)
except Error as e:
    print(e)

# create table in mysql
create_table_query = """
    CREATE TABLE employment_trend (
        id INT AUTO_INCREMENT PRIMARY KEY,
        REF_DATE VARCHAR(100),
        GEO VARCHAR(100),
        DGUID VARCHAR(100),
        NAICS VARCHAR(100),
        Statistics VARCHAR(100),
        Data_Type VARCHAR(100),
        UOM VARCHAR(100),
        UOM_ID VARCHAR(100),
        SCALAR_FACTOR VARCHAR(100),
        SCALAR_ID VARCHAR(100),
        VECTOR VARCHAR(100),
        COORDINATE VARCHAR(100),
        VALUE VARCHAR(100)
    )
"""
connection.reconnect()
with connection.cursor() as cursor:
    cursor.execute(create_table_query)
    connection.commit()

# def insert function
def insert_into_mysql(data, is_mysql=False):
    if is_mysql:
        connection.reconnect()
    with connection.cursor() as cursor:
            insert_query = F"""
                INSERT INTO employment_trend (REF_DATE, GEO, DGUID, NAICS, Statistics, Data_Type, UOM, UOM_ID, SCALAR_FACTOR, SCALAR_ID, VECTOR, COORDINATE, VALUE) VALUES {data} 
            """
            cursor.execute(insert_query)
            connection.commit()

# insert data into mysql db
for batch in tqdm(batches):
    data = [tuple(row) for row in batch.values]
    data = str(data)[1:][:-1]
    insert_into_mysql(data, True)

# get inserted data
all_records = []
connection.reconnect()
select_query = "SELECT * FROM employment_trend"
with connection.cursor() as cursor:
    cursor.execute(select_query)
    result = cursor.fetchall()
    for row in result:
        all_records.append(row)

# connect to sqlite
sqlite3_connection = sqlite3.connect("proconty_test_big_data.db")

# create table in sqlite
sqlite3_connection.execute(create_table_query)

# splitting data for batch insertion
batches = []
jump = 1000
for i in range(0, len(all_records), jump):
    batches.append(all_records[i:i+jump])

# insert data into sqlite db
for batch in tqdm(batches):
    data = [item[1:] for item in batch]
    data = str(data)[1:][:-1]
    sqlite3_connection.execute(f"""
        INSERT INTO employment_trend (REF_DATE, GEO, DGUID, NAICS, Statistics, Data_Type, UOM, UOM_ID, SCALAR_FACTOR, SCALAR_ID, VECTOR, COORDINATE, VALUE) VALUES {data}
    """)

# get inserted data
all_records_2 = []
select_query = "SELECT * FROM employment_trend"
with connection.cursor() as cursor:
    cursor.execute(select_query)
    result = cursor.fetchall()
    for row in result:
        all_records_2.append(row)

# validation
assert len(all_records) == len(all_records_2), "migration was well!"