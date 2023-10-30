import psycopg2
import pandas as pd
import numpy as np
import io

def create_database():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=simon password=")
    except psycopg2.Error as e:
        print("Connection issue")
        print(e)
    
    try:
        # Setting autocommit means we do not have to use conn.commit() to commit each change after every command
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Cursor could not be established")
        print(e)
    
    try:
        cur.execute("DROP DATABASE covidData")
        cur.execute("CREATE DATABASE covidData")
    except psycopg2.Error as e:
        print("Error creating DB")
        print(e)

    try:
        conn.close()
    except psycopg2.Error as e:
        print(e)

    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=coviddata user=simon password=")
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        return cur, conn
    except psycopg2.Error as e:
        print(e)
    



# Creation of Pandas dataframes for three tables 
usa_county_deaths_by_date_df = pd.read_csv("../datasets/COVID/usa_county_deaths_by_date.csv")
usa_county_data_df = pd.read_csv("../datasets/COVID/usa_county_data.csv")
global_daily_data_df = pd.read_csv("../datasets/COVID/global_by_day.csv")

# .head will print the first n rows of the dataframe.
print(global_daily_data_df.head(1))
print(usa_county_deaths_by_date_df.head(1))
print(usa_county_data_df.head(1))

cur, conn = create_database()

# Creates the tables
try:
    cur.execute("DROP TABLE IF EXISTS usa_county_death_by_date; \
                DROP TABLE IF EXISTS usa_county_data; \
                DROP TABLE IF EXISTS global_daily_data; \
                CREATE TABLE IF NOT EXISTS usa_county_death_by_date (UID INT PRIMARY KEY, FIPS INT NOT NULL, date DATE, confirmed INT, deaths INT); \
                CREATE TABLE IF NOT EXISTS usa_county_data (FIPS INT PRIMARY KEY, area_name VARCHAR, province_state VARCHAR, country_region VARCHAR); \
                CREATE TABLE IF NOT EXISTS global_daily_data (date DATE PRIMARY KEY, confirmed INT, deaths INT, recovered INT, active INT, new_cases INT, new_deaths INT, new_recovered INT, number_of_countries INT);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

# # Loads the CSV files into the postgreSQL tables

# Function to insert dataframes into DB, row by row

def insert_DF_into_DB(dataframe, SQL):
    for rowindex, row in dataframe.iterrows():
        cur.execute(SQL, list(row))


# SQL statements to be used with above function to insert dataframe data into DB.

usa_county_data_insert = ("""INSERT INTO usa_county_data(FIPS, area_name, province_state, country_region) \
                                        VALUES (%s, %s, %s, %s);""")


global_daily_data_insert = ("""INSERT INTO global_daily_data(date, confirmed, deaths, recovered, active, new_cases, new_deaths, new_recovered, number_of_countries) \
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""")



# Execute function for the two tables above

insert_DF_into_DB(usa_county_data_df, usa_county_data_insert)
insert_DF_into_DB(global_daily_data_df, global_daily_data_insert)


# The remaining table is 600000+ rows in length. Working direct from CSV file, this works, but need to explore further.

usa_county_death_by_date_file = open("../datasets/COVID/usa_county_deaths_by_date.csv", 'r')
cur.copy_expert(f"COPY usa_county_death_by_date (uid, fips, date, confirmed, deaths) FROM STDIN WITH CSV HEADER", usa_county_death_by_date_file)


# try:
#     usa_county_death_by_date_file = open("../datasets/COVID/usa_county_deaths_by_date.csv", 'r')
#     usa_county_data_file = open("../datasets/COVID/usa_county_data.csv", 'r')
#     global_daily_data_file = open("../datasets/COVID/global_by_day.csv", 'r')
#     cur.copy_expert(f"COPY usa_county_death_by_date (uid, fips, date, confirmed, deaths) FROM STDIN WITH CSV HEADER", usa_county_death_by_date_file)
#     cur.copy_expert(f"COPY usa_county_data (fips, area_name, province_state, country_region) FROM STDIN WITH CSV HEADER", usa_county_data_file)
#     cur.copy_expert(f"COPY global_daily_data FROM STDIN WITH CSV HEADER", global_daily_data_file)
# except psycopg2.DatabaseError as e:
#     print("Issues!")
#     print(e)


#Entered for test branch purposes. Can delete