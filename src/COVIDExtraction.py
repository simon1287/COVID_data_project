import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=simon password=")
except psycopg2.Error as e:
    print("Connection issue")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Cursor could not be established")
    print(e)

# Setting autocommit means we do not have to use conn.commit() to commit each change after every command
conn.set_session(autocommit=True)

try:
    cur.execute("CREATE DATABASE CovidData")
except psycopg2.Error as e:
    print("Error creating DB")
    print(e)

try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=coviddata user=simon password=")
except psycopg2.Error as e:
    print("Connection issue")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Cursor could not be established")
    print(e)

conn.set_session(autocommit=True)


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

# Loads the CSV files into the postgreSQL tables
try:
    usa_county_death_by_date_file = open("../datasets/COVID/usa_county_deaths_by_date.csv", 'r')
    usa_county_data_file = open("../datasets/COVID/usa_county_data.csv", 'r')
    global_daily_data_file = open("../datasets/COVID/global_by_day.csv", 'r')
    cur.copy_expert(f"COPY usa_county_death_by_date (uid, fips, date, confirmed, deaths) FROM STDIN WITH CSV HEADER", usa_county_death_by_date_file)
    cur.copy_expert(f"COPY usa_county_data (fips, area_name, province_state, country_region) FROM STDIN WITH CSV HEADER", usa_county_data_file)
    cur.copy_expert(f"COPY global_daily_data FROM STDIN WITH CSV HEADER", global_daily_data_file)
except psycopg2.DatabaseError as e:
    print("Issues!")
    print(e)


