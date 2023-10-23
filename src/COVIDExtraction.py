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

try:
    cur.execute("DROP TABLE IF EXISTS country_latest; \
                CREATE TABLE IF NOT EXISTS country_latest (country_ID SERIAL PRIMARY KEY, country VARCHAR, confirmed INT, deaths INT, recovered INT, active INT, new_cases INT, new_deaths INT, new_recovered INT, deaths_per_100 DECIMAL, recovered_per_100 DECIMAL, deaths_per_100_recovered DECIMAL, confirmed_last_week INT, one_week_change INT, one_week_percent_chg DECIMAL, who_region VARCHAR)")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)


try:
    country_wise_latest_file = open("../datasets/COVID/country_wise_latest.csv", 'r')
    cur.copy_expert(f"COPY country_latest (country, confirmed, deaths, recovered, active, new_cases, new_deaths, new_recovered, deaths_per_100, recovered_per_100, deaths_per_100_recovered, confirmed_last_week, one_week_change, one_week_percent_chg, who_region) FROM STDIN WITH CSV HEADER", country_wise_latest_file)
    # cur.execute("ALTER TABLE country_latest ADD COLUMN country_ID SERIAL PRIMARY KEY;")
except psycopg2.DatabaseError as e:
    print("Issues!")
    print(e)
