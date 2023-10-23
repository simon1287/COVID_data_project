import psycopg2

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
    cur.execute("CREATE DATABASE dataModelEx1")
except psycopg2.Error as e:
    print("Error creating DB")
    print(e)

try:
    conn.close()
except psycopg2.Error as e:
    print(e)

#Going to now connect to the existing db
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=datamodelex1 user=simon password=")
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
    cur.execute("DROP TABLE IF EXISTS students; \
                CREATE TABLE IF NOT EXISTS students (student_id SERIAL PRIMARY KEY, name VARCHAR, age INT, gender VARCHAR, subject VARCHAR, marks INT)")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)


try:
    cur.execute("INSERT INTO students (name, age, gender, subject, marks) \
                VALUES (%s, %s, %s, %s, %s)", \
                ("Simon", 36, "Male", "Python", 56))
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("INSERT INTO students (name, age, gender, subject, marks) \
                VALUES (%s, %s, %s, %s, %s)", \
                ("Wendy", 69, "Female", "Baking", 82))
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("SELECT * from students;")
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

allrows = cur.fetchall()
print(allrows)

cur.close()
conn.close()