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
        cur.execute("DROP DATABASE CovidData")
        cur.execute("CREATE DATABASE CovidData")
    except psycopg2.Error as e:
        print("Error creating DB")
        print(e)

    try:
        conn.close()
    except psycopg2.Error as e:
        print(e)
    
    return cur, conn
