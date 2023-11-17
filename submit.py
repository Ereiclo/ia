import psycopg2
import csv
import psycopg2.extras


# connect to database movies
def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="movies",
        user="postgres",
        password=1234,
        port=5432
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cur


conn, cur = get_connection()


#create tables in database
def create_tables():
    #drop all tables
    cur.execute('''
        DROP TABLE IF EXISTS movies;
    ''')
    cur.execute('''
        DROP TABLE IF EXISTS ratings;
    ''')
    cur.execute('''
        DROP TABLE IF EXISTS links;
    ''')
    cur.execute('''
        DROP TABLE IF EXISTS tags;
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            movieId INTEGER PRIMARY KEY,
            title VARCHAR(255),
            genres text 
        );
    ''')

    #create schema for ml-latest-small/ratings.csv
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            userId INTEGER,
            movieId INTEGER,
            rating FLOAT,
            timestamp INTEGER
        );
    ''')


    #create schema for ml-latest-small/links.csv
    cur.execute('''
        CREATE TABLE IF NOT EXISTS links (
            movieId INTEGER,
            imdbId INTEGER,
            tmdbId INTEGER
        );
    ''')

    #create schema for ml-latest-small/tags.csv
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            userId INTEGER,
            movieId INTEGER,
            tag VARCHAR(255),
            timestamp INTEGER
        );
    ''')

    #create index on movie id for table movies
    cur.execute('''
        CREATE INDEX movieid_index ON movies (movieid);
        CREATE INDEX ratings_index ON ratings (movieid);
        CREATE INDEX tags_index ON tags (movieid);
    ''')





    conn.commit()


#insert data into tables
def insert_data():
    with open('ml-latest-small/movies.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute('''
                INSERT INTO movies (movieId, title, genres)
                VALUES (%s, %s, %s);
            ''', row)
    conn.commit()

    with open('ml-latest-small/ratings.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute('''
                INSERT INTO ratings (userId, movieId, rating, timestamp)
                VALUES (%s, %s, %s, %s);
            ''', row)
    conn.commit()

    with open('ml-latest-small/links.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row[2] == '':
                row[2] = -1
            cur.execute('''
                INSERT INTO links (movieId, imdbId, tmdbId)
                VALUES (%s, %s, %s);
            ''', row)
    conn.commit()

    with open('ml-latest-small/tags.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute('''
                INSERT INTO tags (userId, movieId, tag, timestamp)
                VALUES (%s, %s, %s, %s);
            ''', row)
    conn.commit()



create_tables()
insert_data()