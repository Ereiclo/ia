import chromadb
from tqdm import tqdm
import psycopg2.extras
from chromadb.utils import embedding_functions
import os
from chromadb.config import Settings
import psycopg2


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


client = chromadb.PersistentClient('./chroma')
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    model_name="text-embedding-ada-002",
    api_key=os.environ.get("OPENAI_API_KEY"),
)
collection = client.get_or_create_collection(name="movies")


conn, cur = get_connection()

cur.execute('''
    SELECT * FROM movies;
''')

movies = cur.fetchall()

progress_bar = tqdm(total=len(movies), unit="movies")


for movie in movies:

    genre = movie['genres'].replace('|', ' ')

    # get average rating
    cur.execute('''
        SELECT AVG(rating) as avg_rating FROM ratings WHERE movieid = %s;
    ''', (movie['movieid'],))

    avg_rating = cur.fetchone()['avg_rating']

    # get 10 tags
    cur.execute('''
        SELECT distinct tag FROM tags WHERE movieid = %s;
    ''', (movie['movieid'],))

    tags = cur.fetchall()
    tag_string = ""
    for tag in tags:
        tag_string += tag['tag'] + " "

    tag_string = tag_string.strip()

    movie_document = movie["title"] + " " + \
        genre + " " + str(avg_rating) + " " + tag_string
    

    collection.add(ids=[str(movie["movieid"])], documents=[movie_document])
    progress_bar.update(1)
