import chromadb
from tqdm import tqdm
import psycopg2.extras
from chromadb.utils import embedding_functions
import os
from chromadb.config import Settings
import psycopg2

import json

with open('config.json') as f:
    config = json.load(f)

# connect to database movies

import requests

# get rating of imdb movie


def get_rating(imdb_id):

    url = "https://imdb8.p.rapidapi.com/title/get-ratings"
    querystring = {"tconst": imdb_id}
    headers = {
        'X-RapidAPI-Key': config['IMDB_API_KEY'],
        'X-RapidAPI-Host': 'imdb8.p.rapidapi.com'
    }

    response = requests.request(
        "GET", url, headers=headers, params=querystring)

    return response.json()['rating']


def get_connection():
    conn = psycopg2.connect(
        host=config['DB_HOST'],
        database="movies",
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        port=5432
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cur


client = chromadb.PersistentClient('./chroma')
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    model_name="text-embedding-ada-002",
    api_key=config['OPEN_API_KEY'],
)
collection = client.get_or_create_collection(name="movies")


conn, cur = get_connection()

cur.execute('''
    SELECT * FROM movies;
''')

movies = cur.fetchall()

progress_bar = tqdm(total=len(movies), unit="movies")


for movie in movies:

    genre = movie['genres'].replace('|', ', ')

    # get average rating
    cur.execute('''
        SELECT AVG(rating) as avg_rating FROM ratings WHERE movieid = %s;
    ''', (movie['movieid'],))

    avg_rating = cur.fetchone()['avg_rating']

    # get 10 tags
    cur.execute('''
        SELECT tag, count(*) as total FROM tags WHERE movieid = %s group by tag ;
    ''', (movie['movieid'],))

    tags = cur.fetchall()
    tag_string = ""
    for tag in tags:
        tag_string += (tag['tag'] + " ")*tag['total']

    amount_of_tags = len(tags)

    tag_string = tag_string.strip()

    cur.execute('''
        SELECT count(*) as total FROM tags WHERE movieid = %s;
    ''', (movie['movieid'],))

    amount_of_tags = cur.fetchone()['total']

    # get imdb rating

    cur.execute('''
        SELECT imdbid FROM links WHERE movieid = %s;
    ''', (movie['movieid'],))

    # try:
    #     imdb_id = str(cur.fetchone()['imdbid'])

    #     imdb_id = imdb_id.zfill(7)

    #     imdb_rating = get_rating('tt' + str(imdb_id))
    # except:
    #     imdb_rating = None

    # if imdb_rating:

    # movie_document = f'The movie {movie["title"]} is associated with {tag_string}. This movie is mentioned {amount_of_tags} times on the web and is catalogued with the following genre list: {genre}. Also, has a average rating of {avg_rating}. Finally, imdb rating is {imdb_rating}.'
    # else:

    # movie_document = f'The movie {movie["title"]} is associated with {tag_string}. This movie is mentioned {amount_of_tags} times on the web and is catalogued with the following genre list: {genre}. Also, has a average rating of {avg_rating}.'

    # movie_document = f'The movie "{movie["title"]}" is associated with {tag_string}. This movie appears {amount_of_tags} times on the web and is often catalogued as an {genre}.'
    avg_rating = avg_rating if avg_rating else 0
    movie_document = f'{tag_string} {genre} {movie["title"]} {avg_rating}'

    collection.add(ids=[str(movie["movieid"])], documents=[
                   movie_document])
    progress_bar.update(1)
