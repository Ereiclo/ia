import chromadb
from tqdm import tqdm
import psycopg2.extras
from chromadb.utils import embedding_functions
import os
from chromadb.config import Settings
import psycopg2

import json

from flask_cors import CORS
from flask import Flask, jsonify, request

with open('config.json') as f:
    config = json.load(f)

# connect to database movies
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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

@app.post("/api/query")
def api_query():
    # get json from request body
    json_body = request.json

    # get query text
    query_text = json_body['query']

    results = collection.query(
        # query_texts=["Give me movies that talk about drugs"],
        query_texts=query_text,
        n_results=5
    )

    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True)

# print(results)
