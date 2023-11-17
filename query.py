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



results = collection.query(
    query_texts=["Recommend me movies where Leonardo Dicrapio is the author"],
    n_results=5
)

print(results)
