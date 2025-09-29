from dotenv import load_dotenv
import os
from langchain_huggingface import HuggingFaceEmbeddings
from neo4jdb import Neo4jDB



load_dotenv()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
URI = os.getenv("URI")
DB_NAME = os.getenv("DB_NAME")

#Files with cleaned data for movie database
DATA_PATH = '../../data/clean/'
files_names = {
    'movies': DATA_PATH + 'clean_movies_metadata.csv',
    'actors': DATA_PATH + 'actors.csv',
    'directors': DATA_PATH + 'directors.csv',
    'crew': DATA_PATH + 'crew.csv',
    'genres': DATA_PATH + 'movie_genres.csv',
    'keywords': DATA_PATH + 'keywords.csv',
    'collections': DATA_PATH + 'movie_collections.csv',
    'production_companies': DATA_PATH + 'movie_production_companies.csv',
    'production_countries': DATA_PATH + 'movie_production_countries.csv',
    'spoken_languages': DATA_PATH + 'movie_spoken_languages.csv',
}

if __name__ == "__main__":
    
    #############################
    #Creating database if needed#
    #############################
    embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
    db = Neo4jDB(URI, USER, PASSWORD, DB_NAME, files_names)
    db.create_db()
    db.create_embeddings("Movie", "overview", embedding_model, True)
    db.create_embedding_index("Movie", "overview_embedding", "OVERVIEW_INDEX", 768)
    db.create_node_info("Movie", "movie_info", embedding_model, update=True)
    db.create_embedding_index("Movie", "movie_info_embedding", "MOVIE_INFO_INDEX", 768)
    print("Database created successfully")