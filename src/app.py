from chatbot import MovieChatBot
from dotenv import load_dotenv
import os

#Import all environment variables
load_dotenv()
HF_TOKEN = os.getenv("HF_TOKEN")
MAX_TOKENS = os.getenv("MAX_TOKENS")
MODEL = os.getenv("MODEL")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
URI = os.getenv("URI")
DB_NAME = os.getenv("DB_NAME")



if __name__ == "__main__":
    
    ##########
    #MAIN APP#
    ##########
    
    db_connection = {
    "url": URI,
    "username": USER,
    "password": PASSWORD,
    "database": DB_NAME
    }
    
    chat = MovieChatBot(HF_TOKEN, db_connection, MODEL, MAX_TOKENS)
    chat.chat()