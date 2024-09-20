from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase

load_dotenv()

def init_database(user: str, password: str, host: str, port: str, database: str) -> SQLDatabase:
    db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return SQLDatabase.from_uri(db_uri)
