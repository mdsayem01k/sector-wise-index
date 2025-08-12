
import os
from dotenv import load_dotenv


load_dotenv()

class database_config:
    def __init__(self, db_name, user, password, host='localhost', port=5432):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def get_db_config_from_env():
        """Fetch database configuration from environment variables."""
        return {
            'server': os.getenv('DB_SERVER'),
            'database': os.getenv('DB_NAME'),
            'driver': '{ODBC Driver 17 for SQL Server}',
            'username': os.getenv('DB_USERNAME'),
            'password': os.getenv('DB_PASSWORD'),
            'use_windows_auth': False
        }