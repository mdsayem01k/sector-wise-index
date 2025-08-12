from typing import Any, Dict, List
import time as time_module
import pandas as pd
from config.log_config import Logger
from sqlalchemy import create_engine, text


class DatabaseConnector:
    """Class for managing database connections and operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self._engine = None
        self._max_connection_attempts = 3
        self._connection_retry_delay = 5  # seconds
        self.logger = Logger.get_logger(self.__class__.__name__)
    
    @property
    def engine(self):
        """Lazy initialization of database engine"""
        if self._engine is None:
            conn_str = self._get_connection_string()
            connection_url = f"mssql+pyodbc:///?odbc_connect={conn_str}"
            self._engine = create_engine(
                connection_url, 
                pool_recycle=1800,  # Recycle connections after 30 minutes
                pool_pre_ping=True,  # Check connections before use
                pool_size=5,  # Limit pool size
                max_overflow=10,  # Maximum number of overflow connections
                connect_args={"timeout": 30}  # Connection timeout in seconds
            )
        return self._engine
    
    def _get_connection_string(self) -> str:
        """Create connection string from config"""
        if self.config.get('use_windows_auth', False):
            return f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};Trusted_Connection=yes"
        else:
            return f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};UID={self.config['username']};PWD={self.config['password']}"
    
    def verify_connection(self) -> bool:
        """Verify database connection is working"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1")).fetchone()
                return True
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> Any:
        """Execute a query and return the result with retry logic"""
        for attempt in range(self._max_connection_attempts):
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(query))
                    if result.returns_rows:
                        # Fetch all results to detach from connection
                        rows = result.fetchall()
                        return rows
                    return result
            except Exception as e:
                if attempt < self._max_connection_attempts - 1:
                    self.logger.warning(f"Query failed (attempt {attempt+1}), retrying: {str(e)}")
                    time_module.sleep(self._connection_retry_delay)
                else:
                    self.logger.error(f"Query failed after {self._max_connection_attempts} attempts: {str(e)}")
                    raise
    
    def fetch_dataframe(self, query: str, params=None) -> pd.DataFrame:
        """Execute a query and return the result as a DataFrame with retry logic"""
        for attempt in range(self._max_connection_attempts):
            try:
                with self.engine.connect() as conn:
                    if params:
                        query_with_params = text(query).bindparams(**params)
                        return pd.read_sql(query_with_params, conn)
                    else:
                        return pd.read_sql(text(query), conn)
            except Exception as e:
                if attempt < self._max_connection_attempts - 1:
                    self.logger.warning(f"DataFrame query failed (attempt {attempt+1}), retrying: {str(e)}")
                    time_module.sleep(self._connection_retry_delay)
                else:
                    self.logger.error(f"DataFrame query failed after {self._max_connection_attempts} attempts: {str(e)}")
                    # Return empty dataframe on failure
                    return pd.DataFrame()
    
    def execute_transaction(self, queries: List[str]) -> bool:
        """Execute multiple queries in a transaction with proper error handling"""
        try:
            with self.engine.begin() as conn:
                for query in queries:
                    conn.execute(text(query))
            return True
        except Exception as e:
            self.logger.error(f"Transaction failed: {str(e)}")
            return False
    def delete_execute_query(self, query: str) -> dict:
        """Execute a DELETE query with retry logic"""
        for attempt in range(self._max_connection_attempts):
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(query))
                    rows_affected = result.rowcount
                    conn.commit()  # Make sure the transaction is committed
                    return {
                        'success': True,
                        'rows_affected': rows_affected,
                        'message': f"Successfully deleted {rows_affected} row(s)"
                    }
            except Exception as e:
                if attempt < self._max_connection_attempts - 1:
                    self.logger.warning(f"Delete query failed (attempt {attempt+1}), retrying: {str(e)}")
                    time_module.sleep(self._connection_retry_delay)
                else:
                    error_msg = f"Delete query failed after {self._max_connection_attempts} attempts: {str(e)}"
                    self.logger.error(error_msg)
                    return {
                        'success': False,
                        'rows_affected': 0,
                        'message': error_msg
                    }
    def check_db_health(self) -> bool:
        """Public method to check database health"""
        try:
            start_time = time_module.time()
            is_healthy = self.verify_connection()
            duration = time_module.time() - start_time

            if duration > 10:
                self.logger.warning(f"Database health check took {duration:.2f} seconds")

            return is_healthy
        except Exception as e:
            self.logger.error(f"Database health check failed: {str(e)}")
            return False
        