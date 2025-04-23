import pandas as pd
import numpy as np
import pyodbc
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import time
import schedule
from sqlalchemy import create_engine, text
from typing import Dict, List, Tuple, Optional, Any
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
import math
# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sector_index.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sector_index")

class DatabaseConnector:
    """Class for managing database connections and operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self._engine = None
        self._max_connection_attempts = 3
        self._connection_retry_delay = 5  # seconds
    
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
            logger.error(f"Database connection error: {str(e)}")
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
                    logger.warning(f"Query failed (attempt {attempt+1}), retrying: {str(e)}")
                    time.sleep(self._connection_retry_delay)
                else:
                    logger.error(f"Query failed after {self._max_connection_attempts} attempts: {str(e)}")
                    raise
    
    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        """Execute a query and return the result as a DataFrame with retry logic"""
        for attempt in range(self._max_connection_attempts):
            try:
                with self.engine.connect() as conn:
                    return pd.read_sql(text(query), conn)
            except Exception as e:
                if attempt < self._max_connection_attempts - 1:
                    logger.warning(f"DataFrame query failed (attempt {attempt+1}), retrying: {str(e)}")
                    time.sleep(self._connection_retry_delay)
                else:
                    logger.error(f"DataFrame query failed after {self._max_connection_attempts} attempts: {str(e)}")
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
            logger.error(f"Transaction failed: {str(e)}")
            return False

class IndexCalculator:
    """Base abstract class for index calculators"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    @abstractmethod
    def calculate(self) -> pd.DataFrame:
        """Calculate the index values"""
        pass
    
    @abstractmethod
    def store_results(self, results: pd.DataFrame) -> None:
        """Store calculated results"""
        pass


class SectorIndexCalculator(IndexCalculator):
    """Class for calculating sector-based indices"""
    
    def __init__(self, db_connector: DatabaseConnector):
        super().__init__(db_connector)
        self.current_indices = {} 
        self.prev_market_cap_data = None 
        self._sector_cache = None  
        self._sector_cache_timestamp = None  
        self._sector_cache_ttl = 3600  # Cache TTL in seconds (1 hour)
        self._indices_initialized = False  # Flag to track if indices have been initialized  # Cache TTL in seconds (1 hour)
    
    def initialize_indices(self) -> None:
        """Initialize index values from daily_index table using a stored procedure"""
        if self._indices_initialized:
            logger.debug("Indices already initialized")
            return

        try:
            logger.info("Initializing sector indices using stored procedure")

            query = "EXEC sector_index.GetLatestSectorIndices"
            indices_df = self.db.fetch_dataframe(query)

            if not indices_df.empty:
                for _, row in indices_df.iterrows():
                    sector_code = row['sector_code']
                    index_value = float(row['end_index_value'])
                    self.current_indices[sector_code] = index_value
                    logger.info(f"Initialized {sector_code} index with value {index_value:.2f}")

                logger.info(f"Initialized {len(indices_df)} sector indices from stored procedure")
            else:
                logger.warning("No previous index data found, using default values")
                for sector_code, _ in self.sector_cache().items():
                    self.current_indices[sector_code] = 100.0
                    logger.info(f"Initialized {sector_code} index with default value 100.0")

            self._indices_initialized = True
        except Exception as e:
            logger.error(f"Error initializing indices with stored procedure: {str(e)}")
            for sector_code, _ in self.sector_cache().items():
                self.current_indices[sector_code] = 100.0
            self._indices_initialized = True



    def sector_cache(self) -> Dict[str, Dict[str, Any]]:
        """Fetch sector information from the database with caching"""
        current_time = time.time()
        
        # Return cached data if valid
        if (self._sector_cache is not None and 
            self._sector_cache_timestamp is not None and 
            current_time - self._sector_cache_timestamp < self._sector_cache_ttl):
            logger.debug("Using cached sector information")
            return self._sector_cache
        
        try:
            logger.info("Loading sector information from database")
    
            # Get active sectors
            sectors_df = self.db.fetch_dataframe(
                "SELECT sector_code, sector_name FROM Sector_Information WHERE isActive = 1"
            )
            
            # Get sector-symbol mappings
            sector_symbol_df = self.db.fetch_dataframe(
                "SELECT sector_code, company FROM Sector_Symbol"
            )
            
            # Create a dictionary of sectors with their symbols
            data = {}
            for sector_code in sectors_df['sector_code'].unique():
                sector_name = sectors_df[sectors_df['sector_code'] == sector_code]['sector_name'].iloc[0]
                symbols = sector_symbol_df[sector_symbol_df['sector_code'] == sector_code]['company'].tolist()
                data[sector_code] = {
                    'name': sector_name,
                    'symbols': symbols,
                    'last_index_value': 100.0  # Initialize with base value of 100
                }
            
            # Update cache and timestamp
            self._sector_cache = data
            self._sector_cache_timestamp = current_time
            
            return data
        except Exception as e:
            logger.error(f"Error fetching sector cache: {str(e)}")
            # Return existing cache if available (even if expired) or empty dict
            return self._sector_cache if self._sector_cache is not None else {}
    
    def get_market_cap_data(self, timestamp: datetime.datetime) -> Dict[str, Dict[str, Any]]:
        """Calculate market cap data for all companies at a specific timestamp"""
        try:
            timestamp_str = timestamp
            query = f"EXEC sector_index.sp_GetMarketCapData @timestamp = '{timestamp_str}'"
            current_market_data_df = self.db.fetch_dataframe(query)
            
            current_market_data = {}
            
            for _, row in current_market_data_df.iterrows():
                company = row['company']
                ltp = row['LTP']
                ltp_dt = row['ltp_dt']
                current_market_data[company] = {
                    'ltp': ltp,
                    'timestamp': timestamp,
                    'total_shares': 0,
                    'market_cap': 0,
                    'free_float_pct': 0,
                    'free_float_mcap': 0,                  
                }
            return current_market_data
        except Exception as e:
            logger.error(f"Error calculating market cap data: {str(e)}")
            return {}
    
    def get_previous_market_cap_data(self) -> Dict[str, Dict[str, Any]]:
        """Get market cap data using stored procedure"""
        try:
            if self.prev_market_cap_data is None:
                logger.info("Fetching previous market cap data using stored procedure")

                # Call the stored procedure
                query = "EXEC sector_index.get_previous_market_cap_data"
                prev_data_df = self.db.fetch_dataframe(query)

                self.prev_market_cap_data = {}
                for _, row in prev_data_df.iterrows():
                    company = row['company']
                    self.prev_market_cap_data[company] = {
                        'ltp': row['ltp'],
                        'timestamp': row['timestamp'],
                        'total_shares': row['total_shares'],
                        'market_cap': row['market_cap'],
                        'free_float_pct': row['free_float_pct'],
                        'free_float_mcap': row['free_float_mcap']
                    }

                logger.info(f"Loaded previous market cap data for {len(self.prev_market_cap_data)} companies")
            else:
                logger.debug("Using cached previous market cap data")

            return self.prev_market_cap_data

        except Exception as e:
            logger.error(f"Error getting previous market cap data: {str(e)}")
            return {}
    
    def save_previous_market_cap_data(self) -> bool:
        """Save the current market cap data to database for use in next trading session"""
        if not self.prev_market_cap_data:
            logger.warning("No market cap data to save")
            return False

        try:
            logger.info("Saving market cap data for next trading day")

            # Start building queries with TRUNCATE first
            queries = ["TRUNCATE TABLE previous_market_cap_data"]

            for company, data in self.prev_market_cap_data.items():
                # Sanitize company name
                sanitized_company = company.replace("'", "''")

                # Format timestamp
                if isinstance(data['timestamp'], datetime.datetime):
                    timestamp_str = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    timestamp_str = str(data['timestamp'])

                # Handle NaN values - replace with NULL for SQL
                ltp = data['ltp'] if not pd.isna(data['ltp']) else 'NULL'
                total_shares = data['total_shares'] if not pd.isna(data['total_shares']) else 'NULL'
                market_cap = data['market_cap'] if not pd.isna(data['market_cap']) else 'NULL'
                free_float_pct = data['free_float_pct'] if not pd.isna(data['free_float_pct']) else 'NULL'
                free_float_mcap = data['free_float_mcap'] if not pd.isna(data['free_float_mcap']) else 'NULL'

                values_part = f"'{sanitized_company}', "
                values_part += f"{ltp}, "
                values_part += f"'{timestamp_str}', "
                values_part += f"{total_shares}, "
                values_part += f"{market_cap}, "
                values_part += f"{free_float_pct}, "
                values_part += f"{free_float_mcap}"

                insert_query = f"""
                INSERT INTO previous_market_cap_data (
                    company, 
                    ltp, 
                    timestamp, 
                    total_shares, 
                    market_cap, 
                    free_float_pct, 
                    free_float_mcap
                )
                VALUES ({values_part})
                """
                queries.append(insert_query)

            # Execute TRUNCATE + all INSERTs as one transaction
            success = self.db.execute_transaction(queries)

            if success:
                logger.info(f"Stored market cap data for {len(self.prev_market_cap_data)} companies")
            else:
                logger.error("Failed to store market cap data")

            return success
        except Exception as e:
            logger.exception(f"Error saving market cap data: {str(e)}")
            return False
    
    def calculate(self) -> pd.DataFrame:
        """Calculate sector indices based on the latest LTP data"""
        try:
            # Initialize indices if not already done
            if not self._indices_initialized:
                self.initialize_indices()
                
            # Verify DB connection before proceeding
            if not self.db.verify_connection():
                logger.error("Database connection failed during calculation")
                return pd.DataFrame()
            
            current_timestamp = datetime.datetime.now().replace(microsecond=0)
            current_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # Get market cap data for current timestamp
            current_mcap_data = self.get_market_cap_data(current_timestamp)
            
            # Ensure previous market cap data is loaded
            if self.prev_market_cap_data is None:
                self.get_previous_market_cap_data()
            
            if not current_mcap_data or not self.prev_market_cap_data:
                logger.warning("Insufficient market cap data for index calculation")
                return pd.DataFrame()
            
            # Update current market data with previous data where needed
            for company, prev_data in self.prev_market_cap_data.items():
                if company not in current_mcap_data:
                    current_mcap_data[company] = prev_data
                else:
                    current_mcap_data[company]['free_float_pct'] = prev_data['free_float_pct']
                    current_mcap_data[company]['total_shares'] = prev_data['total_shares']
                    current_mcap_data[company]['market_cap'] = current_mcap_data[company]['ltp'] * current_mcap_data[company]['total_shares']
                    current_mcap_data[company]['free_float_mcap'] = (current_mcap_data[company]['market_cap'] * current_mcap_data[company]['free_float_pct']) / 100

            # Calculate indices for each sector
            sector_results = []
            logger.info("Calculating sector indices")
            for sector_code, sector_info in self.sector_cache().items():
                try:
                    sector_symbols = sector_info['symbols']
                    
                    # Filter to only include companies in this sector with data available
                    sector_symbols_with_data = [
                        symbol for symbol in sector_symbols 
                        if symbol in current_mcap_data and symbol in self.prev_market_cap_data
                    ]
                    
                    if not sector_symbols_with_data:
                        logger.warning(f"No data available for companies in sector {sector_code}")
                        continue
                    
                    total_sector_ff_mcap = sum(
                        float(current_mcap_data[symbol]['free_float_mcap']) 
                        for symbol in sector_symbols_with_data 
                        if isinstance(current_mcap_data[symbol]['free_float_mcap'], (int, float))
                    )
                    
                    # Calculate company returns and weights
                    company_returns = []
                    
                    for company in sector_symbols_with_data:
                        curr_data = current_mcap_data[company]
                        prev_data = self.prev_market_cap_data[company]
                        
                        # Calculate company weight
                        weight = ((curr_data['free_float_mcap'] / total_sector_ff_mcap) * 100) if total_sector_ff_mcap > 0 else 0
                        
                        # Calculate price return
                        price_return = ((curr_data['free_float_mcap'] / prev_data['free_float_mcap']) - 1) if prev_data['free_float_mcap'] > 0 else 0
                        
                        # Calculate weighted return
                        weighted_return = price_return * (weight / 100)
                        
                        company_returns.append({
                            'company': company,
                            'price_return': price_return,
                            'weight': weight,
                            'weighted_return': weighted_return
                        })
                    
                    # Update previous market cap data for next calculation
                    for company in sector_symbols_with_data:
                        curr_data = current_mcap_data[company]
                        self.prev_market_cap_data[company] = {
                            'ltp': curr_data['ltp'],
                            'timestamp': current_timestamp,
                            'total_shares': curr_data['total_shares'],
                            'market_cap': curr_data['market_cap'],
                            'free_float_pct': curr_data['free_float_pct'],
                            'free_float_mcap': curr_data['free_float_mcap']
                        }
                    
                    company_returns_df = pd.DataFrame(company_returns)
                    total_sector_return = company_returns_df['weighted_return'].sum() if not company_returns_df.empty else 0
                    logger.info(f"Total sector return for {sector_code}: {total_sector_return}")
                    
                    # Get previous index value from cache, fallback to 100.0 if not found
                    previous_index = self.current_indices.get(sector_code, 100.0)
                    current_index = previous_index * (1 + total_sector_return)
                    
                    # Update current index value in cache
                    self.current_indices[sector_code] = current_index
                    
                    # Store result
                    sector_results.append({
                        'sector_code': sector_code,
                        'sector_name': sector_info['name'],
                        'timestamp': current_timestamp,
                        'previous_index': previous_index,
                        'current_index': current_index,
                        'total_return': total_sector_return,
                        'num_companies': len(company_returns)
                    })
                except Exception as e:
                    logger.error(f"Error calculating index for sector {sector_code}: {str(e)}")
            
            results_df = pd.DataFrame(sector_results)
            
            # Store results to database
            if not results_df.empty:
                self.store_results(results_df)
            
            return results_df
        except Exception as e:
            logger.exception(f"Error in index calculation: {str(e)}")
            return pd.DataFrame()
    
    def store_results(self, results: pd.DataFrame) -> None:
        """Store calculated index results to database"""
        try:
            # Verify DB connection before proceeding
            if not self.db.verify_connection():
                logger.error("Database connection failed during results storage")
                return
            
            # Use batch insert for better performance
            insert_queries = []
            index_table_name = "Sector_Index_Values"
            
            for _, row in results.iterrows():
                # Sanitize inputs to prevent SQL injection
                sector_code = row['sector_code'].replace("'", "''")
                sector_name = row['sector_name'].replace("'", "''")
                timestamp = row['timestamp']
                current_index = row['current_index']
                total_return = row['total_return']
                num_companies = row['num_companies']
                
                insert_query = f"""
                INSERT INTO {index_table_name} (sector_code, sector_name, timestamp, index_value, total_return, num_companies)
                VALUES (
                    '{sector_code}',
                    '{sector_name}',
                    '{timestamp}',
                    {current_index},
                    {total_return},
                    {num_companies}
                )
                """
                insert_queries.append(insert_query)
            
            # Execute all inserts in a single transaction
            success = self.db.execute_transaction(insert_queries)
            
            if success:
                logger.info(f"Stored {len(results)} sector index values")
            else:
                logger.error("Failed to store sector index values")
        except Exception as e:
            logger.exception(f"Error storing results: {str(e)}")

class MarketIndexService:
    """Main service class that orchestrates the index calculation pipeline"""
    
    def __init__(self, db_config: Dict[str, str], 
                 trading_start_time: datetime.time = datetime.time(10, 30),
                 trading_end_time: datetime.time = datetime.time(16, 30),
                 weekend_days: List[int] = [5, 6]):  # Default 5=Saturday, 6=Sunday
        self.db_connector = DatabaseConnector(db_config)
        self.index_calculator = SectorIndexCalculator(self.db_connector)
        self.trading_day_processed = False
        self.trading_day_date = datetime.datetime.now().date()
        
        # Configurable trading parameters
        self.trading_start_time = trading_start_time
        self.trading_end_time = trading_end_time
        self.weekend_days = weekend_days
        self.end_window_minutes = 10  # Minutes after trading end to process EOD data  # Minutes after trading end to process EOD data
        
    def is_trading_hours(self) -> bool:
        """Check if current time is within trading hours"""
        now = datetime.datetime.now()
        current_date = now.date()
        weekday = now.weekday()
        
        # Check if weekend
        if weekday in self.weekend_days:
            return False
            
        # Reset the day tracking if we're on a new day
        if current_date != self.trading_day_date:
            self.trading_day_date = current_date
            self.trading_day_processed = False
            
        return self.trading_start_time <= now.time() <= self.trading_end_time
    
    def is_day_end(self) -> bool:
        """Check if we're at the end of trading day but haven't processed EOD data yet"""
        now = datetime.datetime.now()
        
        # Calculate end window time
        end_minutes = self.trading_end_time.hour * 60 + self.trading_end_time.minute
        window_minutes = end_minutes + self.end_window_minutes
        window_hour = window_minutes // 60
        window_minute = window_minutes % 60
        end_window = datetime.time(window_hour, window_minute)
        
        return (self.trading_end_time < now.time() <= end_window) and not self.trading_day_processed
        
    def save_daily_index_data(self) -> None:
        """Call stored procedure to save end-of-day index values"""
        try:
            logger.info("Calling stored procedure to save daily index data")

            # Execute stored procedure
            query = "EXEC sector_index.save_daily_index_data"
            success = self.db_connector.execute_non_query(query)

            if success:
                logger.info("Stored daily index values via stored procedure")

                # Save market cap data for next day
                market_cap_saved = self.index_calculator.save_previous_market_cap_data()
                if market_cap_saved:
                    logger.info("Successfully saved market cap data for next trading day")

                self.trading_day_processed = True
            else:
                logger.error("Failed to store daily index values via stored procedure")

        except Exception as e:
            logger.exception(f"Error saving daily index data: {str(e)}")

    
    def calculate_indices(self) -> None:
        """Calculate indices and log results"""
        try:
            logger.info("Starting sector index calculation")
            results = self.index_calculator.calculate()
            
            if not results.empty:
                logger.info(f"Successfully calculated indices for {len(results)} sectors")
                
                # Log summary of results
                for _, row in results.iterrows():
                    logger.info(f"Sector {row['sector_code']}: {row['current_index']:.2f} ({row['total_return']*100:.2f}%)")
            else:
                logger.warning("No index results were calculated")
        except Exception as e:
            logger.exception(f"Error in sector index calculation: {str(e)}")
    
    def run(self) -> None:
        """Run index calculation if in trading hours, or daily summary if at end of day"""
        if self.is_trading_hours():
            self.calculate_indices()
        elif self.is_day_end():
            logger.info("Trading day ended - saving daily summary")
            self.save_daily_index_data()
        else:
            logger.info("Outside trading hours - skipping processing")
    
    def run_scheduled(self, index_interval_minutes: int = 1) -> None:
        """Schedule execution of index calculation"""
        logger.info(f"Scheduling index calculation every {index_interval_minutes} minute(s)")
        
        # Initialize previous market cap data
        self.index_calculator.get_previous_market_cap_data()
        
        # Initialize sector indices from daily_index table
        self.index_calculator.initialize_indices()
        
        # Run immediately once if in trading hours
        self.run()
        
        # Schedule index calculation (every minute by default)
        schedule.every(index_interval_minutes).minutes.do(self.run)
        
        # Run the scheduled tasks
        while True:
            schedule.run_pending()
            time.sleep(1)
# Create the previous_market_cap_data table if it doesn't exist
def create_required_tables(db_connector):
    try:
        logger.info("Calling stored procedure to ensure required tables exist")

        query = "EXEC sector_index.create_required_tables"
        db_connector.execute_query(query)

        logger.info("Tables created or verified via stored procedure")
        return True
    except Exception as e:
        logger.error(f"Error creating required tables: {str(e)}")
        return False

    
# Get database configuration from environment variables
def get_db_config_from_env():
    return {
        'server': os.getenv('DB_SERVER'),
        'database': os.getenv('DB_NAME'),
        'driver': '{ODBC Driver 17 for SQL Server}',
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
        'use_windows_auth': False
    }

# Main entry point
def main():
    # Load database configuration from environment variables
    db_config = get_db_config_from_env()
    
    # Validate DB configuration
    if not all([db_config['server'], db_config['database'], db_config['username'], db_config['password']]):
        logger.error("Missing database configuration. Please check your .env file.")
        logger.error(f"Server: {db_config['server']}, Database: {db_config['database']}")
        exit(1)
    
    logger.info(f"Connecting to database server: {db_config['server']}, database: {db_config['database']}")
    
    # Create database connector for initial operations
    db_connector = DatabaseConnector(db_config)
    
    # Ensure required tables exist
    if not create_required_tables(db_connector):
        logger.error("Failed to create required tables. Exiting.")
        exit(1)
    
    # Set trading hours and weekend days (configurable)
    trading_start = datetime.time(10, 30)  # 10:30 AM
    trading_end = datetime.time(14, 31)    # 2:31 PM (adjust as needed)
    weekend_days = [4, 5]                  # 5=Saturday, 4=Friday
    
    # Create and start the market index service with configured trading parameters
    service = MarketIndexService(
        db_config,
        trading_start_time=trading_start,
        trading_end_time=trading_end,
        weekend_days=weekend_days
    )
    
    # Run with default scheduling (indices every 1 minute)
    service.run_scheduled()

if __name__ == "__main__":
    main()
