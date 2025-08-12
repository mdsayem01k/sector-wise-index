from datetime import datetime
import threading
from typing import Any, Dict
import pandas as pd
import schedule
import time

from calculators.base_calculator import IndexCalculator
from config.log_config import Logger
from config.trading_hour_config import TradingHourConfig
from database.connector import DatabaseConnector
from utils.cache_processors import CacheProcessor
from utils.index_data_processors import IndexProcessor
from utils.mcap_data_processors import MarketCapDataProcessor
from utils.timestamp_processors import TimeStampProcessor
from services.data_export_service import DataExportService


class RealTimeSectorCalculator(IndexCalculator):
    """Real-time sector index calculator that runs in a separate thread"""
    def __init__(self, db_connector: DatabaseConnector):
        super().__init__(db_connector)
        self.cache_processor = CacheProcessor(self.db)
        self.index_processor = IndexProcessor(self.db)
        self.market_cap_processor = MarketCapDataProcessor(self.db)
        self.timestamp_processor = TimeStampProcessor(self.db)
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.trading_config = TradingHourConfig(self.db)
        self.data_export_service = DataExportService()
        self.running = False
        self.thread = None
        self.log_callback = None
        self.last_trading_day = None
        self.daily_initialization_done = False
        self._indices_initialized = False
        self.current_indices = {} 
        self._sector_cache_ttl = 3600
        self._sector_cache_timestamp = None
        self.prev_market_cap_data = None 
        self._max_query_retries = 3
        self._sector_cache = None 

    def sector_cache(self) -> Dict[str, Dict[str, Any]]:
        """Fetch sector information from the database with caching"""
        current_time = time.time()
        
        # Return cached data if valid
        if (self._sector_cache is not None and 
            self._sector_cache_timestamp is not None and 
            current_time - self._sector_cache_timestamp < self._sector_cache_ttl):
            self.logger.debug("Using cached sector information")
            return self._sector_cache
        
        try:
            self.logger.info("Loading sector information from database")
    
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
                }
            
            # Update cache and timestamp
            self._sector_cache = data
            self._sector_cache_timestamp = current_time
            
            return data
        except Exception as e:
            self.logger.error(f"Error fetching sector cache: {str(e)}")
            # Return existing cache if available (even if expired) or empty dict
            return self._sector_cache if self._sector_cache is not None else {}
           
    def initialize_indices(self) -> None:
        """Initialize index values using the IndexInitializer class"""
        if self._indices_initialized:
            self.logger.debug("Indices already initialized")
            return

        # Use the new get_latest_sector_indices method with real-time mode
        success = self.index_processor.get_latest_sector_indices(
            source_type=1,  # Real-time mode
            current_indices=self.current_indices,
            sector_cache_func=self.sector_cache
        )
        
        if success:
            self._indices_initialized = True
        else:
            self.logger.error("Failed to initialize indices")
            # Set initialization flag to True anyway to prevent infinite retry
            self._indices_initialized = True
    
    def get_previous_market_cap_data(self) -> Dict[str, Dict[str, Any]]:
        """Get market cap data using stored procedure with improved error handling"""
        try:
            if self.prev_market_cap_data is None:
                self.logger.info("Fetching previous market cap data using stored procedure")

                # Retry logic for stored procedure call
                for attempt in range(self._max_query_retries):
                    try:
                        query = "EXEC sector_index.get_previous_market_cap_data"
                        start_time = time.time()
                        prev_data_df = self.db.fetch_dataframe(query)
                        query_duration = time.time() - start_time
                        
                        if query_duration > 15:  # Log slow queries
                            self.logger.warning(f"Previous market cap data query took {query_duration:.2f} seconds")
                        
                        if prev_data_df.empty:
                            if attempt == self._max_query_retries - 1:
                                self.logger.error("No previous market cap data available after all attempts")
                                self.prev_market_cap_data = {}
                                return {}
                            self.logger.warning(f"No data returned, retrying (attempt {attempt + 1})")
                            time.sleep(self._query_retry_delay)
                            continue
                        
                        # Process the data
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

                        self.logger.info(f"Loaded previous market cap data for {len(self.prev_market_cap_data)} companies")
                        
                        # Save to CSV for debugging
                        if self.prev_market_cap_data:
                            prev_data_df_to_save = pd.DataFrame.from_dict(self.prev_market_cap_data, orient='index')
                            prev_data_df_to_save.to_csv('previous_market_cap_data.csv', index_label='company')
                            self.logger.debug("Saved previous market cap data to CSV")
                        
                        break  # Success, exit retry loop
                        
                    except Exception as e:
                        self.logger.error(f"Error in stored procedure call (attempt {attempt + 1}): {str(e)}")
                        if attempt == self._max_query_retries - 1:
                            self.prev_market_cap_data = {}
                            return {}
                        time.sleep(self._query_retry_delay * (attempt + 1))
            else:
                self.logger.debug("Using cached previous market cap data")

            return self.prev_market_cap_data

        except Exception as e:
            self.logger.exception(f"Error getting previous market cap data: {str(e)}")
            self.prev_market_cap_data = {}
            return {}
    

    def calculate(self) -> pd.DataFrame:
        """Calculate sector indices with improved timing and error handling"""
        calculation_start = time.time()
        
        try:
            # Initialize indices if not already done
            if not self._indices_initialized:
                self.initialize_indices()
                
            # Verify DB connection before proceeding
            db_check_start = time.time()
            if not self.db.verify_connection():
                self.logger.error("Database connection failed during calculation")
                return pd.DataFrame()
            db_check_duration = time.time() - db_check_start
            
            if db_check_duration > 5:
                self.logger.warning(f"Database connection check took {db_check_duration:.2f} seconds")
            
            # Use precise timestamp (rounded to minute)
            current_timestamp = datetime.now().replace(second=0, microsecond=0)
            current_timestamp_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"Calculating indices for timestamp: {current_timestamp_str}")
            
            # Get market cap data for current timestamp with timing
            mcap_fetch_start = time.time()
            current_mcap_data = self.market_cap_processor.get_market_cap_data(current_timestamp, mode='real')

            mcap_fetch_duration = time.time() - mcap_fetch_start
            
            if mcap_fetch_duration > 20:
                self.logger.warning(f"Market cap data fetch took {mcap_fetch_duration:.2f} seconds")
            
            # Ensure previous market cap data is loaded
            if self.prev_market_cap_data is None:
                prev_data_start = time.time()
                self.get_previous_market_cap_data()
                prev_data_duration = time.time() - prev_data_start
                self.logger.info(f"Loaded previous market cap data in {prev_data_duration:.2f} seconds")

            # Check if prev_market_cap_data is None or empty
            if not self.prev_market_cap_data:
                self.logger.warning("Insufficient previous market cap data for index calculation")
                return pd.DataFrame()
                
            # Use previous data if no current data available
            if not current_mcap_data:
                self.logger.warning("No current market cap data, using previous data")
                current_mcap_data = self.prev_market_cap_data.copy()
                # Update timestamps
                for company in current_mcap_data:
                    current_mcap_data[company]['timestamp'] = current_timestamp_str
            
            # Update current market data with previous data where needed
            data_merge_start = time.time()
            for company, prev_data in self.prev_market_cap_data.items():
                if company not in current_mcap_data:
                    current_mcap_data[company] = prev_data.copy()
                    current_mcap_data[company]['timestamp'] = current_timestamp_str
                else:
                    # Merge data
                    current_mcap_data[company]['free_float_pct'] = prev_data['free_float_pct']
                    current_mcap_data[company]['total_shares'] = prev_data['total_shares']
                    
                    # Validate LTP data
                    if pd.isna(current_mcap_data[company]['ltp']) or current_mcap_data[company]['ltp'] <= 0:
                        self.logger.warning(f"Invalid LTP for {company}, using previous LTP")
                        current_mcap_data[company]['ltp'] = prev_data['ltp']
                    
                    # Calculate market cap and free float
                    current_mcap_data[company]['market_cap'] = (
                        current_mcap_data[company]['ltp'] * current_mcap_data[company]['total_shares']
                    )
                    current_mcap_data[company]['free_float_mcap'] = (
                        (current_mcap_data[company]['market_cap'] * current_mcap_data[company]['free_float_pct']) / 100
                    )
            
            data_merge_duration = time.time() - data_merge_start
            if data_merge_duration > 5:
                self.logger.warning(f"Data merge took {data_merge_duration:.2f} seconds")
            
            # Save preprocessed market cap data to excel
            try:
                self.data_export_service.save_market_cap_to_excel(current_mcap_data)
            except Exception as e:
                self.logger.warning(f"Failed to save market cap data to Excel: {str(e)}")
            
            # Calculate indices for each sector
            sector_results = []
            sector_calc_start = time.time()
            
            self.logger.info("Calculating sector indices")
            for sector_code, sector_info in self.sector_cache().items():
                try:
                    sector_symbols = sector_info['symbols']
                    
                    # Filter to only include companies in this sector with data available
                    sector_symbols_with_data = [
                        symbol for symbol in sector_symbols 
                        if (symbol in current_mcap_data and 
                            symbol in self.prev_market_cap_data and
                            current_mcap_data[symbol]['free_float_mcap'] > 0 and
                            self.prev_market_cap_data[symbol]['free_float_mcap'] > 0)
                    ]
                    
                    if not sector_symbols_with_data:
                        self.logger.warning(f"No valid data available for companies in sector {sector_code}")
                        continue
                    
                    # Calculate total sector free float market cap
                    total_sector_ff_mcap = sum(
                        float(current_mcap_data[symbol]['free_float_mcap']) 
                        for symbol in sector_symbols_with_data 
                        if isinstance(current_mcap_data[symbol]['free_float_mcap'], (int, float)) and
                        current_mcap_data[symbol]['free_float_mcap'] > 0
                    )
                    
                    if total_sector_ff_mcap <= 0:
                        self.logger.warning(f"Total sector free float market cap is zero for {sector_code}")
                        continue
                    
                    self.logger.debug(f"Sector {sector_code}: {len(sector_symbols_with_data)} companies, "
                               f"Total FF Market Cap: {total_sector_ff_mcap:,.0f}")
                    
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
                            'timestamp': current_timestamp_str,
                            'total_shares': curr_data['total_shares'],
                            'market_cap': curr_data['market_cap'],
                            'free_float_pct': curr_data['free_float_pct'],
                            'free_float_mcap': curr_data['free_float_mcap']
                        }
                    
                    company_returns_df = pd.DataFrame(company_returns)
                    total_sector_return = company_returns_df['weighted_return'].sum() if not company_returns_df.empty else 0
                    
                    # Get previous index value from cache, fallback to 100.0 if not found
                    previous_index = self.current_indices.get(sector_code, 100.0)
                    current_index = previous_index * (1 + total_sector_return)
                    
                    # Update current index value in cache
                    self.current_indices[sector_code] = current_index
                    
                    # Store result
                    sector_results.append({
                        'sector_code': sector_code,
                        'sector_name': sector_info['name'],
                        'timestamp': current_timestamp_str,
                        'previous_index': previous_index,
                        'current_index': current_index,
                        'total_return': total_sector_return,
                        'num_companies': len(company_returns)
                    })
                    
                    self.logger.debug(f"Sector {sector_code}: Index {current_index:.2f} ({total_sector_return*100:.2f}%)")
                    
                except Exception as e:
                    self.logger.error(f"Error calculating index for sector {sector_code}: {str(e)}")
            
            sector_calc_duration = time.time() - sector_calc_start
            self.logger.info(f"Sector calculations completed in {sector_calc_duration:.2f} seconds")
            
            results_df = pd.DataFrame(sector_results)
            
            # Store results to database
            if not results_df.empty:
                store_start = time.time()
                self.store_results(results_df)
                store_duration = time.time() - store_start
                if store_duration > 10:
                    self.logger.warning(f"Database storage took {store_duration:.2f} seconds")
            
            total_duration = time.time() - calculation_start
            self.logger.info(f"Total calculation completed in {total_duration:.2f} seconds for {len(results_df)} sectors")
            
            return results_df
            
        except Exception as e:
            calculation_duration = time.time() - calculation_start
            self.logger.exception(f"Error in index calculation after {calculation_duration:.2f} seconds: {str(e)}")
            return pd.DataFrame()
        

    def store_results(self, results: pd.DataFrame) -> None:
        """Store calculated index results to database"""
        try:
            # Verify DB connection before proceeding
            if not self.db.verify_connection():
                self.logger.error("Database connection failed during results storage")
                return
            
            # Use batch insert for better performance
            insert_queries = []
            index_table_name = "Sector_Index_Values"
            
            for _, row in results.iterrows():
                # Sanitize inputs to prevent SQL injection
                sector_code = row['sector_code'].replace("'", "''")
                sector_name = row['sector_name'].replace("'", "''")
                timestamp = row['timestamp']
                previous_index = row['previous_index']
                current_index = row['current_index']
                total_return = row['total_return']
                num_companies = row['num_companies']
                
                insert_query = f"""
                INSERT INTO {index_table_name} (sector_code, sector_name, timestamp, Pindex_value,Cindex_value, total_return, num_companies)
                VALUES (
                    '{sector_code}',
                    '{sector_name}',
                    '{timestamp}',
                    {previous_index},
                    {current_index},
                    {total_return},
                    {num_companies}
                )
                """
                insert_queries.append(insert_query)
            
            # Execute all inserts in a single transaction
            success = self.db.execute_transaction(insert_queries)
            
            if success:
                self.logger.info(f"Stored {len(results)} sector index values")
            else:
                self.logger.error("Failed to store sector index values")
        except Exception as e:
            self.logger.exception(f"Error storing results: {str(e)}")
