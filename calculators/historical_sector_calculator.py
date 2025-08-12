from datetime import datetime, timedelta
import os
import sys
import time as time_module

from typing import Any, Dict, List
from openpyxl import load_workbook
import pandas as pd
from schedule import logger
from calculators.base_calculator import IndexCalculator
from config.log_config import Logger
from database.connector import DatabaseConnector
from utils.cache_processors import CacheProcessor
from utils.index_data_processors import IndexProcessor
from utils.mcap_data_processors import MarketCapDataProcessor
from utils.timestamp_processors import TimeStampProcessor
from services.data_export_service import DataExportService

class HistoricalSectorIndexCalculator(IndexCalculator):
    """Class for calculating historical sector-based indices"""
    
    def __init__(self, db_connector: DatabaseConnector):
        super().__init__(db_connector)
        self.cache_processor = CacheProcessor(self.db)
        self.index_processor = IndexProcessor(self.db)
        self.market_cap_processor = MarketCapDataProcessor(self.db)
        self.timestamp_processor = TimeStampProcessor(self.db)
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.current_indices = {}
        self.BASE_INDEX = 100.0
        self.prev_market_cap_data = None

    
        
                 
    def calculate(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Calculate historical sector indices from start_date to end_date"""
        try:
            # Verify DB connection before proceeding
            if not self.db.verify_connection():
                logger.error("Database connection failed during calculation")
                return pd.DataFrame()
            
            # Get latest index values from database to use as starting points
            latest_indices = self.index_processor.get_lastest_sector_indices(source_type=0, start_date=start_date)

            
            # Initialize current indices with latest values from DB or default to BASE_INDEX
            sectors = self.cache_processor.sector_cache()
            for sector_code in sectors:
                self.current_indices[sector_code] = latest_indices.get(sector_code, self.BASE_INDEX)
                logger.info(f"Using base index for {sector_code}: {self.current_indices[sector_code]}")
            
            # Initialize previous market cap data before using it
            
            self.prev_market_cap_data = self.market_cap_processor.get_previous_market_cap_data(source_type=0, start_date=start_date)
            if not self.prev_market_cap_data:
                logger.error("Failed to get previous market cap data")
                return pd.DataFrame()
            
            # Get trading timestamps
            timestamps = self.timestamp_processor.get_trading_timestamps(start_date, end_date)
            if not timestamps:
                logger.warning("No trading data found in the specified date range")
                return pd.DataFrame()
            
            # Get market cap data for all timestamps
            market_cap_df = self.market_cap_processor.get_market_cap_data(timestamps, mode='hist')
            self.logger.info(f"Retrieved market cap data for {len(market_cap_df)} records across: ")
            if market_cap_df.empty:
                self.logger.error("Failed to get market cap data")
                return pd.DataFrame()
            
            # Calculate indices for each timestamp
            sector_results = []
            all_company_returns = []
            for i in range(1, len(timestamps)):
                prev_timestamp = timestamps[i-1]
                curr_timestamp = timestamps[i]
                self.logger.info(f"Calculating index for timestamp {curr_timestamp} (previous: {prev_timestamp})")

                # Filter market cap data for current timestamp
                curr_mcap_data = market_cap_df[
                    (market_cap_df['timestamp'] <= curr_timestamp) & 
                    (market_cap_df['timestamp'] >= prev_timestamp)
                ]

                self.logger.info(f"Filtered market cap data for timestamp {curr_timestamp}: {len(curr_mcap_data)} records")
                
                if curr_mcap_data.empty:
                    self.logger.warning(f"No market cap data available for timestamp {curr_timestamp}")
                    continue
                
                # Convert curr_mcap_data DataFrame to dictionary for easier access
                curr_mcap_dict = {}
                
                # First, populate curr_mcap_dict with current market cap data
                for _, row in curr_mcap_data.iterrows():
                    company = row['company']    
                    curr_mcap_dict[company] = {
                        'ltp': row['ltp'],
                        'timestamp': row['timestamp'],
                        'total_shares': row['total_shares'],
                        'market_cap': row['market_cap'],
                        'free_float_pct': row['free_float_pct'],
                        'free_float_mcap': row['free_float_mcap']
                    }
                
                # Fill in missing companies with previous data
                for company, prev_data in self.prev_market_cap_data.items():
                    if company not in curr_mcap_dict:
                        curr_mcap_dict[company] = prev_data
                        curr_mcap_dict[company]['timestamp'] = curr_timestamp
                    else:
                        # If current LTP is invalid, use previous data
                       if curr_mcap_dict[company]['ltp'] is None or curr_mcap_dict[company]['ltp'] <= 0:
                            curr_mcap_dict[company]['ltp'] = prev_data['ltp']
                            curr_mcap_dict[company]['free_float_pct'] = prev_data['free_float_pct']
                            curr_mcap_dict[company]['total_shares'] = prev_data['total_shares']
                            curr_mcap_dict[company]['market_cap'] = curr_mcap_dict[company]['ltp'] * curr_mcap_dict[company]['total_shares']
                            curr_mcap_dict[company]['free_float_mcap'] = (curr_mcap_dict[company]['market_cap'] * curr_mcap_dict[company]['free_float_pct']) / 100
                # save preprocess market cap data to excel
                exporter = DataExportService()
                exporter.save_market_cap_to_excel(curr_mcap_dict)

                # Calculate indices for each sector
                
                for sector_code, sector_info in sectors.items():
                    try:
                        sector_symbols = sector_info['symbols']
                        
                        # Filter to only include companies in this sector with data available
                        sector_symbols_with_data = [
                            symbol for symbol in sector_symbols 
                            if symbol in curr_mcap_dict and symbol in self.prev_market_cap_data
                        ]
                        
                        if not sector_symbols_with_data:
                            self.logger.warning(f"No data available for companies in sector {sector_code}")
                            continue
                        
                        # Calculate total sector free float market cap
                        total_sector_ff_mcap = 0
                        for symbol in sector_symbols_with_data:
                            ff_mcap = curr_mcap_dict[symbol]['free_float_mcap']
                            if isinstance(ff_mcap, (int, float)) and ff_mcap > 0:
                                total_sector_ff_mcap += ff_mcap
                        
                        if total_sector_ff_mcap <= 0:
                            self.logger.warning(f"Total sector free float market cap is zero or negative for sector {sector_code}")
                            continue
                        
                        # Calculate company returns and weightsRetrieved market
                        company_returns = []
                        
                        for company in sector_symbols_with_data:
                            curr_data = curr_mcap_dict[company]
                            prev_data = self.prev_market_cap_data[company]

                            if curr_data['ltp'] <= 0 or prev_data['free_float_mcap'] <= 0 or curr_data['free_float_mcap'] <= 0  or curr_data['total_shares'] <= 0 :
                                self.logger.warning(f"Invalid data for company {company}: LTP={curr_data['ltp']}, Previous FF MCAP={prev_data['free_float_mcap']}")
                                sys.exit("Execution stopped due to invalid market data.")

                            # Calculate company weight
                            weight = (curr_data['free_float_mcap'] / total_sector_ff_mcap)

                            # Calculate price return
                            price_return = (curr_data['free_float_mcap'] / prev_data['free_float_mcap']) - 1

                            # Calculate weighted return
                            weighted_return = price_return * weight
                            
                            company_returns.append({
                                'company': company,
                                'price_return': price_return,
                                'weight': weight,
                                'weighted_return': weighted_return
                            })
                        
                        if not company_returns:
                            self.logger.warning(f"No valid company returns calculated for sector {sector_code}")
                            continue
                        if company_returns:
                            all_company_returns.extend(company_returns)  
                        company_returns_df = pd.DataFrame(company_returns)
                        total_sector_return = company_returns_df['weighted_return'].sum()
                        self.logger.info(f"Total sector return for {sector_code}: {total_sector_return}")
                        
                        # Get previous index value from cache, fallback to BASE_INDEX if not found
                        previous_index = self.current_indices.get(sector_code, self.BASE_INDEX)
                        current_index = previous_index * (1 + total_sector_return)
                        
                        # Update current index value in cache
                        self.current_indices[sector_code] = current_index
                        
                        # Store result
                        sector_results.append({
                            'sector_code': sector_code,
                            'sector_name': sector_info['name'],
                            'timestamp': curr_timestamp,
                            'previous_index': previous_index,
                            'current_index': current_index,
                            'total_return': total_sector_return,
                            'num_companies': len(company_returns)
                        })
                    except Exception as e:
                        self.logger.error(f"Error calculating index for sector {sector_code}: {str(e)}")
                
                # Update previous market cap data for next calculation
                for company, curr_data in curr_mcap_dict.items():
                    if company in self.prev_market_cap_data:
                        self.prev_market_cap_data[company] = curr_data
            if all_company_returns:
                pd.DataFrame(all_company_returns).to_csv('company_returns.csv', index=False)
                self.logger.info(f"Saved {len(all_company_returns)} company returns to company_returns.csv")
            
            results_df = pd.DataFrame(sector_results)
            
            # Store results to database
            if not results_df.empty:
                self.store_results(results_df)
                market_cap_df.to_csv('market_cap_data.csv', index=False)
                self.logger.info(f"Successfully calculated and stored {len(results_df)} index values")
            else:
                self.logger.warning("No sector index results were calculated")
            
            return results_df
        except Exception as e:
            self.logger.exception(f"Error in historical index calculation: {str(e)}")
            return pd.DataFrame()
    
    def store_results(self, results: pd.DataFrame) -> None:
        """Store calculated historical index results to database"""
        try:
            if not self.db.verify_connection():
                self.logger.error("Database connection failed during results storage")
                return

            if results.empty:
                self.logger.warning("No results to store")
                return

            insert_queries = []
            successful_inserts = 0

            for _, row in results.iterrows():
                try:
                    sector_code = row['sector_code']
                    sector_name = row['sector_name']
                    timestamp = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

                    previous_index = float(row['previous_index'])
                    current_index = float(row['current_index'])
                    total_return = float(row['total_return'])
                    num_companies = int(row['num_companies'])

                    insert_query = f"""
                    INSERT INTO Historical_Sector_Index_Values
                    (sector_code, sector_name, timestamp, Pindex_value, Cindex_value,total_return, num_companies)
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
                    successful_inserts += 1

                except Exception as row_error:
                    self.logger.error(f"Failed to insert row for sector {row.get('sector_code', 'unknown')}: {str(row_error)}")

            if insert_queries:
                self.db.execute_transaction(insert_queries)

            self.logger.info(f"Successfully stored {successful_inserts} out of {len(results)} historical sector index values")

        except Exception as e:
            self.logger.exception(f"Error storing historical results: {str(e)}")
