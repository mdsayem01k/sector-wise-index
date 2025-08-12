

from datetime import datetime
import logging
from pydoc import text
from typing import Dict
import pandas as pd


from database.connector import DatabaseConnector
from config.log_config import Logger

class IndexProcessor:
    """Base class for index processors"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.logger = Logger.get_logger(self.__class__.__name__)
    #for historical
    def get_latest_sector_indices(self, source_type: int = 1, start_date: datetime = None, current_indices: dict = None, sector_cache_func=None) -> Dict[str, float]:
        """Fetch sector index values (historical or real-time)
        
        Args:
            source_type: 0 for historical, 1 for real-time
            start_date: Required for historical data
            current_indices: Dictionary to store the initialized indices (for real-time)
            sector_cache_func: Function to get sector cache data (for real-time)
            
        Returns:
            Dict[str, float]: Dictionary of sector codes and their index values (for historical)
            bool: True/False for success (for real-time)
        """
        try:
            if source_type == 0:
                # Historical
                if start_date is None:
                    self.logger.error("start_date is required for historical data")
                    return {}
                    
                count = self.db.execute_query("SELECT COUNT(*) FROM Historical_Sector_Index_Values")
                if not count or count[0][0] == 0:
                    self.logger.info("Historical data empty. Using BASE_INDEX.")
                    return {}

                latest_ts_df = self.db.fetch_dataframe(
                    "SELECT MAX(timestamp) as latest_timestamp FROM Historical_Sector_Index_Values WHERE timestamp < :start_date",
                    {'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S')}
                )
                if latest_ts_df.empty or pd.isna(latest_ts_df.at[0, 'latest_timestamp']):
                    self.logger.info("No historical index found. Using BASE_INDEX.")
                    return {}

                latest_ts = latest_ts_df.at[0, 'latest_timestamp']
                index_df = self.db.fetch_dataframe(
                    "SELECT sector_code, Cindex_value FROM Historical_Sector_Index_Values WHERE timestamp = :timestamp",
                    {'timestamp': latest_ts.strftime('%Y-%m-%d %H:%M:%S')}
                )
                if index_df.empty:
                    self.logger.info("No index values found for latest timestamp. Using BASE_INDEX.")
                    return {}

                return {row['sector_code']: float(row['Cindex_value']) for _, row in index_df.iterrows()}

            elif source_type == 1:
                # Real-time
                if current_indices is None or sector_cache_func is None:
                    self.logger.error("current_indices and sector_cache_func are required for real-time data")
                    return False
                    
                try:
                    self.logger.info("Initializing sector indices using stored procedure")

                    query = "EXEC sector_index.GetLatestSectorIndices"
                    indices_df = self.db.fetch_dataframe(query)

                    if not indices_df.empty:
                        for _, row in indices_df.iterrows():
                            sector_code = row['sector_code']
                            index_value = float(row['end_index_value'])
                            current_indices[sector_code] = index_value
                            self.logger.info(f"Initialized {sector_code} index with value {index_value:.2f}")

                        self.logger.info(f"Initialized {len(indices_df)} sector indices from stored procedure")
                        print(f"Initialized {len(indices_df)} sector indices from stored procedure")
                        return True
                    else:
                        self.logger.warning("No previous index data found, using default values")
                        for sector_code, _ in sector_cache_func().items():
                            current_indices[sector_code] = 100.0
                            self.logger.info(f"Initialized {sector_code} index with default value 100.0")
                        return True

                except Exception as e:
                    self.logger.error(f"Error initializing indices with stored procedure: {str(e)}")
                    # Fallback to default values
                    try:
                        for sector_code, _ in sector_cache_func().items():
                            current_indices[sector_code] = 100.0
                            self.logger.info(f"Fallback: Initialized {sector_code} index with default value 100.0")
                        return True
                    except Exception as fallback_error:
                        self.logger.error(f"Error in fallback initialization: {str(fallback_error)}")
                        return False
                        
            else:
                self.logger.error("Invalid source_type. Use 0 for historical, 1 for real-time.")
                return {} if source_type == 0 else False

        except Exception as e:
            self.logger.error(f"Error in get_latest_sector_indices: {e}")
            return {} if source_type == 0 else False
            
    def initialize_indices(self, current_indices: dict, sector_cache_func) -> bool:
        """Legacy method for backward compatibility - calls get_latest_sector_indices with real-time mode"""
        return self.get_latest_sector_indices(
            source_type=1, 
            current_indices=current_indices, 
            sector_cache_func=sector_cache_func
        )
 

    def summarize_historical_index_results(self,results: pd.DataFrame):
        self.logger.info(f"Historical index calculation completed with {len(results)} data points")
        # print("results", len(results), type(results))
        # results.to_csv('historical_sector_indices.csv', index=False)

        sector_summary = results.groupby('sector_code').agg({
            'sector_name': 'first',
            'previous_index': 'first',
            'current_index': ['last', 'max', 'min'],
            'total_return': 'sum'
        }).reset_index()

        sector_summary.columns = [
            'sector_code', 'sector_name', 'previous_index',
            'current_index', 'max_index', 'min_index', 'total_period_return'
        ]

        sector_summary['period_return_pct'] = (
            (sector_summary['current_index'] / sector_summary['previous_index']) - 1
        ) * 100

        sector_summary['start_index'] = sector_summary['previous_index']
        sector_summary['end_index'] = sector_summary['current_index']

        self.logger.info("Summary of historical index calculation:")
        for _, row in sector_summary.iterrows():
            self.logger.info(
                f"Sector {row['sector_code']} ({row['sector_name']}): "
                f"Start: {row['start_index']:.2f}, End: {row['end_index']:.2f}, "
                f"Return: {row['period_return_pct']:.2f}%"
            )

    def save_daily_index_data(self) -> None:
        """Call stored procedure to save end-of-day index values"""
        try:
            self.logger.info("Calling stored procedure to save daily index data")
            
            # Check DB health first
            if not self._check_db_health():
                self.logger.error("Database unhealthy, skipping daily index save")
                return

            # Execute stored procedure with timeout
            with self.db_connector.engine.begin() as conn:
                conn.execute(text("EXEC sector_index.save_daily_index_data"))
                self.logger.info("Stored daily index values via stored procedure")

                # Save market cap data for next day
                market_cap_saved = self.index_calculator.save_previous_market_cap_data()
                if market_cap_saved:
                    self.logger.info("Successfully saved market cap data for next trading day")
                
                self.trading_day_processed = True

        except Exception as e:
            self.logger.exception(f"Error saving daily index data: {str(e)}")