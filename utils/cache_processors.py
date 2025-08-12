

import time
from typing import Any, Dict
from schedule import logger

from database.connector import DatabaseConnector


class CacheProcessor:
    """Base class for cache processors"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.BASE_INDEX = 100.0  
        self._sector_cache = None
        self._sector_cache_timestamp = None
        self._sector_cache_ttl = 3600

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
                    'last_index_value': self.BASE_INDEX  # Initialize with base value
                }
            
            # Update cache and timestamp
            self._sector_cache = data
            self._sector_cache_timestamp = current_time
            
            return data
        except Exception as e:
            logger.error(f"Error fetching sector cache: {str(e)}")
            # Return existing cache if available (even if expired) or empty dict
            return self._sector_cache if self._sector_cache is not None else {}