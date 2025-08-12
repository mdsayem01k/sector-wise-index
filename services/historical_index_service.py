


from datetime import datetime
from typing import Dict

import pandas as pd
from calculators.historical_sector_calculator import HistoricalSectorIndexCalculator
from database import connector
from database.connector import DatabaseConnector
from services.data_export_service import DataExportService
from utils.index_data_processors import IndexProcessor
from config.log_config import Logger    

class HistoricalIndexService:
    """Service class that manages historical index calculation"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_connector = DatabaseConnector(db_config)
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.index_calculator = HistoricalSectorIndexCalculator(self.db_connector)
         
    
    def calculate_historical_indices(self, start_date_str: str, end_date_str: str) -> pd.DataFrame:
        """Calculate historical indices for the specified date range"""
        try:
            self.logger.info(f"Starting historical index calculation from {start_date_str} to {end_date_str}")
            
            # Parse dates
            start_date = start_date_str
            end_date = end_date_str
            # Calculate indices
            results = self.index_calculator.calculate(start_date, end_date)
            
            print("results", len(results))
            print("results", type(results), len(results))

            if isinstance(results, pd.DataFrame) and not results.empty:
                processor = IndexProcessor(self.db_connector)
                processor.summarize_historical_index_results(results)
            else:
                self.logger.warning("No historical index results were calculated")


                    
            return results
        except Exception as e:
            self.logger.exception(f"Error in historical index calculation service: {str(e)}")
            return pd.DataFrame()
    
    