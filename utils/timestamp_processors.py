

from datetime import datetime, timedelta
from typing import List

import pandas as pd
from schedule import logger
from config.log_config import Logger
from database.connector import DatabaseConnector
from config.trading_hour_config import TradingHourConfig

class TimeStampProcessor:
    """Utility class for date-related operations"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.trading_hour_config = TradingHourConfig(db_connector)
    
    def get_trading_timestamps(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Generate timestamps for trading days with proper holiday handling"""
        try:
            # Query database for holidays (a better approach than hardcoding)
            print("Fetching holidays from database...")
            holiday_query = """
            select holiday_date from holidays
            WHERE holiday_date BETWEEN :start_date AND :end_date
            """
            params = {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
            print("Executing query:", holiday_query, "with params:", params)
            holidays_df = self.db.fetch_dataframe(holiday_query, params)
            
            # Convert to set of date objects for faster lookup
            manual_holidays = set()
            if not holidays_df.empty and 'holiday_date' in holidays_df.columns:
                manual_holidays = set(pd.to_datetime(holidays_df['holiday_date']).dt.date)
            
            
            
            timestamps = []
            current_day = start_date.date()
            
            while current_day <= end_date.date():
              
                if current_day not in manual_holidays:
                    # Generate timestamps for trading hours
                    
                    start_time ,end_time= self.trading_hour_config._load_trading_hours_from_db(current_day)
                    start_dt = datetime.combine(current_day, start_time)
                    end_dt = datetime.combine(current_day, end_time)
                    # Generate timestamps at 1-minute intervals
                    daily_range = pd.date_range(start=start_dt, end=end_dt, freq="1min")
                    timestamps.extend(daily_range)
                else:
                    self.logger.info(f"Skipping holiday: {current_day}")
                current_day += timedelta(days=1)

            if not timestamps:
                self.logger.warning(f"No trading days found between {start_date.date()} and {end_date.date()}")
            
            self.logger.info(f"Generated {len(timestamps)} trading timestamps from {start_date.date()} to {end_date.date()}")
            return timestamps
            
        except Exception as e:
            self.logger.error(f"Error generating trading timestamps: {str(e)}")
            return []