from datetime import datetime, time as dtime, date
from config.log_config import Logger

class TradingHourConfig:
    """
    Configuration class for trading hours.
    Reads trading hours from the database if available; otherwise, uses default values.
    Caches daily trading hours and checks whether current time is within the range.
    """

    def __init__(self, db_connector):
        self.db = db_connector
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.cached_trading_hours = None
        self.cached_day = None

        # Default hours
        self.default_start_time = dtime(10, 0)
        self.default_end_time = dtime(14, 31)

    def _load_trading_hours_from_db(self,day_str):
        """Fetch trading hours from the database"""
        try:
           
            query = """
                SELECT TOP 1 start_time, end_time 
                FROM set_trading_hour 
                WHERE trading_day = :trading_day
            """
            result = self.db.fetch_dataframe(query, {'trading_day': day_str})

            if not result.empty:
                start = result.at[0, 'start_time']
                end = result.at[0, 'end_time']
                self.logger.info(f"Loaded trading hours from DB: {start} to {end}")
                return start, end
            else:
                self.logger.warning(f"No trading hours found for {day_str}. Using default hours.")
                return self.default_start_time, self.default_end_time
        except Exception as e:
            self.logger.error(f"Error fetching trading hours: {e}")

        # Fallback to default
        self.logger.warning("Using default trading hours.")
        return self.default_start_time, self.default_end_time

    def _refresh_trading_hours(self):
        """Refresh the trading hour cache if the day has changed"""
        today = date.today()
        if self.cached_day != today:
            self.cached_day = today
            self.cached_trading_hours = self._load_trading_hours_from_db(today)

    def is_trading_hours(self) -> bool:
        """Check if current time is within trading hours"""
        self._refresh_trading_hours()
        now = datetime.now().time()
        start, end = self.cached_trading_hours
        return start <= now <= end
