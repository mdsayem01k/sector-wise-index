

from datetime import datetime, time
from config.log_config import Logger


class TradingDayEnd:
    def __init__(self, trading_start_time: datetime.time, trading_end_time: datetime.time, 
                 end_window_minutes: int = 30,trading_day_processed: bool = False):   
       
        self.logger = Logger.get_logger(self.__class__.__name__)
              
        self.end_window_minutes = end_window_minutes
        self.trading_day_processed = trading_day_processed
        self.trading_start_time = trading_start_time
        self.trading_end_time = trading_end_time

    def is_day_end(self) -> bool:
        now = datetime.now()

        end_minutes = self.trading_end_time.hour * 60 + self.trading_end_time.minute
        window_minutes = end_minutes + self.end_window_minutes
        window_hour = window_minutes // 60
        window_minute = window_minutes % 60
        end_window = time(window_hour, window_minute)

        return (self.trading_end_time < now.time() <= end_window) and not self.trading_day_processed