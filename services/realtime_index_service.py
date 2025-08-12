from datetime import datetime, time, timedelta
import time as time_module
import signal
import threading
from typing import Dict, List

from calculators.realtime_sector_calculator import RealTimeSectorCalculator
from config.log_config import Logger
from config.trading_hour_config import TradingHourConfig
from database.connector import DatabaseConnector
from utils.mcap_data_processors import MarketCapDataProcessor
from utils.index_data_processors import IndexProcessor
from config.day_end_config import TradingDayEnd

class RealIndexService:
    """Main service class that orchestrates the index calculation pipeline"""
    
    def __init__(self, db_config: Dict[str, str], 
                 trading_start_time: time = time(10, 0),
                 trading_end_time: time = time(14, 31)):
        self.db_connector = DatabaseConnector(db_config)
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.trading_day_processed = False
        self.end_window_minutes = 10
        self.trading_day_date = datetime.now().date()
        self.trading_config = TradingHourConfig(self.db_connector)
        self.market_cap_processor = MarketCapDataProcessor(self.db_connector)
        self.index_processor = IndexProcessor(self.db_connector)
        self.trading_day_end = TradingDayEnd(trading_start_time, trading_end_time, self.end_window_minutes, self.trading_day_date)
        self.index_calculator = RealTimeSectorCalculator(self.db_connector)

        self.trading_start_time = trading_start_time
        self.trading_end_time = trading_end_time
        self.end_window_minutes = 10
        
        self.last_calculation_time = None
        self.calculation_errors = 0
        self.max_consecutive_errors = 5
        self.db_connection_timeout = 30
        self.calculation_timeout = 120
        
        self.calculation_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
    
    def calculate_indices(self) -> bool:
        calculation_start_time = time_module.time()
        current_time = datetime.now()

        if self.last_calculation_time:
            time_since_last = (current_time - self.last_calculation_time).total_seconds()
            if time_since_last < 30:
                self.logger.debug(f"Skipping calculation - only {time_since_last:.1f}s since last calculation")
                return True
        
        if not self.calculation_lock.acquire(blocking=False):
            self.logger.warning("Calculation already in progress, skipping this cycle")
            return False
        
        try:
            self.logger.info(f"Starting sector index calculation at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if not self.db_connector.check_db_health():
                self.logger.error("Database unhealthy, skipping calculation")
                self.calculation_errors += 1
                return False
            
            calculation_thread = threading.Thread(target=self._perform_calculation)
            calculation_thread.daemon = True
            calculation_thread.start()
            calculation_thread.join(timeout=self.calculation_timeout)
            
            if calculation_thread.is_alive():
                self.logger.error(f"Calculation timed out after {self.calculation_timeout} seconds")
                self.calculation_errors += 1
                return False
            
            calculation_duration = time_module.time() - calculation_start_time
            self.logger.info(f"Index calculation completed in {calculation_duration:.2f} seconds")
            
            self.calculation_errors = 0
            self.last_calculation_time = current_time
            return True
            
        except Exception as e:
            self.logger.exception(f"Error in sector index calculation: {str(e)}")
            self.calculation_errors += 1
            return False
        finally:
            self.calculation_lock.release()
    
    def _perform_calculation(self):
        try:
            results = self.index_calculator.calculate()
            
            if not results.empty:
                self.logger.info(f"Successfully calculated indices for {len(results)} sectors")
                for _, row in results.iterrows():
                    self.logger.info(f"Sector {row['sector_code']}: {row['current_index']:.2f} ({row['total_return']*100:.2f}%)")
            else:
                self.logger.warning("No index results were calculated")
        except Exception as e:
            self.logger.exception(f"Error in calculation thread: {str(e)}")
            raise
    
    def run(self) -> None:
        current_time = datetime.now()
        
        if self.calculation_errors >= self.max_consecutive_errors:
            self.logger.error(f"Too many consecutive errors ({self.calculation_errors}), waiting before retry")
            time_module.sleep(60)
            self.calculation_errors = max(0, self.calculation_errors - 1)
            return
        
        if self.trading_config.is_trading_hours():
            self.logger.debug(f"In trading hours - attempting calculation at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            success = self.calculate_indices()
            if not success:
                self.logger.warning(f"Calculation failed at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        elif self.trading_day_end.is_day_end():
            self.logger.info("Trading day ended - saving daily summary")
            self.market_cap_processor.save_daily_index_data()
        else:
            self.logger.debug(f"Outside trading hours at {current_time.strftime('%Y-%m-%d %H:%M:%S')} - skipping processing")
    
    def run_scheduled(self, index_interval_minutes: int = 1) -> None:
        self.logger.info(f"Scheduling index calculation every {index_interval_minutes} minute(s)")
        
        try:
            max_init_attempts = 3
            for attempt in range(max_init_attempts):
                try:
                    self.logger.info(f"Initializing data (attempt {attempt + 1}/{max_init_attempts})")
                    self.market_cap_processor.get_previous_market_cap_data()
                    self.index_processor.get_latest_sector_indices(source_type=1)
                    self.logger.info("Data initialization completed successfully")
                    break
                except Exception as e:
                    self.logger.error(f"Data initialization failed (attempt {attempt + 1}): {str(e)}")
                    if attempt == max_init_attempts - 1:
                        self.logger.error("All initialization attempts failed")
                        raise
                    time_module.sleep(10)
            
            if self.trading_config.is_trading_hours():
                self.run()
            
            next_run_time = datetime.now().replace(second=0, microsecond=0)
            next_run_time += timedelta(minutes=index_interval_minutes)
            
            self.logger.info(f"Next scheduled run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            while not self.shutdown_event.is_set():
                try:
                    current_time = datetime.now()
                    
                    if current_time >= next_run_time:
                        time_diff = (current_time - next_run_time).total_seconds()
                        if abs(time_diff) > 5:
                            self.logger.warning(f"Scheduling drift detected: {time_diff:.1f} seconds")
                        
                        self.run()
                        
                        next_run_time = current_time.replace(second=0, microsecond=0)
                        next_run_time += timedelta(minutes=index_interval_minutes)
                        
                        self.logger.debug(f"Next run scheduled for: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    time_module.sleep(1)
                    
                except KeyboardInterrupt:
                    self.logger.info("Received shutdown signal (Ctrl+C)")
                    break
                except Exception as e:
                    self.logger.exception(f"Error in scheduled execution: {str(e)}")
                    time_module.sleep(5)
            
            self.logger.info("Performing graceful shutdown - saving daily index data...")
            try:
                if self.trading_config.is_trading_hours() or self.trading_day_end.is_day_end():
                    self.market_cap_processor.save_daily_index_data()
                    self.logger.info("Daily index data saved successfully")
            except Exception as e:
                self.logger.error(f"Error saving daily index data during shutdown: {str(e)}")
            
            self.logger.info("Shutdown complete")
        
        except Exception as e:
            self.logger.exception(f"Fatal error in scheduler: {str(e)}")
            raise
 

    def stop_scheduler(self):
        """Gracefully stop the scheduler and save final data"""
        self.logger.info("Stopping scheduler - initiating graceful shutdown...")
        
        # Signal the main loop to stop
        self.shutdown_event.set()
        
        # Wait for any ongoing calculations to complete
        if self.calculation_lock.locked():
            self.logger.info("Waiting for current calculation to complete...")
            max_wait_time = 30  # seconds
            wait_start = time_module.time()
            
            while self.calculation_lock.locked() and (time_module.time() - wait_start) < max_wait_time:
                time_module.sleep(0.1)
            
            if self.calculation_lock.locked():
                self.logger.warning("Calculation did not complete within timeout, forcing shutdown")
        
        try:
            # Save daily index data if we're in trading hours or at day end
            if self.trading_config.is_trading_hours() or self.trading_day_end.is_day_end():
                self.logger.info("Saving daily index data before shutdown...")
                self.market_cap_processor.save_daily_index_data()
                self.logger.info("Daily index data saved successfully")
            else:
                self.logger.info("Outside trading hours - no daily data to save")
                
        except Exception as e:
            self.logger.error(f"Error saving daily index data during shutdown: {str(e)}")
        
        # Reset error counter
        self.calculation_errors = 0
        
        self.logger.info("Scheduler shutdown complete")