import tkinter as tk
from tkinter import messagebox
from datetime import datetime
from gui.log_widget import LogWidget
from config.log_config import Logger
import subprocess
import threading
import tkinter as tk
from tkcalendar import DateEntry
from config.database_config import database_config
from services.historical_index_service import HistoricalIndexService

class HistoricalFrame(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg="#f0f0f0")
        # NOW initialize logger for this class
        self.logger = Logger.get_logger(self.__class__.__name__)
        
        # Initialize service (make sure it also uses Logger.get_logger())
        db_config = database_config.get_db_config_from_env()
        self.service = HistoricalIndexService(db_config)
        
        
        # Create the UI elements first
        tk.Label(self, text="Historical Index Calculation", font=("Arial", 14), bg="#f0f0f0").pack(pady=10)

        # Start Date & Time Frame
        start_frame = tk.Frame(self, bg="#f0f0f0")
        start_frame.pack(pady=5)

        tk.Label(start_frame, text="Start Date", bg="#f0f0f0").grid(row=0, column=0, padx=5)
        self.start_date_entry = DateEntry(start_frame, date_pattern='yyyy-mm-dd')
        self.start_date_entry.grid(row=0, column=1, padx=5)

        # End Date & Time Frame
        end_frame = tk.Frame(self, bg="#f0f0f0")
        end_frame.pack(pady=5)

        tk.Label(end_frame, text="End Date", bg="#f0f0f0").grid(row=0, column=0, padx=5)
        self.end_date_entry = DateEntry(end_frame, date_pattern='yyyy-mm-dd')
        self.end_date_entry.grid(row=0, column=1, padx=5)

        self.run_button = tk.Button(self, text="Run Historical Calculation", command=self.run_historical_calculation)
        self.run_button.pack(pady=10)

        # Create log widget FIRST
        self.log_widget = LogWidget(self)
        self.log_widget.pack(fill='both', expand=True)
        
        # Set up GUI logging BEFORE creating any loggers
        Logger.set_gui_handler(self.log_widget)
        
        # Test the logger immediately
        self.logger.info("Historical tab loaded successfully!")
        self.logger.debug("Debug message test")
        self.logger.warning("Warning message test")

    def run_historical_calculation(self):
        try:
            start_date_str = self.start_date_entry.get()
            end_date_str = self.end_date_entry.get()

            start_dt = datetime.strptime(f"{start_date_str} 00:00:00", "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(f"{end_date_str} 00:00:00", "%Y-%m-%d %H:%M:%S")

            if start_dt > end_dt:
                self.logger.error("Start datetime must be before end datetime.")
                messagebox.showerror("Error", "Start datetime must be before end datetime.")
                return

            self.logger.info(f"Running historical index calculation from {start_dt} to {end_dt}...")

            # Disable button during execution
            self.run_button.config(state='disabled')
            
            thread = threading.Thread(target=self.execute_hist_main, args=(start_dt, end_dt))
            thread.daemon = True  # Dies with main thread
            thread.start()

        except Exception as e:
            self.logger.error(f"Error in run_historical_calculation: {e}")
            messagebox.showerror("Error", str(e))

    def execute_hist_main(self, start_date, end_date):
        try:
            self.logger.info("Executing calculation directly in-process...")
            
            # Add some progress logging
            self.logger.info("Connecting to database...")
            self.logger.info("Fetching historical data...")
            
            results = self.service.calculate_historical_indices(start_date, end_date)
            
            self.logger.info("Historical index calculation completed successfully!")
            
            # Re-enable button on main thread
            self.after(0, lambda: self.run_button.config(state='normal'))

        except Exception as e:
            self.logger.error(f"Execution error: {e}")
            # Re-enable button on main thread
            self.after(0, lambda: self.run_button.config(state='normal'))