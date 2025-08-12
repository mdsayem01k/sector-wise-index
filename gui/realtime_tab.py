import tkinter as tk
import queue
import threading
from gui.log_widget import LogWidget
from config.log_config import Logger
from config.database_config import database_config
from database.connector import DatabaseConnector
from services.realtime_index_service import RealIndexService

class RealtimeFrame(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg="#f0f0f0")  
        self.logger = Logger.get_logger(self.__class__.__name__)
        
        # Initialize service
        db_config = database_config.get_db_config_from_env()
        self.db_connector = DatabaseConnector(db_config)
        self.service = RealIndexService(db_config)
        
        # Threading control
        self.scheduler_thread = None
        self.is_running = False
        
        # UI Setup
        tk.Label(self, text="Realtime Index Calculation", font=("Arial", 14), bg="#f0f0f0").pack(pady=10)

        # Frame to hold buttons side by side
        button_frame = tk.Frame(self, bg="#f0f0f0")
        button_frame.pack(pady=5)

        self.run_button = tk.Button(button_frame, text="Run Scheduler", command=self.run_scheduler)
        self.run_button.pack(side='left', padx=10)

        self.stop_button = tk.Button(button_frame, text="Stop Scheduler", command=self.stop_scheduler, state='disabled')
        self.stop_button.pack(side='left', padx=10)

        # Status indicator
        self.status_label = tk.Label(self, text="Status: Stopped", font=("Arial", 10), bg="#f0f0f0", fg="red")
        self.status_label.pack(pady=5)

        self.log_widget = LogWidget(self)
        self.log_widget.pack(fill='both', expand=True)
        self.log_widget.log("Realtime interface loaded.")

        # Handle window close event
        self.bind_all('<Destroy>', self.on_destroy)

    def run_scheduler(self):
        """Start the scheduler in a separate thread"""
        if self.is_running:
            self.log_widget.log("Scheduler is already running.")
            return
            
        try:
            self.is_running = True
            self.run_button.config(state='disabled')
            self.stop_button.config(state='normal')
            self.status_label.config(text="Status: Starting...", fg="orange")
            
            # Start scheduler in separate thread to prevent GUI blocking
            self.scheduler_thread = threading.Thread(target=self._run_scheduler_thread, daemon=True)
            self.scheduler_thread.start()
            
            self.log_widget.log("Scheduler starting...")
            
        except Exception as e:
            self.logger.error(f"Error starting scheduler: {str(e)}")
            self.log_widget.log(f"Error starting scheduler: {str(e)}")
            self._reset_ui_state()

    def _run_scheduler_thread(self):
        """Run the scheduler in a separate thread"""
        try:
            self.status_label.config(text="Status: Running", fg="green")
            self.log_widget.log("Scheduler started successfully.")
            
            # This will run the infinite loop in the background
            self.service.run_scheduled()
            
        except Exception as e:
            self.logger.error(f"Scheduler error: {str(e)}")
            self.log_widget.log(f"Scheduler error: {str(e)}")
        finally:
            # Reset UI state when scheduler stops
            self.after(0, self._reset_ui_state)  # Use after() to update UI from main thread

    def stop_scheduler(self):
        """Stop the scheduler gracefully"""
        if not self.is_running:
            self.log_widget.log("Scheduler is not running.")
            return
            
        try:
            self.status_label.config(text="Status: Stopping...", fg="orange")
            self.log_widget.log("Stopping scheduler...")
            
            # Signal the service to stop
            self.service.stop_scheduler()
            
            # Wait for thread to complete (with timeout)
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5.0)
                
            self.log_widget.log("Scheduler stopped successfully.")
            
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {str(e)}")
            self.log_widget.log(f"Error stopping scheduler: {str(e)}")
        finally:
            self._reset_ui_state()

    def _reset_ui_state(self):
        """Reset UI to initial state"""
        self.is_running = False
        self.run_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self.status_label.config(text="Status: Stopped", fg="red")

    def on_destroy(self, event=None):
        """Handle window close event"""
        if self.is_running:
            self.log_widget.log("Window closing - stopping scheduler...")
            self.stop_scheduler()