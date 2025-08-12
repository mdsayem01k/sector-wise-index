import tkinter as tk
from tkinter import messagebox
from datetime import datetime, timedelta
from config.database_config import database_config
from config.log_config import Logger
from services.historical_index_service import HistoricalIndexService
from tkcalendar import DateEntry
from gui.log_widget import LogWidget
from database.connector import DatabaseConnector

class TradingHourSetupFrame(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg="#f0f0f0")
       
        
        # Title
        tk.Label(self, text="Trading Hour Setup", font=("Arial", 14, "bold"), bg="#f0f0f0").pack(pady=10)

        # Main horizontal frame
        main_frame = tk.Frame(self, bg="#f0f0f0")
        main_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Set minimum size for this frame
        self.pack_propagate(False)  # Don't let children control frame size
        self.config(width=1200, height=800)  # Set fixed frame size

        # Configure grid layout - left side much wider than right side
        main_frame.columnconfigure(0, weight=3)  # Left side gets 3/4 of space
        main_frame.columnconfigure(1, weight=1)  # Right side gets 1/4 of space

        # Left side: Trading Hour Configuration
        left_frame = tk.LabelFrame(main_frame, text="Trading Hour Configuration", font=("Arial", 12, "bold"), 
                                  bg="#f0f0f0", fg="#333333", padx=20, pady=20)
        left_frame.grid(row=0, column=0, sticky='nsew', padx=(0, 10))

        # Right side: Log Container
        right_frame = tk.LabelFrame(main_frame, text="Log Container", font=("Arial", 12, "bold"), 
                                   bg="#f0f0f0", fg="#333333", padx=20, pady=20)
        right_frame.grid(row=0, column=1, sticky='nsew', padx=(10, 0))

        # === LEFT FRAME CONTENT ===
        # Form inputs section
        form_frame = tk.Frame(left_frame, bg="#f0f0f0")
        form_frame.pack(fill='x', pady=(0, 10))

        # Date selection frame
        date_frame = tk.Frame(form_frame, bg="#f0f0f0")
        date_frame.pack(fill='x', pady=5)

        tk.Label(date_frame, text="From Date:", bg="#f0f0f0", font=("Arial", 10)).grid(row=0, column=0, sticky='w', padx=(0, 5))
        self.from_entry = DateEntry(date_frame, date_pattern='yyyy-mm-dd', width=12)
        self.from_entry.grid(row=0, column=1, padx=(0, 15), sticky='w')

        tk.Label(date_frame, text="To Date:", bg="#f0f0f0", font=("Arial", 10)).grid(row=0, column=2, sticky='w', padx=(0, 5))
        self.to_entry = DateEntry(date_frame, date_pattern='yyyy-mm-dd', width=12)
        self.to_entry.grid(row=0, column=3, sticky='w')

        # Time selection frame
        time_frame = tk.Frame(form_frame, bg="#f0f0f0")
        time_frame.pack(fill='x', pady=5)

        tk.Label(time_frame, text="Start Time:", bg="#f0f0f0", font=("Arial", 10)).grid(row=0, column=0, sticky='w', padx=(0, 5))
        self.start_time_entry = tk.Entry(time_frame, width=15)
        self.start_time_entry.grid(row=0, column=1, padx=(0, 15), sticky='w')
        self.start_time_entry.insert(0, "09:00")  # Default value

        tk.Label(time_frame, text="End Time:", bg="#f0f0f0", font=("Arial", 10)).grid(row=0, column=2, sticky='w', padx=(0, 5))
        self.end_time_entry = tk.Entry(time_frame, width=15)
        self.end_time_entry.grid(row=0, column=3, sticky='w')
        self.end_time_entry.insert(0, "17:00")  # Default value

        # Time format hints
        hint_frame = tk.Frame(form_frame, bg="#f0f0f0")
        hint_frame.pack(fill='x', pady=(0, 5))
        tk.Label(hint_frame, text="Format: HH:MM (24-hour format)", bg="#f0f0f0", 
                font=("Arial", 8), fg="#666666").pack(anchor='w')

        # Created by
        created_frame = tk.Frame(form_frame, bg="#f0f0f0")
        created_frame.pack(fill='x', pady=5)
        tk.Label(created_frame, text="Created By:", bg="#f0f0f0", font=("Arial", 10)).pack(anchor='w')
        self.created_by_entry = tk.Entry(created_frame, width=40)
        self.created_by_entry.pack(fill='x', pady=(2, 0))

        # Set button
        tk.Button(form_frame, text="Set Trading Hours", command=self.set_trading_hours, 
                 bg="#4CAF50", fg="white", font=("Arial", 10, "bold"), 
                 relief='raised', padx=20, pady=5).pack(pady=10)

        # Separator
        separator = tk.Frame(left_frame, height=2, bg="#cccccc")
        separator.pack(fill='x', pady=10)

        # Trading hours list section
        list_frame = tk.Frame(left_frame, bg="#f0f0f0")
        list_frame.pack(fill='both', expand=True)

        tk.Label(list_frame, text="Current Trading Hours", bg="#f0f0f0", 
                font=("Arial", 11, "bold")).pack(anchor='w', pady=(0, 5))

        # Listbox with scrollbar
        listbox_frame = tk.Frame(list_frame, bg="#f0f0f0")
        listbox_frame.pack(fill='both', expand=True)

        scrollbar = tk.Scrollbar(listbox_frame)
        scrollbar.pack(side='right', fill='y')

        self.trading_hours_listbox = tk.Listbox(listbox_frame, height=12, yscrollcommand=scrollbar.set,
                                              font=("Arial", 9), selectmode='single')
        self.trading_hours_listbox.pack(side='left', fill='both', expand=True)
        scrollbar.config(command=self.trading_hours_listbox.yview)

        # Delete button
        tk.Button(list_frame, text="Delete Selected Hours", command=self.delete_selected_hours,
                 bg="#f44336", fg="white", font=("Arial", 10, "bold"), 
                 relief='raised', padx=20, pady=5).pack(pady=(10, 0))

        # === RIGHT FRAME CONTENT ===
        # Log widget
        self.log_widget = LogWidget(right_frame)
        self.log_widget.pack(fill='both', expand=True)
        self.log_widget.log("Trading hour setup interface loaded.")

         # Set up GUI logging BEFORE creating any loggers
        Logger.set_gui_handler(self.log_widget)
        
        # NOW initialize logger for this class
        self.logger = Logger.get_logger(self.__class__.__name__)
        
        # Initialize service (make sure it also uses Logger.get_logger())
        db_config = database_config.get_db_config_from_env()
        self.db_connector = DatabaseConnector(db_config)
        
        # Test the logger immediately
        self.logger.info("Trading Hour Setup tab loaded successfully!")
        # Load trading hours
        self.load_trading_hours_list()

    def validate_time_format(self, time_str):
        """Validate time format HH:MM"""
        try:
            datetime.strptime(time_str, "%H:%M")
            return True
        except ValueError:
            return False

    def set_trading_hours(self):
        try:
            from_date = datetime.strptime(self.from_entry.get(), "%Y-%m-%d").date()
            to_date = datetime.strptime(self.to_entry.get(), "%Y-%m-%d").date()
            start_time = self.start_time_entry.get().strip()
            end_time = self.end_time_entry.get().strip()
            created_by = self.created_by_entry.get().strip()

            # Validation
            if from_date > to_date:
                raise ValueError("From date must be before or equal to To date.")
            
            if not self.validate_time_format(start_time):
                raise ValueError("Invalid start time format. Please use HH:MM format.")
            
            if not self.validate_time_format(end_time):
                raise ValueError("Invalid end time format. Please use HH:MM format.")
            
            if not created_by:
                raise ValueError("Created By field is required.")
            
            # Check if start time is before end time
            start_datetime = datetime.strptime(start_time, "%H:%M")
            end_datetime = datetime.strptime(end_time, "%H:%M")
            if start_datetime >= end_datetime:
                raise ValueError("Start time must be before end time.")

            trading_dates = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d")
                            for i in range((to_date - from_date).days + 1)]

            queries = [f"""
                INSERT INTO set_trading_hour (start_time, end_time, trading_day, created_by)
                VALUES ('{start_time}', '{end_time}', '{d}', '{created_by}')
            """ for d in trading_dates]

            success = self.db_connector.execute_transaction(queries)

            if success:
                self.log_widget.log(f"✅ Successfully set trading hours for {len(trading_dates)} day(s).")
                self.log_widget.log(f"   Period: {from_date} to {to_date}")
                self.log_widget.log(f"   Hours: {start_time} - {end_time}")
                self.load_trading_hours_list()
                # Clear form fields
                self.clear_form()
                messagebox.showinfo("Success", f"Trading hours set successfully for {len(trading_dates)} day(s)!")
            else:
                self.log_widget.log("❌ Failed to update trading hours.")
                messagebox.showerror("Error", "Failed to update trading hours.")
                
        except Exception as e:
            self.log_widget.log(f"❌ Error: {str(e)}")
            messagebox.showerror("Error", str(e))

    def clear_form(self):
        """Clear form fields after successful addition"""
        self.created_by_entry.delete(0, tk.END)
        # Reset time entries to default values
        self.start_time_entry.delete(0, tk.END)
        self.start_time_entry.insert(0, "09:00")
        self.end_time_entry.delete(0, tk.END)
        self.end_time_entry.insert(0, "17:00")

    def load_trading_hours_list(self):
        """Load trading hours from database and display in listbox"""
        try:
            # Get recent trading hours (last 30 days or upcoming)
            query = """
               SELECT TOP 100 trading_day, start_time, end_time, created_by
                FROM set_trading_hour
                ORDER BY trading_day DESC
            """
            rows = self.db_connector.execute_query(query)
            self.trading_hours_listbox.delete(0, tk.END)
            
            if rows:
                for row in rows:
                    # Format: "YYYY-MM-DD: 09:00-17:00 (by: username)"
                    display_text = f"{row.trading_day}: {row.start_time}-{row.end_time} (by: {row.created_by})"
                    self.trading_hours_listbox.insert(tk.END, display_text)
                self.log_widget.log(f"Loaded {len(rows)} trading hour records.")
            else:
                self.trading_hours_listbox.insert(tk.END, "No trading hours configured")
                self.log_widget.log("No trading hours found in database.")
                
        except Exception as e:
            self.log_widget.log(f"Error loading trading hours list: {e}")
            self.trading_hours_listbox.delete(0, tk.END)
            self.trading_hours_listbox.insert(tk.END, "Error loading data")

    def delete_selected_hours(self):
        """Delete selected trading hours"""
        try:
            selected_index = self.trading_hours_listbox.curselection()
            if not selected_index:
                self.log_widget.log("No trading hours selected for deletion.")
                return

            selected_text = self.trading_hours_listbox.get(selected_index)
            
            # Check if it's the "No trading hours configured" or error message
            if "No trading hours" in selected_text or "Error loading" in selected_text:
                self.log_widget.log("Cannot delete this entry.")
                return
                
            # Extract trading day from "YYYY-MM-DD: HH:MM-HH:MM (by: username)"
            trading_day = selected_text.split(':')[0]
            time_info = selected_text.split(': ')[1].split(' (by:')[0]

            # Confirmation dialog
            result = messagebox.askyesno(
                "Confirm Deletion",
                f"Are you sure you want to delete trading hours:\n\n"
                f"Date: {trading_day}\n"
                f"Hours: {time_info}\n\n"
                f"This action cannot be undone.",
                icon='warning'
            )

            if result:
                query = f"DELETE FROM set_trading_hour WHERE trading_day = '{trading_day}'"
                delete_result = self.db_connector.delete_execute_query(query)
                
                if delete_result['success'] and delete_result['rows_affected'] > 0:
                    self.log_widget.log(f"Deleted trading hours for {trading_day}.")
                    self.load_trading_hours_list()
                elif delete_result['success'] and delete_result['rows_affected'] == 0:
                    self.log_widget.log(f"No trading hours found for {trading_day} to delete.")
                else:
                    self.log_widget.log(f"Failed to delete trading hours: {delete_result['message']}")
            else:
                self.log_widget.log("Trading hours deletion cancelled.")
                
        except Exception as e:
            self.log_widget.log(f"Error deleting trading hours: {e}")
            messagebox.showerror("Error", f"Error deleting trading hours: {e}")