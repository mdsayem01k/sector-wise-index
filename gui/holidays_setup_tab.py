import tkinter as tk
from tkinter import messagebox
from datetime import datetime, timedelta
from config.database_config import database_config
from database.connector import DatabaseConnector
from tkcalendar import DateEntry
from gui.log_widget import LogWidget
from config.log_config import Logger


class HolidaySetupFrame(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg="#f0f0f0")
        db_config = database_config.get_db_config_from_env()
        self.db_connector = DatabaseConnector(db_config)
        self.logger = Logger.get_logger(self.__class__.__name__)
        
        
        
        # Title
        tk.Label(self, text="Holiday Setup", font=("Arial", 14, "bold"), bg="#f0f0f0").pack(pady=10)

        # Main horizontal frame
        main_frame = tk.Frame(self, bg="#f0f0f0")
        main_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Set minimum size for this frame
        self.pack_propagate(False)  # Don't let children control frame size
        self.config(width=1200, height=800)  # Set fixed frame size

        # Configure grid layout - left side much wider than right side
        main_frame.columnconfigure(0, weight=3)  # Left side gets 3/4 of space
        main_frame.columnconfigure(1, weight=1)  # Right side gets 1/4 of space

        # Left side: Holiday Information
        left_frame = tk.LabelFrame(main_frame, text="Holiday Information", font=("Arial", 12, "bold"), 
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

        # Holiday name
        name_frame = tk.Frame(form_frame, bg="#f0f0f0")
        name_frame.pack(fill='x', pady=5)
        tk.Label(name_frame, text="Holiday Name:", bg="#f0f0f0", font=("Arial", 10)).pack(anchor='w')
        self.holiday_name_entry = tk.Entry(name_frame, width=40)
        self.holiday_name_entry.pack(fill='x', pady=(2, 0))

        # Description
        desc_frame = tk.Frame(form_frame, bg="#f0f0f0")
        desc_frame.pack(fill='x', pady=5)
        tk.Label(desc_frame, text="Description:", bg="#f0f0f0", font=("Arial", 10)).pack(anchor='w')
        self.description_entry = tk.Entry(desc_frame, width=40)
        self.description_entry.pack(fill='x', pady=(2, 0))

        # Created by
        created_frame = tk.Frame(form_frame, bg="#f0f0f0")
        created_frame.pack(fill='x', pady=5)
        tk.Label(created_frame, text="Created By:", bg="#f0f0f0", font=("Arial", 10)).pack(anchor='w')
        self.created_by_entry = tk.Entry(created_frame, width=40)
        self.created_by_entry.pack(fill='x', pady=(2, 0))

        # Add button
        tk.Button(form_frame, text="Add Holidays", command=self.add_holidays, 
                 bg="#4CAF50", fg="white", font=("Arial", 10, "bold"), 
                 relief='raised', padx=20, pady=5).pack(pady=10)

        # Separator
        separator = tk.Frame(left_frame, height=2, bg="#cccccc")
        separator.pack(fill='x', pady=10)

        # Holiday list section
        list_frame = tk.Frame(left_frame, bg="#f0f0f0")
        list_frame.pack(fill='both', expand=True)

        tk.Label(list_frame, text="This Year's Holidays", bg="#f0f0f0", 
                font=("Arial", 11, "bold")).pack(anchor='w', pady=(0, 5))

        # Listbox with scrollbar
        listbox_frame = tk.Frame(list_frame, bg="#f0f0f0")
        listbox_frame.pack(fill='both', expand=True)

        scrollbar = tk.Scrollbar(listbox_frame)
        scrollbar.pack(side='right', fill='y')

        self.holiday_listbox = tk.Listbox(listbox_frame, height=12, yscrollcommand=scrollbar.set,
                                         font=("Arial", 9), selectmode='single')
        self.holiday_listbox.pack(side='left', fill='both', expand=True)
        scrollbar.config(command=self.holiday_listbox.yview)

        # Delete button
        tk.Button(list_frame, text="Delete Selected Holiday", command=self.delete_selected_holiday,
                 bg="#f44336", fg="white", font=("Arial", 10, "bold"), 
                 relief='raised', padx=20, pady=5).pack(pady=(10, 0))

        self.log_widget = LogWidget(right_frame)
        self.log_widget.pack(fill='both', expand=True)
        self.log_widget.log("Holidays setup interface loaded.")

         # Set up GUI logging BEFORE creating any loggers
        Logger.set_gui_handler(self.log_widget)

        # Load holidays
        self.load_holiday_list()

    def add_holidays(self):
        try:
            from_date = datetime.strptime(self.from_entry.get(), "%Y-%m-%d").date()
            to_date = datetime.strptime(self.to_entry.get(), "%Y-%m-%d").date()
            holiday_name = self.holiday_name_entry.get().strip()
            description = self.description_entry.get().strip()
            created_by = self.created_by_entry.get().strip()

            if from_date > to_date:
                raise ValueError("From date must be before or equal to To date.")
            if not holiday_name or not created_by:
                raise ValueError("Holiday name and created by fields are required.")

            holiday_dates = [(from_date + timedelta(days=i)) for i in range((to_date - from_date).days + 1)]

            queries = [
                f"""
                INSERT INTO Holidays (holiday_date, day_name, holiday_name, description, created_by)
                VALUES ('{d.strftime('%Y-%m-%d')}', '{d.strftime('%A')}', '{holiday_name}', '{description}', '{created_by}')
                """ for d in holiday_dates
            ]

            success = self.db_connector.execute_transaction(queries)

            if success:
                self.log_widget.log(f"Added {len(holiday_dates)} holiday(s) to the database.")
                self.load_holiday_list()
                # Clear form fields
                self.clear_form()
            else:
                self.log_widget.log("Failed to add holidays to the database.")
        except Exception as e:
            self.log_widget.log(f"Error: {e}")

    def clear_form(self):
        """Clear all form fields after successful addition"""
        self.holiday_name_entry.delete(0, tk.END)
        self.description_entry.delete(0, tk.END)
        self.created_by_entry.delete(0, tk.END)

    def load_holiday_list(self):
        try:
            current_year = datetime.now().year
            query = f"""
                SELECT holiday_date, day_name, holiday_name
                FROM Holidays
                WHERE YEAR(holiday_date) = {current_year}
                ORDER BY holiday_date
            """
            rows = self.db_connector.execute_query(query)
            self.holiday_listbox.delete(0, tk.END)
            for row in rows:
                self.holiday_listbox.insert(tk.END, f"{row.holiday_date} ({row.day_name}) - {row.holiday_name}")
        except Exception as e:
            self.log_widget.log(f"Error loading holiday list: {e}")

    def delete_selected_holiday(self):
        try:
            selected_index = self.holiday_listbox.curselection()
            if not selected_index:
                self.log_widget.log("No holiday selected for deletion.")
                return

            selected_text = self.holiday_listbox.get(selected_index)
            holiday_date = selected_text.split()[0]  # Extract date from "YYYY-MM-DD (Day) - Name"
            holiday_name = selected_text.split(' - ', 1)[1]  # Extract holiday name

            # Confirmation dialog
            result = messagebox.askyesno(
                "Confirm Deletion",
                f"Are you sure you want to delete the holiday:\n\n"
                f"Date: {holiday_date}\n"
                f"Holiday: {holiday_name}\n\n"
                f"This action cannot be undone.",
                icon='warning'
            )

            if result:
                query = f"DELETE FROM Holidays WHERE holiday_date = '{holiday_date}'"
                delete_result = self.db_connector.delete_execute_query(query)
                

                if delete_result['success'] and delete_result['rows_affected'] > 0:
                    self.log_widget.log(f"Deleted holiday '{holiday_name}' on {holiday_date}.")
                    self.load_holiday_list()
                elif delete_result['success'] and delete_result['rows_affected'] == 0:
                    self.log_widget.log(f"No holiday found on {holiday_date} to delete.")
                else:
                    self.log_widget.log(f"Failed to delete holiday: {delete_result['message']}")
            else:
                self.log_widget.log("Holiday deletion cancelled.")
                

        except Exception as e:
            self.log_widget.log(f"Error deleting holiday: {e}")