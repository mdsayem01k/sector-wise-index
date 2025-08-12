# gui/main_window.py
import os
import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
from datetime import datetime, timedelta
import mplcursors
from matplotlib.dates import num2date
from dotenv import load_dotenv

from gui.realtime_tab import RealtimeFrame
from gui.historical_tab import HistoricalFrame
from gui.holidays_setup_tab import HolidaySetupFrame
from gui.trading_hour_setup_tab import TradingHourSetupFrame


class MainWindow(tk.Tk):
    """
    The main application window for the Sector Index GUI.

    Manages the overall layout, navigation, and state of the application,
    including auto-refresh functionality for the home dashboard.
    """
    def __init__(self):
        super().__init__()
        self.title("Sector Index GUI")
        self.geometry("1200x700")

        # Auto-refresh variables
        self.auto_refresh_interval = 60000  # 60,000 ms = 1 minute
        self.auto_refresh_job = None
        self.is_home_active = False
        self.auto_refresh_enabled = True

        self._create_navbar()

        # Main content area
        self.container = tk.Frame(self)
        self.container.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Initialize with the home view
        self.show_home()

    def _create_navbar(self):
        """Creates the top navigation bar with buttons."""
        navbar = tk.Frame(self, bg="lightgray")
        navbar.pack(side="top", fill="x")

        buttons = [
            ("Home", self.show_home),
            ("DB Config", self.show_db_config),
            ("Rebuild", self.show_rebuild),
            ("Realtime", self.show_realtime),
            ("Historical", self.show_historical),
            ("Holiday Setup", self.show_holiday_setup),
            ("Trading Hour Setup", self.show_trading_hour_setup),
        ]

        for text, command in buttons:
            btn = tk.Button(navbar, text=text, command=command)
            btn.pack(side="left", padx=5, pady=5)

    def _start_auto_refresh(self):
        """Starts the auto-refresh timer."""
        self._stop_auto_refresh()
        if self.is_home_active and self.auto_refresh_enabled:
            self.auto_refresh_job = self.after(
                self.auto_refresh_interval, self._auto_refresh_callback
            )

    def _stop_auto_refresh(self):
        """Stops the auto-refresh timer."""
        if self.auto_refresh_job:
            self.after_cancel(self.auto_refresh_job)
            self.auto_refresh_job = None

    def _auto_refresh_callback(self):
        """Callback function for the auto-refresh timer."""
        try:
            if self.is_home_active and self.auto_refresh_enabled:
                self.refresh_chart_data()

                current_time = datetime.now()
                selected_sector = (
                    self.sector_var.get()
                    if hasattr(self, "sector_var")
                    else "Unknown"
                )
                self.refresh_status_label.config(
                    text=f"Auto-refresh: ACTIVE | Last: {current_time.strftime('%H:%M:%S')} ({selected_sector})",
                    fg="green",
                )

                self._start_auto_refresh()
            else:
                self._stop_auto_refresh()
        except Exception as e:
            print(f"Error in auto-refresh: {e}")
            if self.is_home_active and self.auto_refresh_enabled:
                self._start_auto_refresh()

    def _on_close(self):
        """Handles window close event."""
        self._stop_auto_refresh()
        self.destroy()
        os._exit(0)

    def _clear_container(self):
        """Removes all widgets from the main container."""
        for widget in self.container.winfo_children():
            widget.destroy()

    def show_db_config(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
        # frame = DBConfigFrame(self.container, self.service)
        # frame.pack(fill="both", expand=True)

    def show_rebuild(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
    

    def show_realtime(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
        frame = RealtimeFrame(self.container)
        frame.pack(fill="both", expand=True)

    def show_historical(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
        frame = HistoricalFrame(self.container)
        frame.pack(fill="both", expand=True)

    def show_holiday_setup(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
        frame = HolidaySetupFrame(self.container)
        frame.pack(fill="both", expand=True)

    def show_trading_hour_setup(self):
        self.is_home_active = False
        self._stop_auto_refresh()
        self._clear_container()
        frame = TradingHourSetupFrame(self.container)
        frame.pack(fill="both", expand=True)

    def show_home(self):
        """Displays the home dashboard with the sector index chart."""
        self.is_home_active = True
        self._clear_container()

        main_frame = tk.Frame(self.container, padx=10, pady=10)
        main_frame.pack(fill="both", expand=True)

        self._create_home_header(main_frame)
        self._create_home_controls(main_frame)

        self.chart_frame = tk.Frame(main_frame)
        self.chart_frame.pack(fill="both", expand=True)

        self._setup_chart()
        self._load_sectors()
        self._update_chart()

        # Delay auto-refresh start to ensure UI is ready
        self.after(1000, self._delayed_start_auto_refresh)

    def _create_home_header(self, parent):
        """Creates the title and status labels for the home page."""
        title_frame = tk.Frame(parent)
        title_frame.pack(fill="x", pady=(0, 10))

        title_label = tk.Label(
            title_frame, text="Sector Index Dashboard", font=("Arial", 18, "bold")
        )
        title_label.pack(side="left")

        self.refresh_status_label = tk.Label(
            title_frame,
            text="Auto-refresh: STARTING...",
            font=("Arial", 10),
            fg="orange",
        )
        self.refresh_status_label.pack(side="right")

    def _create_home_controls(self, parent):
        """Creates the control widgets for the home page."""
        control_frame = tk.Frame(parent)
        control_frame.pack(fill="x", pady=(0, 10))

        # Sector selection
        tk.Label(
            control_frame, text="Select Sector:", font=("Arial", 10)
        ).pack(side="left", padx=(0, 5))
        self.sector_var = tk.StringVar(value="bank")
        self.sector_combo = ttk.Combobox(
            control_frame, textvariable=self.sector_var, width=20, state="readonly"
        )
        self.sector_combo.pack(side="left", padx=(0, 10))
        self.sector_combo.bind("<<ComboboxSelected>>", self._update_chart)

        # Refresh button
        refresh_btn = tk.Button(
            control_frame, text="Manual Refresh", command=self.refresh_chart_data
        )
        refresh_btn.pack(side="left", padx=(0, 10))

        # Toggle auto-refresh button
        self.toggle_auto_refresh_btn = tk.Button(
            control_frame,
            text="Disable Auto-Refresh",
            command=self._toggle_auto_refresh,
        )
        self.toggle_auto_refresh_btn.pack(side="left", padx=(0, 10))

    def _delayed_start_auto_refresh(self):
        """Starts auto-refresh after the UI is fully loaded."""
        self._start_auto_refresh()
        if self.auto_refresh_job:
            self.refresh_status_label.config(
                text=f"Auto-refresh: ACTIVE (every {self.auto_refresh_interval // 1000}s)",
                fg="green",
            )

    def _toggle_auto_refresh(self):
        """Toggles the auto-refresh functionality on or off."""
        if self.auto_refresh_enabled:
            self.auto_refresh_enabled = False
            self._stop_auto_refresh()
            self.toggle_auto_refresh_btn.config(text="Enable Auto-Refresh")
            self.refresh_status_label.config(text="Auto-refresh: DISABLED", fg="red")
        else:
            self.auto_refresh_enabled = True
            if self.is_home_active:
                self._start_auto_refresh()
                self.toggle_auto_refresh_btn.config(text="Disable Auto-Refresh")
                self.refresh_status_label.config(text="Auto-refresh: ENABLED", fg="green")

    def _setup_chart(self):
        """Sets up the matplotlib figure and canvas."""
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, self.chart_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        self.fig.patch.set_facecolor("white")
        self.ax.grid(True, alpha=0.3)
        self.ax.set_xlabel("Date", fontsize=10)
        self.ax.set_ylabel("Index Value", fontsize=10)

    def _load_sectors(self):
        """Loads available sectors from the database into the combobox."""
        try:
            current_selection = (
                self.sector_var.get() if hasattr(self, "sector_var") else None
            )
            query = "SELECT DISTINCT sector_name FROM Sector_Information ORDER BY sector_name"
            result = self.service.execute_query(query)

            if result:
                sectors = [row[0] for row in result]
                self.sector_combo["values"] = sectors
                if current_selection and current_selection in sectors:
                    self.sector_var.set(current_selection)
                elif "bank" in sectors:
                    self.sector_var.set("bank")
                elif sectors:
                    self.sector_var.set(sectors[0])
            else:
                self.sector_combo["values"] = ["No sectors found"]

        except Exception as e:
            print(f"Error loading sectors: {e}")
            self.sector_combo["values"] = ["Error loading sectors"]

    def _update_chart(self, event=None):
        """Updates the chart with data for the selected sector."""
        selected_sector = self.sector_var.get()

        if selected_sector in ["No sectors found", "Error loading sectors"]:
            self._show_no_data_message()
            return

        try:
            query = f"""
                SELECT timestamp, Pindex_value, Cindex_value, total_return, num_companies
                FROM Sector_Index_Values
                WHERE sector_name = '{selected_sector}'
                ORDER BY timestamp
            """
            result = self.service.execute_query(query)

            if not result:
                self._show_no_data_message()
                return

            df = pd.DataFrame(
                result,
                columns=[
                    "timestamp",
                    "Pindex_value",
                    "Cindex_value",
                    "total_return",
                    "num_companies",
                ],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            self.ax.clear()
            self.ax.plot(
                df["timestamp"],
                df["Cindex_value"],
                label="Current Index",
                linewidth=2,
                color="#ff7f0e",
            )

            cursor = mplcursors.cursor(self.ax.lines, hover=True)

            @cursor.connect("add")
            def on_add(sel):
                x_date = num2date(sel.target[0])
                y_value = sel.target[1]
                sel.annotation.set_text(
                    f"{x_date.strftime('%Y-%m-%d %H:%M')}\nIndex: {y_value:.2f}"
                )

            self.ax.set_title(
                f"{selected_sector.title()} Sector Index Values",
                fontsize=14,
                fontweight="bold",
                pad=20,
            )
            self.ax.set_xlabel("Date", fontsize=10)
            self.ax.set_ylabel("Index Value", fontsize=10)
            self.ax.legend(loc="upper left")
            self.ax.grid(True, alpha=0.3)
            self.fig.autofmt_xdate()
            self.ax.tick_params(axis="both", which="major", labelsize=9)
            self.fig.tight_layout()
            self.canvas.draw()

            self._update_info_label(df, selected_sector)

        except Exception as e:
            print(f"Error updating chart: {e}")
            self._show_error_message(str(e))

    def _show_no_data_message(self):
        """Displays a message on the chart when no data is available."""
        self.ax.clear()
        self.ax.text(
            0.5,
            0.5,
            "No data available for selected sector",
            ha="center",
            va="center",
            transform=self.ax.transAxes,
            fontsize=14,
            color="gray",
        )
        self.ax.set_xlim(0, 1)
        self.ax.set_ylim(0, 1)
        self.canvas.draw()

    def _show_error_message(self, error_msg):
        """Displays an error message on the chart."""
        self.ax.clear()
        self.ax.text(
            0.5,
            0.5,
            f"Error loading data:\n{error_msg}",
            ha="center",
            va="center",
            transform=self.ax.transAxes,
            fontsize=12,
            color="red",
        )
        self.ax.set_xlim(0, 1)
        self.ax.set_ylim(0, 1)
        self.canvas.draw()

    def _update_info_label(self, df, sector_name):
        """Updates the info label below the chart with the latest data."""
        if hasattr(self, "info_label"):
            self.info_label.destroy()

        if not df.empty:
            latest_row = df.iloc[-1]
            info_text = (
                f"Latest Data for {sector_name.title()}: "
                f"Price Index: {latest_row['Pindex_value']:.2f}, "
                f"Current Index: {latest_row['Cindex_value']:.2f}, "
                f"Companies: {int(latest_row['num_companies'])}, "
                f"Last Updated: {latest_row['timestamp'].strftime('%Y-%m-%d %H:%M')}"
            )
            self.info_label = tk.Label(
                self.chart_frame, text=info_text, font=("Arial", 9), fg="gray"
            )
            self.info_label.pack(side="bottom", pady=5)

    def refresh_chart_data(self):
        """Manually refreshes the chart data and sector list."""
        self._load_sectors()
        self._update_chart()
        current_time = datetime.now()
        selected_sector = (
            self.sector_var.get() if hasattr(self, "sector_var") else "Unknown"
        )
        if hasattr(self, "refresh_status_label") and self.auto_refresh_enabled:
            self.refresh_status_label.config(
                text=f"Auto-refresh: ACTIVE | Last: {current_time.strftime('%H:%M:%S')} ({selected_sector})",
                fg="green",
            )