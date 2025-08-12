import tkinter as tk
from datetime import datetime
from tkinter import scrolledtext


class LogWidget(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        
        # Create frame for controls
        control_frame = tk.Frame(self)
        control_frame.pack(fill='x', padx=5, pady=2)
        
        # Clear button
        tk.Button(control_frame, text="Clear Logs", 
                 command=self.clear_logs).pack(side='left', padx=2)
        
        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(control_frame, text="Auto-scroll", 
                      variable=self.auto_scroll_var).pack(side='left', padx=10)
        
        # Log level filter
        tk.Label(control_frame, text="Level:").pack(side='left', padx=(20, 2))
        self.level_var = tk.StringVar(value="ALL")
        level_combo = tk.OptionMenu(control_frame, self.level_var, 
                                   "ALL", "DEBUG", "INFO", "WARNING", "ERROR")
        level_combo.pack(side='left', padx=2)
        
        # Add trace to refresh display when filter changes
        self.level_var.trace('w', lambda *args: self.refresh_display())
        
        # Text widget with scrollbar
        self.text = scrolledtext.ScrolledText(
            self, 
            height=15, 
            wrap='word',
            font=('Consolas', 9),
            bg='#f8f8f8',
            fg='#333333'
        )
        self.text.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Configure text tags for different log levels
        self.text.tag_configure("DEBUG", foreground="#666666")
        self.text.tag_configure("INFO", foreground="#0066cc")
        self.text.tag_configure("WARNING", foreground="#ff8800")
        self.text.tag_configure("ERROR", foreground="#cc0000", font=('Consolas', 9, 'bold'))
        
        # Store all log messages for filtering
        self.all_logs = []
    
    def log(self, message):
        """Original method for backward compatibility"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_message = f"[{timestamp}] {message}"
        self.add_log_message(full_message, level="INFO")
    
    def add_log_message(self, message, level=None):
        """Add log message with optional level for coloring"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Detect level from message if not provided
        if level is None:
            if '[DEBUG]' in message:
                level = 'DEBUG'
            elif '[INFO]' in message:
                level = 'INFO'
            elif '[WARNING]' in message:
                level = 'WARNING'
            elif '[ERROR]' in message:
                level = 'ERROR'
            else:
                level = 'INFO'
        
        # Store the log entry
        log_entry = {
            'timestamp': timestamp,
            'message': message,
            'level': level
        }
        self.all_logs.append(log_entry)
        
        # Apply filter and display
        self.refresh_display()
    
    def refresh_display(self):
        """Refresh the display based on current filter"""
        current_filter = self.level_var.get()
        
        # Clear current display
        self.text.delete('1.0', tk.END)
        
        # Add filtered messages
        for log_entry in self.all_logs:
            if current_filter == "ALL" or log_entry['level'] == current_filter:
                # Insert message with appropriate tag
                self.text.insert('end', f"{log_entry['message']}\n", log_entry['level'])
        
        # Auto-scroll if enabled
        if self.auto_scroll_var.get():
            self.text.see('end')
    
    def clear_logs(self):
        """Clear all log messages"""
        self.all_logs.clear()
        self.text.delete('1.0', tk.END)