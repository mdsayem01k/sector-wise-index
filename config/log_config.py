import logging
import tkinter as tk
from datetime import datetime

class GUILogHandler(logging.Handler):
    """Custom logging handler that sends logs to GUI widget"""
    def __init__(self, log_widget=None):
        super().__init__()
        self.log_widget = log_widget
    
    def set_log_widget(self, log_widget):
        """Set the log widget after initialization"""
        self.log_widget = log_widget
    
    def emit(self, record):
        if self.log_widget:
            msg = self.format(record)
            # Use after() to ensure thread safety when updating GUI from different threads
            self.log_widget.after(0, self.log_widget.add_log_message, msg)

class Logger:
    _gui_handler = None
    
    @staticmethod
    def set_gui_handler(log_widget):
        """Set the GUI log widget for all loggers"""
        if Logger._gui_handler is None:
            Logger._gui_handler = GUILogHandler()
        Logger._gui_handler.set_log_widget(log_widget)
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.DEBUG)
            
            # Console handler (your existing setup)
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # GUI handler (new addition)
            if Logger._gui_handler:
                gui_formatter = logging.Formatter(
                    '[%(levelname)s] [%(name)s] %(message)s'
                )
                Logger._gui_handler.setFormatter(gui_formatter)
                logger.addHandler(Logger._gui_handler)
            
            logger.propagate = False
        elif Logger._gui_handler and Logger._gui_handler not in logger.handlers:
            # Add GUI handler to existing loggers
            gui_formatter = logging.Formatter(
                '[%(levelname)s] [%(name)s] %(message)s'
            )
            Logger._gui_handler.setFormatter(gui_formatter)
            logger.addHandler(Logger._gui_handler)
            
        return logger