"""
Event Handlers - Concrete implementations for handling different events
"""

from abc import ABC, abstractmethod
import logging
import pandas as pd
from typing import Optional
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .events import (
    BaseEvent, 
    CalculationStartedEvent, 
    CalculationCompletedEvent,
    DataExportEvent, 
    ErrorEvent
)

logger = logging.getLogger(__name__)


class BaseEventHandler(ABC):
    """Abstract base class for event handlers"""
    
    @abstractmethod
    def handle(self, event: BaseEvent) -> None:
        """Handle the event"""
        pass
    
    def can_handle(self, event: BaseEvent) -> bool:
        """Check if this handler can process the event"""
        return True


class LoggingEventHandler(BaseEventHandler):
    """Handler for logging all events"""
    
    def __init__(self, log_level: str = "INFO"):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.log_level = getattr(logging, log_level.upper())
    
    def handle(self, event: BaseEvent) -> None:
        """Log event details"""
        if isinstance(event, CalculationStartedEvent):
            self.logger.info(f"Calculation started: {event.start_date} to {event.end_date}")
            self.logger.info(f"Sectors: {', '.join(event.sectors)}")
        
        elif isinstance(event, CalculationCompletedEvent):
            if event.success:
                self.logger.info(f"Calculation completed: {event.results_count} results in {event.execution_time:.2f}s")
            else:
                self.logger.error(f"Calculation failed: {event.error_message}")
        
        elif isinstance(event, DataExportEvent):
            self.logger.info(f"Data export {event.export_status}: {event.file_path} ({event.record_count} records)")
        
        elif isinstance(event, ErrorEvent):
            log_method = getattr(self.logger, event.severity.lower(), self.logger.error)
            log_method(f"Error ({event.error_type}): {event.error_message}")
            if event.stack_trace:
                log_method(f"Stack trace: {event.stack_trace}")
        
        else:
            self.logger.log(self.log_level, f"Event: {event.__class__.__name__} from {event.source}")


class ExportEventHandler(BaseEventHandler):
    """Handler for data export events"""
    
    def __init__(self, base_export_path: str = "./exports/"):
        self.base_export_path = base_export_path
    
    def handle(self, event: BaseEvent) -> None:
        """Handle export events"""
        if isinstance(event, CalculationCompletedEvent) and event.success:
            self._auto_export_results(event)
        
        elif isinstance(event, DataExportEvent):
            self._log_export_status(event)
    
    def _auto_export_results(self, event: CalculationCompletedEvent) -> None:
        """Automatically export calculation results"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"calculation_results_{timestamp}.csv"
            logger.info(f"Auto-exporting {event.results_count} results to {filename}")
        except Exception as e:
            logger.error(f"Auto-export failed: {str(e)}")
    
    def _log_export_status(self, event: DataExportEvent) -> None:
        """Log export operation status"""
        if event.export_status == "completed":
            logger.info(f"Export completed: {event.file_path}")
        elif event.export_status == "failed":
            logger.error(f"Export failed: {event.file_path}")


class NotificationEventHandler(BaseEventHandler):
    """Handler for sending notifications"""
    
    def __init__(self, email_config: Optional[dict] = None):
        self.email_config = email_config or {}
        self.enabled = bool(self.email_config.get('smtp_server'))
    
    def handle(self, event: BaseEvent) -> None:
        """Send notifications for critical events"""
        if not self.enabled:
            return
        
        if isinstance(event, ErrorEvent) and event.severity == "critical":
            self._send_error_notification(event)
        
        elif isinstance(event, CalculationCompletedEvent):
            if event.success:
                self._send_success_notification(event)
            else:
                self._send_failure_notification(event)
    
    def _send_error_notification(self, event: ErrorEvent) -> None:
        """Send critical error notification"""
        subject = f"Critical Error in Index Calculation System"
        body = f"""
        A critical error occurred:
        
        Error Type: {event.error_type}
        Message: {event.error_message}
        Time: {event.timestamp}
        Source: {event.source}
        """
        self._send_email(subject, body)
    
    def _send_success_notification(self, event: CalculationCompletedEvent) -> None:
        """Send calculation success notification"""
        subject = "Index Calculation Completed Successfully"
        body = f"""
        Index calculation completed successfully:
        
        Results: {event.results_count} records
        Execution Time: {event.execution_time:.2f} seconds
        Completed At: {event.timestamp}
        """
        self._send_email(subject, body)
    
    def _send_failure_notification(self, event: CalculationCompletedEvent) -> None:
        """Send calculation failure notification"""
        subject = "Index Calculation Failed"
        body = f"""
        Index calculation failed:
        
        Error: {event.error_message}
        Failed At: {event.timestamp}
        """
        self._send_email(subject, body)
    
    def _send_email(self, subject: str, body: str) -> None:
        """Send email notification"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config.get('from_email')
            msg['To'] = ', '.join(self.email_config.get('to_emails', []))
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(
                self.email_config['smtp_server'], 
                self.email_config.get('smtp_port', 587)
            )
            server.starttls()
            server.login(
                self.email_config['username'], 
                self.email_config['password']
            )
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Notification sent: {subject}")
            
        except Exception as e:
            logger.error(f"Failed to send notification: {str(e)}")


class MetricsEventHandler(BaseEventHandler):
    """Handler for collecting metrics and performance data"""
    
    def __init__(self):
        self.metrics = {
            'calculations_started': 0,
            'calculations_completed': 0,
            'calculations_failed': 0,
            'total_execution_time': 0.0,
            'errors_by_type': {},
            'exports_completed': 0
        }
    
    def handle(self, event: BaseEvent) -> None:
        """Collect metrics from events"""
        if isinstance(event, CalculationStartedEvent):
            self.metrics['calculations_started'] += 1
        
        elif isinstance(event, CalculationCompletedEvent):
            if event.success:
                self.metrics['calculations_completed'] += 1
                self.metrics['total_execution_time'] += event.execution_time
            else:
                self.metrics['calculations_failed'] += 1
        
        elif isinstance(event, ErrorEvent):
            error_type = event.error_type
            self.metrics['errors_by_type'][error_type] = \
                self.metrics['errors_by_type'].get(error_type, 0) + 1
        
        elif isinstance(event, DataExportEvent) and event.export_status == "completed":
            self.metrics['exports_completed'] += 1
    
    def get_metrics(self) -> dict:
        """Get current metrics"""
        return self.metrics.copy()
    
    def reset_metrics(self) -> None:
        """Reset all metrics"""
        self.metrics = {
            'calculations_started': 0,
            'calculations_completed': 0,
            'calculations_failed': 0,
            'total_execution_time': 0.0,
            'errors_by_type': {},
            'exports_completed': 0
        }