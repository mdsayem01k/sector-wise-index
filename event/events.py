"""
Events - Event classes for event-driven architecture
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass


@dataclass
class BaseEvent(ABC):
    """Base class for all events"""
    event_id: str
    timestamp: datetime
    source: str
    data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if not self.event_id:
            self.event_id = f"{self.__class__.__name__}_{id(self)}"
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class CalculationStartedEvent(BaseEvent):
    """Event fired when index calculation starts"""
    start_date: datetime
    end_date: datetime
    sectors: List[str]
    calculation_type: str = "historical_sector_index"
    
    def __post_init__(self):
        super().__post_init__()
        self.data = {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'sectors': self.sectors,
            'calculation_type': self.calculation_type
        }


@dataclass
class CalculationCompletedEvent(BaseEvent):
    """Event fired when index calculation completes"""
    results_count: int
    execution_time: float
    success: bool = True
    error_message: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.data = {
            'results_count': self.results_count,
            'execution_time': self.execution_time,
            'success': self.success,
            'error_message': self.error_message
        }


@dataclass
class DataExportEvent(BaseEvent):
    """Event fired when data export is requested or completed"""
    export_format: str
    file_path: str
    record_count: int
    export_status: str = "completed"  # pending, completed, failed
    
    def __post_init__(self):
        super().__post_init__()
        self.data = {
            'export_format': self.export_format,
            'file_path': self.file_path,
            'record_count': self.record_count,
            'export_status': self.export_status
        }


@dataclass
class ErrorEvent(BaseEvent):
    """Event fired when an error occurs"""
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    severity: str = "error"  # info, warning, error, critical
    
    def __post_init__(self):
        super().__post_init__()
        self.data = {
            'error_type': self.error_type,
            'error_message': self.error_message,
            'stack_trace': self.stack_trace,
            'severity': self.severity
        }