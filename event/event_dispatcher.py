"""
Event Dispatcher - Observer Pattern for event management
"""

from typing import Dict, List, Callable, Type, Any
from collections import defaultdict
import logging
from .events import BaseEvent

logger = logging.getLogger(__name__)


class EventDispatcher:
    """Observer pattern implementation for event dispatching"""
    
    def __init__(self):
        self._subscribers: Dict[Type[BaseEvent], List[Callable]] = defaultdict(list)
        self._global_subscribers: List[Callable] = []
    
    def subscribe(self, event_type: Type[BaseEvent], handler: Callable[[BaseEvent], None]) -> None:
        """Subscribe a handler to specific event type"""
        if handler not in self._subscribers[event_type]:
            self._subscribers[event_type].append(handler)
            logger.debug(f"Subscribed {handler.__name__} to {event_type.__name__}")
    
    def subscribe_all(self, handler: Callable[[BaseEvent], None]) -> None:
        """Subscribe a handler to all events"""
        if handler not in self._global_subscribers:
            self._global_subscribers.append(handler)
            logger.debug(f"Subscribed {handler.__name__} to all events")
    
    def unsubscribe(self, event_type: Type[BaseEvent], handler: Callable[[BaseEvent], None]) -> None:
        """Unsubscribe a handler from specific event type"""
        if handler in self._subscribers[event_type]:
            self._subscribers[event_type].remove(handler)
            logger.debug(f"Unsubscribed {handler.__name__} from {event_type.__name__}")
    
    def unsubscribe_all(self, handler: Callable[[BaseEvent], None]) -> None:
        """Unsubscribe a handler from all events"""
        if handler in self._global_subscribers:
            self._global_subscribers.remove(handler)
            logger.debug(f"Unsubscribed {handler.__name__} from all events")
    
    def dispatch(self, event: BaseEvent) -> None:
        """Dispatch event to all subscribed handlers"""
        event_type = type(event)
        
        # Notify specific event handlers
        for handler in self._subscribers[event_type]:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error in handler {handler.__name__}: {str(e)}")
        
        # Notify global handlers
        for handler in self._global_subscribers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error in global handler {handler.__name__}: {str(e)}")
    
    def get_subscriber_count(self, event_type: Type[BaseEvent] = None) -> int:
        """Get number of subscribers for event type or total"""
        if event_type:
            return len(self._subscribers[event_type])
        return sum(len(handlers) for handlers in self._subscribers.values()) + len(self._global_subscribers)
    
    def clear_subscribers(self, event_type: Type[BaseEvent] = None) -> None:
        """Clear subscribers for specific event type or all"""
        if event_type:
            self._subscribers[event_type].clear()
        else:
            self._subscribers.clear()
            self._global_subscribers.clear()


# Singleton instance
dispatcher = EventDispatcher()