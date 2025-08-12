from abc import ABC, abstractmethod

import pandas as pd

from database.connector import DatabaseConnector


class IndexCalculator(ABC):
    """Base abstract class for index calculators"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    @abstractmethod
    def calculate(self) -> pd.DataFrame:
        """Calculate the index values"""
        pass
    
