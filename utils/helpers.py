import pandas as pd
from schedule import logger

from database.connector import DatabaseConnector


class HelperClass:
    """A helper class for various utility functions."""
   


    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector

        
    def get_share_information(self) -> pd.DataFrame:
        """Fetch share information for all companies"""
        try:
            query = """
            SELECT DISTINCT ss.company, ss.total_share, 
                   ss.Sponsor, ss.Govt, ss.Institute, ss.Foreign_share, ss.public_share
            FROM Symbol_Share ss
            INNER JOIN (
                SELECT company, MAX(scraping_date) as max_date
                FROM Symbol_Share
                GROUP BY company
            ) latest ON ss.company = latest.company AND ss.scraping_date = latest.max_date
            """
            share_data = self.db.fetch_dataframe(query)
            
            # Calculate free float percentage
            share_data['free_float_pct'] = 100 - (
                (share_data['Sponsor']  + share_data['Govt'])
            )

            return share_data
        except Exception as e:
            logger.error(f"Error getting share information: {str(e)}")
            return pd.DataFrame()