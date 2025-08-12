
import os
import pandas as pd
from schedule import logger
from openpyxl import load_workbook

class DataExportService:
    """Service for exporting data to CSV files"""
    
    def __init__(self):
        pass

    def export_to_csv(self, results, filename):
        """Export results to CSV file"""
        try:
            if not results.empty:
                results.to_csv(filename, index=False)
                logger.info(f"Exported data to {filename}")
            else:
                logger.warning("No results to export")
        except Exception as e:
            logger.error(f"Error exporting results to CSV: {str(e)}")

    
    def save_market_cap_to_excel(self, mcap_data: dict, file_name: str = 'hist_prep_market_cap_data.xlsx'):
        """Append current market cap data to an Excel file without removing existing data."""
        try:
            df = pd.DataFrame.from_dict(mcap_data, orient='index')

            if os.path.exists(file_name):
                with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    sheet = writer.sheets['Sheet1']
                    startrow = sheet.max_row
                    df.to_excel(writer, startrow=startrow, index=True, header=False)
            else:
                df.to_excel(file_name, index=True)

        except Exception as e:
            logger.error(f"Failed to save market cap data to Excel: {e}")