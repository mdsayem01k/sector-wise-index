
from datetime import datetime

from schedule import logger
from config.database_config import database_config
from database.connector import DatabaseConnector
from calculators.historical_sector_calculator import HistoricalSectorIndexCalculator
from services.historical_index_service import HistoricalIndexService
from services.data_export_service import DataExportService

def main():
    # Load database configuration from environment variables
    db_config = database_config.get_db_config_from_env()
    
    # Validate DB configuration
    if not all([db_config['server'], db_config['database'], db_config['username'], db_config['password']]):
        logger.error("Missing database configuration. Please check your .env file.")
        logger.error(f"Server: {db_config['server']}, Database: {db_config['database']}")
        exit(1)
    
    logger.info(f"Connecting to database server: {db_config['server']}, database: {db_config['database']}")
    
    # Create historical index service
    service = HistoricalIndexService(db_config)
    
    
    end_date = datetime(2025, 7, 22)
    start_date =  datetime(2025, 7, 22)

    # Format dates for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Calculate historical indices
    results = service.calculate_historical_indices(start_date_str, end_date_str)
    
    # Export results to CSV
        # Export results to CSV
    if not results.empty:
        exporter = DataExportService()
        exporter.export_to_csv(results, "historical_sector_indices.csv")

    
if __name__ == "__main__":
    main()