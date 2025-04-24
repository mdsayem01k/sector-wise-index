# Sector-wise Index Analysis 📊

## Overview

This document provides comprehensive documentation for a sector index calculation system. The system calculates financial sector indices based on stock market data, stores results in a database, and manages the trading session lifecycle.

## Classes

### DatabaseConnector

A class for managing database connections and operations with SQL Server.

#### Methods

- **`__init__(self, config: Dict[str, str])`**
  - Task: Initialize the database connector with connection parameters
  - Parameters: config - Dictionary containing database connection parameters
  - Return: None

- **`engine(self)`**
  - Task: Property that lazily initializes and returns the database engine
  - Return: SQLAlchemy engine instance

- **`_get_connection_string(self) -> str`**
  - Task: Create connection string from configuration
  - Return: ODBC connection string

- **`verify_connection(self) -> bool`**
  - Task: Verify database connection is working
  - Return: Boolean indicating connection success

- **`execute_query(self, query: str) -> Any`**
  - Task: Execute a SQL query with retry logic
  - Parameters: query - SQL statement to execute
  - Return: Query result rows or execution result

- **`fetch_dataframe(self, query: str) -> pd.DataFrame`**
  - Task: Execute a query and return the result as a pandas DataFrame
  - Parameters: query - SQL statement to execute
  - Return: DataFrame containing query results

- **`execute_transaction(self, queries: List[str]) -> bool`**
  - Task: Execute multiple queries as a transaction
  - Parameters: queries - List of SQL statements
  - Return: Boolean indicating transaction success

### IndexCalculator (Abstract Base Class)

Base abstract class for different types of index calculators.

#### Methods

- **`__init__(self, db_connector: DatabaseConnector)`**
  - Task: Initialize calculator with database connector
  - Parameters: db_connector - DatabaseConnector instance
  - Return: None

- **`calculate(self) -> pd.DataFrame`** (abstract)
  - Task: Calculate index values
  - Return: DataFrame containing calculation results

- **`store_results(self, results: pd.DataFrame) -> None`** (abstract)
  - Task: Store calculated results in database
  - Parameters: results - DataFrame with calculation results
  - Return: None

### SectorIndexCalculator

Class for calculating sector-based indices, extending IndexCalculator.

#### Methods

- **`__init__(self, db_connector: DatabaseConnector)`**
  - Task: Initialize sector index calculator
  - Parameters: db_connector - DatabaseConnector instance
  - Return: None

- **`initialize_indices(self) -> None`**
  - Task: Initialize index values from daily_index table using stored procedure
  - Return: None

- **`sector_cache(self) -> Dict[str, Dict[str, Any]]`**
  - Task: Fetch sector information from database with caching
  - Return: Dictionary mapping sector codes to sector information

- **`get_market_cap_data(self, timestamp: datetime.datetime) -> Dict[str, Dict[str, Any]]`**
  - Task: Calculate market cap data for all companies at specific timestamp
  - Parameters: timestamp - Datetime for which to get market data
  - Return: Dictionary mapping company symbols to market cap data

- **`get_previous_market_cap_data(self) -> Dict[str, Dict[str, Any]]`**
  - Task: Get market cap data using stored procedure
  - Return: Dictionary mapping company symbols to previous market cap data

- **`save_previous_market_cap_data(self) -> bool`**
  - Task: Save current market cap data to database for next trading session
  - Return: Boolean indicating success

- **`calculate(self) -> pd.DataFrame`**
  - Task: Calculate sector indices based on latest LTP data
  - Return: DataFrame containing calculated sector indices

- **`store_results(self, results: pd.DataFrame) -> None`**
  - Task: Store calculated index results to database
  - Parameters: results - DataFrame with calculation results
  - Return: None

### MarketIndexService

Main service class that orchestrates the index calculation pipeline.

#### Methods

- **`__init__(self, db_config: Dict[str, str], trading_start_time: datetime.time, trading_end_time: datetime.time, weekend_days: List[int])`**
  - Task: Initialize service with configuration and trading parameters
  - Parameters: 
    - db_config - Database configuration
    - trading_start_time - Daily trading start time
    - trading_end_time - Daily trading end time
    - weekend_days - List of integers representing weekend days (0=Monday, 6=Sunday)
  - Return: None

- **`is_trading_hours(self) -> bool`**
  - Task: Check if current time is within trading hours
  - Return: Boolean indicating if current time is in trading hours

- **`is_day_end(self) -> bool`**
  - Task: Check if current time is at end of trading day but EOD processing not done
  - Return: Boolean indicating if it's time for end-of-day processing

- **`save_daily_index_data(self) -> None`**
  - Task: Call stored procedure to save end-of-day index values
  - Return: None

- **`calculate_indices(self) -> None`**
  - Task: Calculate indices and log results
  - Return: None

- **`run(self) -> None`**
  - Task: Run index calculation if in trading hours, or daily summary at end of day
  - Return: None

- **`run_scheduled(self, index_interval_minutes: int = 1) -> None`**
  - Task: Schedule execution of index calculation
  - Parameters: index_interval_minutes - Interval between calculations in minutes
  - Return: None

## Utility Functions

- **`create_required_tables(db_connector)`**
  - Task: Create required tables using stored procedure
  - Parameters: db_connector - DatabaseConnector instance
  - Return: Boolean indicating success

- **`get_db_config_from_env()`**
  - Task: Load database configuration from environment variables
  - Return: Dictionary with database configuration

- **`main()`**
  - Task: Main entry point that initializes and runs the service
  - Return: None

## Flow of Execution

1. Program loads database configuration from environment variables
2. Creates database tables if they don't exist
3. Initializes the MarketIndexService with trading parameters
4. Loads previous market cap data and sector indices
5. Runs index calculations on a schedule during trading hours
6. Processes end-of-day data after trading hours
7. Saves market cap data for the next trading day

## Data Model

The system relies on the following database tables:
- Sector_Information - Stores sector definitions
- Sector_Symbol - Maps companies to sectors
- Sector_Index_Values - Stores calculated index values
- previous_market_cap_data - Stores market cap data for next calculation cycle

The system also uses stored procedures for database operations:
- sector_index.GetLatestSectorIndices
- sector_index.sp_GetMarketCapData
- sector_index.get_previous_market_cap_data
- sector_index.save_daily_index_data
- sector_index.create_required_tables
