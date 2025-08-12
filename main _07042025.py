import pandas as pd
import numpy as np
import pyodbc
import time
import logging
from datetime import datetime
import schedule
import threading
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.dialects.mssql import DECIMAL
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sector_index.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SectorIndexCalculator")

class SectorIndexCalculator:
    def __init__(self, base_timestamp=None, base_index_value=100):
        """
        Initialize the Sector Index Calculator
        
        Parameters:
        base_timestamp: The starting timestamp for index calculation (default: None, will use first available)
        base_index_value: The initial index value (default: 100)
        """
        self.db_server = os.getenv('DB_SERVER')
        self.db_name = os.getenv('DB_NAME')
        self.db_username = os.getenv('DB_USERNAME')
        self.db_password = os.getenv('DB_PASSWORD')
        
        self.conn_str = f"Driver={{SQL Server}};Server={self.db_server};Database={self.db_name};UID={self.db_username};PWD={self.db_password}"
        

        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={self.conn_str}")
        
        self.base_timestamp = base_timestamp
        self.base_index_value = base_index_value
        self.sector_indices = {}  # Store the latest index values for each sector
        self.last_processed_time = None
        
        # Create sector_indices table if it doesn't exist
        self._create_sector_indices_table()
        
        logger.info(f"Connected to database: {self.db_name} on server: {self.db_server}")
    
    def _create_sector_indices_table(self):
        """Create the sector_indices table if it doesn't exist"""
        try:
            create_table_query = """
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sector_indices]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[sector_indices](
                    [id] [int] IDENTITY(1,1) PRIMARY KEY,
                    [sector_code] [varchar](50) NOT NULL,
                    [sector_name] [varchar](100) NOT NULL,
                    [index_value] [decimal](18, 4) NOT NULL,
                    [sector_return] [decimal](18, 4) NULL,
                    [calculation_timestamp] [datetime] NOT NULL,
                    [created_at] [datetime] NOT NULL DEFAULT GETDATE()
                )
            END
            """
            with self.engine.connect() as connection:
                connection.execute(text(create_table_query))
                connection.commit()
                
            logger.info("Sector indices table created or confirmed existing")
        except Exception as e:
            logger.error(f"Error creating sector indices table: {str(e)}")
    
    def get_latest_data(self):
        """Get the latest price data and sector/company info from the database"""
        try:
            price_data_query = """
                SELECT 
                    company AS symbol, 
                    LTP AS ltp, 
                    ltp_dt AS timestamp
                FROM Symbol_LTP 
                WHERE ltp_dt > ISNULL(?, '1900-01-01')
                ORDER BY ltp_dt ASC
            """
                                    
            company_info_query = """
                SELECT 
                    ss.company AS symbol,
                    si.sector_code,
                    si.sector_name,
                    sh.total_share AS shares_outstanding,
                    sh.Sponsor / 100.0 AS sponsor_shareholding_pct,
                    sh.Govt / 100.0 AS govt_shareholding_pct
                FROM Sector_Symbol ss
                JOIN Sector_Information si ON ss.sector_code = si.sector_code
                JOIN Symbol_Share sh ON ss.company = sh.company
                WHERE si.isActive = 1
            """
            
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                
                cursor.execute(price_data_query, (self.last_processed_time,))
                price_data = cursor.fetchall()
                
                cursor.execute(company_info_query)
                company_info_data = cursor.fetchall()
        
            symbols = []
            ltps = []
            timestamps = []
            
            for row in price_data:
                symbols.append(row[0])      
                ltps.append(row[1])         
                timestamps.append(row[2])   
            
            price_df = pd.DataFrame({
                'symbol': symbols,
                'ltp': ltps,
                'timestamp': timestamps
            })
            
            company_symbols = []
            sector_codes = []
            sector_names = []
            shares_outstandings = []
            sponsor_shareholding_pcts = []
            govt_shareholding_pcts = []
            
            for row in company_info_data:
                company_symbols.append(row[0])      
                sector_codes.append(row[1])           
                sector_names.append(row[2])             
                shares_outstandings.append(row[3])     
                sponsor_shareholding_pcts.append(row[4]) 
                govt_shareholding_pcts.append(row[5])   
            
            company_info_df = pd.DataFrame({
                'symbol': company_symbols,
                'sector_code': sector_codes,
                'sector_name': sector_names,
                'shares_outstanding': shares_outstandings,
                'sponsor_shareholding_pct': sponsor_shareholding_pcts,
                'govt_shareholding_pct': govt_shareholding_pcts
            })
            
           
            price_df = price_df.dropna(subset=['ltp'])
            
            if not price_df.empty:
                self.last_processed_time = price_df['timestamp'].max()
            
            return price_df, company_info_df
            
        except Exception as e:
            logger.error(f"Error getting data from database: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()

    def prepare_complete_price_data(self, price_df, company_info_df):
        """
        Ensure we have price data for all 656 companies at each timestamp interval.
        For each 10s interval, include the latest price for each company.
        """
        try:

            price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
            

            price_df = price_df.sort_values(['symbol', 'timestamp'])
            
            # Define the time range and interval (10 seconds)
            min_time = price_df['timestamp'].min()
            max_time = price_df['timestamp'].max()
            
            # Generate timestamps at 10-second intervals
            time_range = pd.date_range(start=min_time, end=max_time, freq='10s')
            
            # Create a list to store results
            results = []
            
            # For each 10-second timestamp
            for ts in time_range:
                # Get data for each company up to this timestamp
                temp_df = price_df[price_df['timestamp'] <= ts]
                
                # Group by symbol and get the latest entry for each
                latest_prices = temp_df.groupby('symbol').last().reset_index()
                
                # Add the timestamp column to identify this interval
                # Remove the original timestamp column first to avoid duplication
                latest_prices = latest_prices.drop('timestamp', axis=1)  # Drop the original timestamp
                latest_prices['timestamp'] = ts  # Use the same name for consistency
                
                # Append to results
                results.append(latest_prices)
            
            # Combine all results
            merged_df = pd.concat(results, ignore_index=True)
            
            # Create a complete DataFrame with all symbol-timestamp combinations
            all_symbols = company_info_df['symbol'].unique()
            all_intervals = list(time_range)
            
            complete_index = pd.MultiIndex.from_product([all_symbols, all_intervals], names=['symbol', 'timestamp'])
            complete_df = pd.DataFrame(index=complete_index).reset_index()
            
            # Merge with our results to fill in available data
            final_complete_df = pd.merge(complete_df, merged_df, on=['symbol', 'timestamp'], how='left')
            
            # For rows where price is missing, forward fill from previous timestamps
            final_complete_df = final_complete_df.sort_values(['symbol', 'timestamp'])
            final_complete_df['ltp'] = final_complete_df.groupby('symbol')['ltp'].ffill()
            
            logger.info(f"Prepared complete price data with {len(final_complete_df)} rows")
            # final_complete_df.to_csv("final_complete_df.csv", index=False)
            return final_complete_df
        
        except Exception as e:
            logger.error(f"Error in prepare_complete_price_data: {str(e)}")
            return price_df  # Return original if error
            
        
    
    def calculate_market_cap(self, df):
        """
        Step 1: Calculate market capitalization for each company
        Market Cap = LTP * number of shares
        """
        df['market_cap'] = df['ltp'] * df['shares_outstanding']
        return df
    
    def calculate_free_float(self, df):
        """
        Step 2: Calculate free float percentage
        Free Float (%) = 1 - (Sponsor Shareholding (%) + Govt. Shareholding (%))
        """
        df['free_float_pct'] = 1 - (df['sponsor_shareholding_pct'] + df['govt_shareholding_pct'])
        return df
    
    def calculate_free_float_mcap(self, df):
        """
        Step 3: Calculate free float market capitalization
        Free Float Market Cap = Free Float (%) * Market Cap
        """
        df['free_float_mcap'] = df['free_float_pct'] * df['market_cap']
        return df
    
    def calculate_sector_weights(self, df):
        """
        Step 4: Calculate free float market cap weight for each company within its sector
        Company FF Mcap Weight (%) = (Company FF Mcap / Total Sectoral FF Mcap) * 100
        """
        # Calculate total FF Mcap for each sector
        sector_totals = df.groupby('sector_code')['free_float_mcap'].sum().reset_index()
        sector_totals.rename(columns={'free_float_mcap': 'total_sector_ff_mcap'}, inplace=True)
        
        # Merge back to get the sector totals
        df = pd.merge(df, sector_totals, on='sector_code', how='left')
        
        # Calculate the weight
        # Ensure we're working with float values instead of Decimal to avoid DivisionUndefined error
        df['free_float_mcap'] = df['free_float_mcap'].astype(float)
        df['total_sector_ff_mcap'] = df['total_sector_ff_mcap'].astype(float)
        
        # Use numpy's where to handle division by zero
        df['ff_mcap_weight'] = np.where(
            df['total_sector_ff_mcap'] > 0,  # Check only if denominator is > 0
            (df['free_float_mcap'] / df['total_sector_ff_mcap']) * 100,
            0  # Default to 0 if denominator is 0
        )
        
        return df
    
    def process_timestamp_data(self, current_data, previous_data=None):
        """
        Process data for the current timestamp, calculating steps 5-8
        
        Parameters:
        current_data: DataFrame containing current timestamp data with all calculations up to step 4
        previous_data: DataFrame containing previous timestamp data with all calculations (default: None)
        
        Returns:
        DataFrame with sector indices for the current timestamp
        """
        if previous_data is None and self.base_timestamp is None:
            # First run, set base timestamp and return initial index values
            sectors = current_data[['sector_code', 'sector_name']].drop_duplicates()
            current_timestamp = current_data['timestamp'].iloc[0]
            
            result_data = []
            for _, sector in sectors.iterrows():
                # Store the initial index values
                self.sector_indices[sector['sector_code']] = self.base_index_value
                
                # Add to results
                result_data.append({
                    'sector_code': sector['sector_code'],
                    'sector_name': sector['sector_name'],
                    'index_value': float(self.base_index_value),  # Ensure it's a float
                    'sector_return': 0.0,  # No return for base timestamp
                    'calculation_timestamp': current_timestamp
                })
            
            return pd.DataFrame(result_data)
        
        # Group data by sector to process each sector
        sectors = current_data[['sector_code', 'sector_name']].drop_duplicates()
        result_data = []
        current_timestamp = current_data['timestamp'].iloc[0]
        
        for _, sector in sectors.iterrows():
            sector_code = sector['sector_code']
            sector_name = sector['sector_name']
            
            # Get data for current sector
            sector_current = current_data[current_data['sector_code'] == sector_code]
            
            if previous_data is not None:
                sector_previous = previous_data[previous_data['sector_code'] == sector_code]
                
                if not sector_previous.empty:
                    # Step 5 & 6: Calculate price returns and weighted price returns
                    merged_data = pd.merge(
                        sector_current[['symbol', 'free_float_mcap', 'ff_mcap_weight']],
                        sector_previous[['symbol', 'free_float_mcap']],
                        on='symbol',
                        how='inner',
                        suffixes=('_current', '_previous')
                    )
                    
                    # Skip if we don't have matching data
                    if merged_data.empty:
                        continue
                    
                    # Convert to float to avoid Decimal type errors
                    merged_data['free_float_mcap_current'] = merged_data['free_float_mcap_current'].astype(float)
                    merged_data['free_float_mcap_previous'] = merged_data['free_float_mcap_previous'].astype(float)
                    merged_data['ff_mcap_weight'] = merged_data['ff_mcap_weight'].astype(float)
                    
                    # Calculate price returns with proper zero handling
                    merged_data['price_return'] = np.where(
                        merged_data['free_float_mcap_previous'] > 0,
                        (merged_data['free_float_mcap_current'] / merged_data['free_float_mcap_previous']) - 1,
                        0  # If previous free float mcap is 0, set return to 0
                    )
                    
                    # Calculate weighted price returns - fix the condition logic
                    merged_data['weighted_price_return'] = np.where(
                        (merged_data['price_return'] > 0) & (merged_data['ff_mcap_weight'] > 0),
                        merged_data['price_return'] * (merged_data['ff_mcap_weight'] / 100),
                        0
                    )
                    
                    # Step 7: Calculate total sectoral return
                    total_sector_return = merged_data['weighted_price_return'].sum()
                    
                    # Step 8: Calculate index value
                    previous_index = self.sector_indices.get(sector_code, self.base_index_value)
                    current_index = previous_index * (1 + total_sector_return)
                    
                    # Store the updated index value
                    self.sector_indices[sector_code] = current_index
                    
                    # Add to results - ensure types are explicitly floats
                    result_data.append({
                        'sector_code': sector_code,
                        'sector_name': sector_name,
                        'index_value': float(current_index),
                        'sector_return': float(total_sector_return * 100),  # Convert to percentage
                        'calculation_timestamp': current_timestamp
                    })
                else:
                    # Sector not in previous data, use base value
                    current_index = self.sector_indices.get(sector_code, self.base_index_value)
                    result_data.append({
                        'sector_code': sector_code,
                        'sector_name': sector_name,
                        'index_value': float(current_index),
                        'sector_return': 0.0,
                        'calculation_timestamp': current_timestamp
                    })
            else:
                # For the first calculation against the base
                current_index = self.sector_indices.get(sector_code, self.base_index_value)
                result_data.append({
                    'sector_code': sector_code,
                    'sector_name': sector_name,
                    'index_value': float(current_index),
                    'sector_return': 0.0,  # No return for base timestamp
                    'calculation_timestamp': current_timestamp
                })
        
        return pd.DataFrame(result_data)
    
    def save_indices_to_db(self, indices_df):
        """Save the calculated indices to the database"""
        try:
            if indices_df.empty:
                logger.warning("No indices to save to database")
                return
            
            # Convert numeric columns to float to avoid any Decimal issues
            indices_df['index_value'] = indices_df['index_value'].astype(float)
            indices_df['sector_return'] = indices_df['sector_return'].astype(float)
            
            # Insert data using parameterized queries for safety
            with self.engine.connect() as connection:
                for _, record in indices_df.iterrows():
                    insert_query = text("""
                    INSERT INTO sector_indices 
                    (sector_code, sector_name, index_value, sector_return, calculation_timestamp)
                    VALUES (:sector_code, :sector_name, :index_value, :sector_return, :calculation_timestamp)
                    """)
                    
                    connection.execute(insert_query, {
                        'sector_code': record['sector_code'],
                        'sector_name': record['sector_name'],
                        'index_value': float(record['index_value']),
                        'sector_return': float(record['sector_return']),
                        'calculation_timestamp': record['calculation_timestamp']
                    })
                connection.commit()
                
            logger.info(f"Saved {len(indices_df)} sector indices to database")
        except Exception as e:
            logger.error(f"Error saving indices to database: {str(e)}")
    
    def calculate_indices(self):
        """
        Main function to calculate the sector indices
        """
        try:
            start_time = time.time()
            logger.info("Starting sector index calculation...")
            
            # Get the latest data
            price_df, company_info_df = self.get_latest_data() 
            
            if price_df.empty or company_info_df.empty:
                logger.warning("No data available for processing")
                return
            
            # Prepare complete price data to ensure all 656 companies at each timestamp
            complete_price_df = self.prepare_complete_price_data(price_df, company_info_df)
            
            # Group the price data by timestamp to process each timestamp separately
            timestamps = complete_price_df['timestamp'].unique()
            previous_processed_data = None
            
            for timestamp in sorted(timestamps):
                # Get data for current timestamp
                current_timestamp_data = complete_price_df[complete_price_df['timestamp'] == timestamp]
                
                # Merge with company and sector info
                merged_data = pd.merge(
                    current_timestamp_data,
                    company_info_df,
                    on='symbol',
                    how='inner'
                )
                
                if merged_data.empty:
                    logger.warning(f"No matching data for timestamp {timestamp}")
                    continue
            
                # merged_data.to_csv("merged_data.csv", index=False)

                # Perform calculations steps 1-4
                merged_data = self.calculate_market_cap(merged_data)
                merged_data = self.calculate_free_float(merged_data)
                merged_data = self.calculate_free_float_mcap(merged_data)
                merged_data = self.calculate_sector_weights(merged_data)
                
                # Process the timestamp data (steps 5-8)
                indices_df = self.process_timestamp_data(merged_data, previous_processed_data)
                # indices_df.to_csv("indices_df.csv", index=False)
                # Save the indices to the database
                self.save_indices_to_db(indices_df)
                
                # Update previous data for the next iteration
                previous_processed_data = merged_data.copy()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Sector index calculation completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in sector index calculation: {str(e)}")
    
    def run_scheduler(self):
        """Run the calculator on a schedule"""
        # Schedule to run every 10 seconds
        schedule.every(10).seconds.do(self.calculate_indices)
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def start(self):
        """Start the calculator in a separate thread"""
        logger.info("Starting Sector Index Calculator...")
        thread = threading.Thread(target=self.run_scheduler)
        thread.daemon = True
        thread.start()
        return thread

# Example usage
if __name__ == "__main__":
    # Create and start the calculator
    calculator = SectorIndexCalculator()
    calculator_thread = calculator.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping Sector Index Calculator...")