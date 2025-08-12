import time
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import logging

from .helpers import HelperClass
from database.connector import DatabaseConnector

from config.log_config import Logger


class MarketCapDataProcessor:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.logger = Logger.get_logger(self.__class__.__name__)
        self.current_indices = {} 
        self._max_query_retries = 3
        self._query_retry_delay = 2
    
  
    def get_market_cap_data(
        self,
        timestamp: Union[datetime, List[datetime]],
        mode: str = 'real'
    ) -> Union[Dict[str, Dict[str, Any]], pd.DataFrame]:
        try:
            if mode == 'real':
                for attempt in range(self._max_query_retries):
                    try:
                        timestamp_str = timestamp
                        query = f"EXEC sector_index.sp_GetMarketCapData @timestamp = '{timestamp_str}'"
                        
                        self.logger.debug(f"Fetching market cap data for timestamp: {timestamp_str} (attempt {attempt + 1})")
                        start_time = time.time()
                        df = self.db.fetch_dataframe(query)
                        duration = time.time() - start_time

                        if duration > 10:
                            self.logger.warning(f"Query took {duration:.2f} seconds")

                        if df.empty:
                            self.logger.warning(f"No market cap data for timestamp {timestamp_str}")
                            if attempt == self._max_query_retries - 1:
                                return {}
                            time.sleep(self._query_retry_delay)
                            continue

                        share_data = self._get_share_data()
                        share_dict = {row['company']: row for _, row in share_data.iterrows()} if not share_data.empty else {}

                        data = self._process_market_cap_rows(df, share_dict, timestamp)
                        self.logger.info(f"Successfully fetched data for {len(data)} companies")
                        return data

                    except Exception as e:
                        self.logger.error(f"Error (attempt {attempt + 1}): {str(e)}")
                        if attempt == self._max_query_retries - 1:
                            return {}
                        time.sleep(self._query_retry_delay * (attempt + 1))

            elif mode == 'hist':
                if not isinstance(timestamp, list) or len(timestamp) < 2:
                    self.logger.error("For 'hist', timestamp must be a list with at least 2 items")
                    return pd.DataFrame()

                share_data = self._get_share_data()
                if share_data.empty:
                    self.logger.error("Failed to get share information")
                    return pd.DataFrame()

                share_dict = {row['company']: row for _, row in share_data.iterrows()}
                records = []

                for i in range(1, len(timestamp)):
                    current_ts = timestamp[i]
                    prev_ts = timestamp[i - 1]

                    params = {
                        'current_timestamp': current_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'previous_timestamp': prev_ts.strftime('%Y-%m-%d %H:%M:%S')
                    }

                    query = """
                    EXEC sector_index.sp_GetMarketCapData_HIST 
                    @current_timestamp = :current_timestamp, 
                    @previous_timestamp = :previous_timestamp
                    """

                    df = self.db.fetch_dataframe(query, params)
                    if df.empty:
                        self.logger.warning(f"No data for {current_ts}")
                        continue

                    processed = self._process_market_cap_rows(df, share_dict, current_ts, return_dict=False)
                    records.extend(processed)

                    self.logger.info(f"Data collected for {current_ts} vs {prev_ts}")

                return pd.DataFrame(records)
            else:
                self.logger.error(f"Invalid mode: {mode}")
                return pd.DataFrame() if mode == 'hist' else {}

        except Exception as e:
            self.logger.error(f"Error in get_market_cap_data: {str(e)}")
            return pd.DataFrame() if mode == 'hist' else {}

    def _process_market_cap_rows(
        self,
        df: pd.DataFrame,
        share_dict: Dict[str, Dict],
        ts: datetime,
        return_dict: bool = True
    ) -> Union[Dict[str, Dict], List[Dict]]:
        result = {} if return_dict else []

        for _, row in df.iterrows():
            company = row['company']
            ltp = row.get('LTP')
            share_info = share_dict.get(company, {})
            total_shares = share_info.get('total_share')
            free_float_pct = share_info.get('free_float_pct')

            if not isinstance(ltp, (int, float)) or ltp <= 0:
                self.logger.warning(f"Invalid LTP for {company}: {ltp}")
                continue

            if not isinstance(total_shares, (int, float)) or total_shares <= 0:
                self.logger.warning(f"Invalid shares for {company}: {total_shares}")
                continue

            market_cap = ltp * total_shares
            free_float_mcap = market_cap * (free_float_pct / 100)

            entry = {
                'timestamp': ts,
                'company': company,
                'ltp': ltp,
                'total_shares': total_shares,
                'market_cap': market_cap,
                'free_float_pct': free_float_pct,
                'free_float_mcap': free_float_mcap
            }

            if return_dict:
                result[company] = entry
            else:
                result.append(entry)

        return result


    def get_previous_market_cap_data(self, source_type: int = 0, start_date: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        try:
            share_data = self._get_share_data()
            if share_data.empty:
                return {}

            share_lookup = {row['company']: row for _, row in share_data.iterrows()}
            result = {}

            if source_type == 0:
                if not start_date:
                    self.logger.error("start_date is required for historical data")
                    return {}

                prev_ts = start_date - timedelta(days=10)
                prev_df = self._fetch_previous_market_data(start_date, prev_ts)
                if prev_df.empty:
                    return {}

            elif source_type == 1:
                if hasattr(self, 'prev_market_cap_data') and self.prev_market_cap_data is not None:
                    self.logger.debug("Using cached real-time market cap data")
                    return self.prev_market_cap_data

                for attempt in range(self._max_query_retries):
                    try:
                        query = "EXEC sector_index.get_previous_market_cap_data"
                        start_time = time.time()
                        prev_df = self.db.fetch_dataframe(query)
                        duration = time.time() - start_time

                        if duration > 15:
                            self.logger.warning(f"Stored procedure query took {duration:.2f} seconds")

                        if not prev_df.empty:
                            break

                        self.logger.warning(f"Empty result, retrying (attempt {attempt + 1})")
                        time.sleep(self._query_retry_delay)

                    except Exception as e:
                        self.logger.error(f"Stored procedure error (attempt {attempt + 1}): {e}")
                        time.sleep(self._query_retry_delay * (attempt + 1))

                else:
                    self.logger.error("Stored procedure failed after retries")
                    return {}

            else:
                self.logger.error("Invalid source_type value. Use 0 (hist) or 1 (real-time).")
                return {}

            # process data
            for _, row in prev_df.iterrows():
                company = row['company']
                ltp = row.get('ltp') or row.get('LTP')
                share_info = share_lookup.get(company)

                if not self._is_valid_data(company, ltp, share_info):
                    continue

                total_shares = share_info['total_share']
                free_float_pct = share_info['free_float_pct']
                market_cap = ltp * total_shares
                free_float_mcap = market_cap * (free_float_pct / 100)

                result[company] = {
                    'ltp': ltp,
                    'timestamp': row.get('timestamp') or row.get('ltp_dt', start_date),
                    'total_shares': total_shares,
                    'market_cap': market_cap,
                    'free_float_pct': free_float_pct,
                    'free_float_mcap': free_float_mcap
                }

            if result:
                pd.DataFrame.from_dict(result, orient='index').to_csv('previous_market_cap_data.csv', index_label='company')
                self.logger.info(f"Loaded previous market cap data for {len(result)} companies")

            if source_type == 1:
                self.prev_market_cap_data = result

            return result

        except Exception as e:
            self.logger.exception(f"Error in get_previous_market_cap_data: {e}")
            return {}

    def _get_share_data(self) -> pd.DataFrame:
        helper = HelperClass(self.db)
        share_df = helper.get_share_information()
        if share_df.empty:
            self.logger.error("Failed to retrieve share information.")
        return share_df

    def _is_valid_data(self, company, ltp, share_info) -> bool:
        return (
            pd.notnull(ltp)
            and share_info is not None
            and 'total_share' in share_info
            and 'free_float_pct' in share_info
            and pd.notnull(share_info['total_share'])
            and pd.notnull(share_info['free_float_pct'])
        )


    def _fetch_market_data(self, current_ts: datetime, prev_ts: datetime) -> pd.DataFrame:
        params = {
            'current_timestamp': current_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'previous_timestamp': prev_ts.strftime('%Y-%m-%d %H:%M:%S')
        }
        query = """
        EXEC sector_index.sp_GetMarketCapData_HIST 
        @current_timestamp = :current_timestamp, 
        @previous_timestamp = :previous_timestamp
        """
        df = self.db.fetch_dataframe(query, params)
        if df.empty:
            self.logger.warning(f"No market data for {current_ts}")
        
        self.logger.info(f"Processed market cap for {current_ts} vs {prev_ts}")

        return df

    def _fetch_previous_market_data(self, current_ts: datetime, prev_ts: datetime) -> pd.DataFrame:
        params = {
            'current_timestamp': current_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'previous_timestamp': prev_ts.strftime('%Y-%m-%d %H:%M:%S')
        }
        query = """
        EXEC sector_index.sp_GetPreviousMarketCapData_HIST 
        @current_timestamp = :current_timestamp, 
        @previous_timestamp = :previous_timestamp
        """
        df = self.db.fetch_dataframe(query, params)
        if df.empty:
            self.logger.warning(f"No previous market cap data for {current_ts}")
        return df
  

    def save_previous_market_cap_data(self) -> bool:
        """Save the current market cap data to database for use in next trading session"""
        if not self.prev_market_cap_data:
            self.logger.warning("No market cap data to save")
            return False

        try:
            self.logger.info("Saving market cap data for next trading day")

            # Start building queries with TRUNCATE first
            queries = ["TRUNCATE TABLE previous_market_cap_data"]

            for company, data in self.prev_market_cap_data.items():
                # Sanitize company name
                sanitized_company = company.replace("'", "''")

                # Format timestamp
                if isinstance(data['timestamp'], datetime.datetime):
                    timestamp_str = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    timestamp_str = str(data['timestamp'])

                # Handle NaN values - replace with NULL for SQL
                ltp = data['ltp'] if not pd.isna(data['ltp']) else 'NULL'
                total_shares = data['total_shares'] if not pd.isna(data['total_shares']) else 'NULL'
                market_cap = data['market_cap'] if not pd.isna(data['market_cap']) else 'NULL'
                free_float_pct = data['free_float_pct'] if not pd.isna(data['free_float_pct']) else 'NULL'
                free_float_mcap = data['free_float_mcap'] if not pd.isna(data['free_float_mcap']) else 'NULL'

                values_part = f"'{sanitized_company}', "
                values_part += f"{ltp}, "
                values_part += f"'{timestamp_str}', "
                values_part += f"{total_shares}, "
                values_part += f"{market_cap}, "
                values_part += f"{free_float_pct}, "
                values_part += f"{free_float_mcap}"

                insert_query = f"""
                INSERT INTO previous_market_cap_data (
                    company, 
                    ltp, 
                    timestamp, 
                    total_shares, 
                    market_cap, 
                    free_float_pct, 
                    free_float_mcap
                )
                VALUES ({values_part})
                """
                queries.append(insert_query)

            # Execute TRUNCATE + all INSERTs as one transaction
            success = self.db.execute_transaction(queries)

            if success:
                self.logger.info(f"Stored market cap data for {len(self.prev_market_cap_data)} companies")
            else:
                self.logger.error("Failed to store market cap data")

            return success
        except Exception as e:
            self.logger.exception(f"Error saving market cap data: {str(e)}")
            return False