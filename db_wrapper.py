import sqlite3
from datetime import datetime, timedelta
from utils import *
import pandas as pd

class DBWrapper():
    def __init__(self, db_path) -> None:
        """
        Initializes the DBWrapper instance and establishes a connection to the database.

        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.logger = set_logger(name=__name__, log_file='db_wrapper.log', log_level='INFO')

        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()          

    def _create_tables(self):
        """
        Creates the necessary tables if they don't exist in the database.
        """
        create_table_sql = '''
            CREATE TABLE IF NOT EXISTS trades (
                trade_seq INTEGER,
                trade_id INTEGER,
                timestamp TEXT,
                avg_px REAL,
                tick_direction TEXT,
                state TEXT,
                self_trade INTEGER,
                risk_reducing INTEGER,
                reduce_only INTEGER,
                profit_loss REAL,
                price REAL,
                post_only INTEGER,
                order_type TEXT,
                order_id INTEGER,
                mmp TEXT,
                matching_id INTEGER,
                mark_price REAL,
                liquidity TEXT,
                instrument_name TEXT,
                index_price REAL,
                fee_currency TEXT,
                fee REAL,
                direction TEXT,
                api TEXT,
                amount REAL,
                underlying_price REAL,
                iv REAL,
                advanced TEXT,
                datetime TEXT,
                currency TEXT,
                PRIMARY KEY (trade_id, currency)
            );
            '''
        self.cursor.execute(create_table_sql)

        create_table_sql = '''
            CREATE TABLE IF NOT EXISTS transaction_logs (
                username TEXT,
                user_seq INTEGER,
                user_role TEXT,
                user_id INTEGER,
                type TEXT,
                trade_id INTEGER,
                timestamp TEXT,
                side TEXT,
                profit_as_cashflow TEXT,
                price_currency TEXT,
                price REAL,
                position REAL,
                order_id INTEGER,
                mark_price REAL,
                interest_pl REAL,
                instrument_name TEXT,
                info TEXT,
                index_price REAL,
                id INTEGER,
                fee_balance REAL,
                equity REAL,
                currency TEXT,
                commission REAL,
                change REAL,
                cashflow REAL,
                balance REAL,
                amount REAL,
                total_interest_pl REAL,
                session_upl REAL,
                session_rpl REAL,
                combo_trade_id INTEGER,
                combo_id INTEGER,
                role TEXT,
                datetime TEXT,
                PRIMARY KEY (id, currency)
            );
            '''

        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def save_to_db(self, df, table_name, if_exists='append', index=False):
        """
        Saves a DataFrame to the specified database table.

        Args:
            df (pd.DataFrame): DataFrame to be saved.
            table_name (str): Name of the database table.
            if_exists (str): Behavior when the table already exists ('fail', 'replace', 'append').
            index (bool): Whether to include the index in the table.

        Returns:
            None
        """
        df = self._exclude_existing_records_from_df(df, table_name)
        df = self._convert_dtypes(df)
        df.to_sql(name=table_name, con=self.conn, if_exists=if_exists, index=index)
        self.conn.commit()

    def _get_unique_keys(self, table_name):
        """
        Retrieves primary keys and all key combinations from a table.

        Args:
            table_name (str): Name of the database table.

        Returns:
            primary_keys (list): List of primary key column names.
            all_keys_combinations (list): List of tuples representing existing key combinations.
        """
        self.cursor.execute(f"SELECT * FROM pragma_table_info('{table_name}') WHERE pk")
        table_info = self.cursor.fetchall()

        primary_keys = [column[1] for column in table_info]

        all_keys_combinations_sql = f'''
            SELECT {primary_keys[0]}, {primary_keys[1]}
            FROM {table_name}
            '''
        
        self.cursor.execute(all_keys_combinations_sql)
        all_keys_combinations = self.cursor.fetchall()

        return primary_keys, all_keys_combinations
    
    def _convert_dtypes(self, df):
        """
        Converts DataFrame column data types for compatibility.

        Args:
            df (pd.DataFrame): DataFrame to be converted.

        Returns:
            df (pd.DataFrame): Converted DataFrame.
        """
        for column in df.columns:
            df[column] = df[column].apply(convert_to_int_or_str)
        return df
    
    def _exclude_existing_records_from_df(self, df, table_name):
        """
        Excludes existing records from the DataFrame based on primary keys.

        Args:
            df (pd.DataFrame): DataFrame to be filtered.
            table_name (str): Name of the database table.

        Returns:
            df_filtered (pd.DataFrame): Filtered DataFrame without existing records.
        """
        table_keys, existing_records = self._get_unique_keys(table_name)

        def filter_pairs(row):
            return (convert_to_int_or_str(row[table_keys[0]]), convert_to_int_or_str(row[table_keys[1]])) not in existing_records

        df_filtered = df[df.apply(filter_pairs, axis=1)]
        self.logger.info(f"Number of new rows saved in DB : {df_filtered.shape[0]}")
        return df_filtered

    def get_transactions_by_datetime_range(self, start_range=None, end_range=None):
        """
        Retrieves transactions within the specified datetime range.

        Args:
            start_range (datetime): Start of the datetime range.
            end_range (datetime): End of the datetime range.

        Returns:
            transactions_df (pd.DataFrame): DataFrame containing transactions within the range.
        """
        if end_range == None:
            end_range = datetime.now()
        if start_range == None:
            start_range = end_range - timedelta(weeks=2)

        sql_query = f'''
            SELECT *
            FROM transaction_logs
            WHERE timestamp >= {datetime_to_unix_ms(start_range)} AND timestamp <= {datetime_to_unix_ms(end_range)}
            '''
        return pd.read_sql_query(sql_query, self.conn)
    
    def get_trades_by_datetime_range(self, start_range=None, end_range=None):
        """
        Retrieves trades within the specified datetime range.

        Args:
            start_range (datetime): Start of the datetime range.
            end_range (datetime): End of the datetime range.

        Returns:
            trades_df (pd.DataFrame): DataFrame containing trades within the range.
        """
        if end_range == None:
            end_range = datetime.now()
        if start_range == None:
            start_range = end_range - timedelta(weeks=2)

        sql_query = f'''
            SELECT *
            FROM trades
            WHERE timestamp >= {datetime_to_unix_ms(start_range)} AND timestamp <= {datetime_to_unix_ms(end_range)}
            '''
        return pd.read_sql_query(sql_query, self.conn)

