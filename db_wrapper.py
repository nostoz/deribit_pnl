import sqlite3
from datetime import datetime, timedelta
from utils import *
import pandas as pd

class DBWrapper():
    def __init__(self, db_path) -> None:
        self.logger = set_logger(name=__name__, log_file='db_wrapper.log', log_level='INFO')

        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()          

    def _create_tables(self):
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
        df = self._exclude_existing_records_from_df(df, table_name)
        df = self._convert_dtypes(df)
        df.to_sql(name=table_name, con=self.conn, if_exists=if_exists, index=index)
        self.conn.commit()

    def _get_unique_keys(self, table_name):
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
        for column in df.columns:
            df[column] = df[column].apply(convert_to_int_or_str)
        return df
    
    def _exclude_existing_records_from_df(self, df, table_name):
        table_keys, existing_records = self._get_unique_keys(table_name)

        def filter_pairs(row):
            return (convert_to_int_or_str(row[table_keys[0]]), convert_to_int_or_str(row[table_keys[1]])) not in existing_records

        df_filtered = df[df.apply(filter_pairs, axis=1)]
        self.logger.info(f"Number of new rows saved in DB : {df_filtered.shape[0]}")
        return df_filtered

    def get_transactions_by_datetime_range(self, start_range=None, end_range=None):
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

        
if __name__ == '__main__':
    
    config = read_json('config.json')
    db_wrapper = DBWrapper(config['db_path'])

    start = datetime(2022,1,1)
    print(db_wrapper.get_transactions_by_datetime_range(start_range=start))
    # print(db_wrapper.get_trades_by_datetime_range(start_range=start))