import pandas as pd
from deribit_api_wrapper import DeribitApiWrapper
from db_wrapper import DBWrapper
from utils import *
import asyncio


class PnLCalculator():
    def __init__(self, config,
                       deribit_wrapper:DeribitApiWrapper,
                       db_wrapper:DBWrapper,
                       start_range, end_range) -> None:
        """
        Initialize the PnLCalculator.

        Args:
            deribit_wrapper (DeribitApiWrapper): Instance of DeribitApiWrapper.
            db_wrapper (DBWrapper): Instance of DBWrapper.
            start_range (datetime): Start date for PnL calculations.
            end_range (datetime): End date for PnL calculations.
        """
        self.config = config
        self.trades = pd.DataFrame()
        self.deribit_wrapper = deribit_wrapper
        self.db_wrapper = db_wrapper
        self.start_calc_date = start_range
        self.end_calc_date = end_range
        self._load_trades()

        self.instrument_live_prices = {}

    def _calc_expiry(self, instrument_name):
        """
        Calculate the details for the given instrument.

        Args:
            instrument_name (str): Name of the instrument.

        Returns:
            tuple: Tuple containing trade type, expiry, strike, and call/put.
        """
        strike = None
        cp = None
        if 'PERP' in instrument_name:
            return 'future', 'PERP', strike, cp
        else:
            instrument_breakdown = instrument_name.split('-')
            if len(instrument_breakdown) > 2:
                #option
                strike = instrument_breakdown[2]
                cp = instrument_breakdown[3]
            elif len(instrument_breakdown) == 1:
                return 'SPOT'

            return 'option', convert_from_deribit_date(instrument_breakdown[1], as_string=False), strike, cp
    
    def _get_direction(self, side):
        """
        Extract the direction from the given side.

        Args:
            side (str): Side of the trade.

        Returns:
            str: Direction of the trade (buy/sell).
        """
        return side.split(' ')[1]
    
    def _process_transactions_from_db(self, transactions:pd.DataFrame):
        transactions = transactions[(transactions['type'] == 'trade') & (~transactions['instrument_name'].str.contains('_'))].reset_index(drop=True)
        transactions[['trade_type', 'expiry','strike','cp']] = pd.DataFrame(transactions['instrument_name'].apply(self._calc_expiry).to_list())
        transactions['direction'] = transactions['side'].apply(self._get_direction)
        transactions['timestamp'] = transactions['timestamp'].apply(int)
        transactions['datetime'] = transactions['timestamp'].apply(unix_ms_to_datetime)
        transactions.loc[transactions['trade_type'] == 'future', 'amount'] = transactions.loc[transactions['trade_type'] == 'future', 'amount'] / transactions.loc[transactions['trade_type'] == 'future', 'index_price']

        return transactions
                   
    def _load_trades(self):
        """
        Loads trades and transaction data from the database and Deribit API.
        """
        async def load_transactions_from_deribit():
            for ccy in self.config['deribit']['currencies']:
                transactions_from_deribit = await self.deribit_wrapper.get_transaction_log(currency=ccy, count=1000)
                transactions_from_deribit = pd.DataFrame(transactions_from_deribit['result']['logs'])
                self.db_wrapper.save_to_db(transactions_from_deribit, table_name="transaction_logs")

        asyncio.run(load_transactions_from_deribit())
        
        transactions = self.db_wrapper.get_transactions_by_datetime_range(self.start_calc_date,
                                                                          self.end_calc_date)
        self.trades = self._process_transactions_from_db(transactions)

    def _get_trades(self):
        """
        Returns the list of trades.

        Returns:
            trades: DataFrame containing trade data.
        """
        return self.trades
    
    async def _calc_settlement_price(self, instrument):
        """
        Calculates the settlement price for a given instrument.

        Args:
            instrument: Name of the instrument.

        Returns:
            settlement_price: Calculated settlement price.
        """
        trade_type, expiry, strike, cp = self._calc_expiry(instrument)
        result = await self.deribit_wrapper.get_delivery_prices((f"{instrument.split('-')[0]}_usd").lower(),
                                                                    offset=(datetime.now()-expiry).days)
        index_settlement = result['result']['data'][0]['delivery_price']

        if trade_type == 'future':
            return index_settlement
        elif trade_type == 'option':
            strike = int(strike)
            return (max(0, (strike - index_settlement)) if cp == 'P' else max(0, (index_settlement - strike))) / index_settlement
        else:
            raise Exception('trade type not supported')

    async def update_live_prices(self):
        """
        Updates live prices for instruments and currencies.
        """
        instruments = self.trades['instrument_name'].unique()
        ccy_list = self.trades['currency'].unique()
        tasks_instruments = [self._update_instrument_price(instrument) for instrument in instruments]
        tasks_ccy = [self._update_ccy_price(ccy) for ccy in ccy_list]

        batch_size = 20
        num_batches = len(tasks_instruments) // batch_size + (1 if len(tasks_instruments) % batch_size > 0 else 0)

        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = start_idx + batch_size
            batch_tasks = tasks_instruments[start_idx:end_idx]

            results = await asyncio.gather(*batch_tasks)

            for instrument, px in zip(instruments[start_idx:end_idx], results):
                self.instrument_live_prices[instrument] = px

            if batch_idx < num_batches - 1:
                await asyncio.sleep(1)

        results = await asyncio.gather(*tasks_ccy)
        for ccy, ccy_px in zip(ccy_list, results):
            self.instrument_live_prices[ccy] = ccy_px

    async def _update_instrument_price(self, instrument):
        """
        Updates the live price of a specific instrument.

        Args:
            instrument: Name of the instrument.

        Returns:
            price: Updated live price.
        """
        _, expiry, _, _ = self._calc_expiry(instrument)

        if isinstance(expiry, datetime) and expiry < datetime.now():
            # expired instrument
            return await self._calc_settlement_price(instrument)
        else:
            result = await self.deribit_wrapper.get_order_book_by_instrument(instrument)
            return result['result']['mark_price']

    async def _update_ccy_price(self, ccy):
        """
        Updates the live price of a specific currency.

        Args:
            ccy: Currency code.

        Returns:
            price: Updated currency price.
        """
        result = await self.deribit_wrapper.get_index_price(f"{ccy.lower()}_usd")
        return result['result']['index_price']


    def usd_pnl_by_trade(self, trade, include_fees=True):
        """
        Calculates USD PnL for a trade.

        Args:
            trade: Trade information.
            include_fees: Flag to include fees in PnL calculation.

        Returns:
            usd_pnl: Calculated USD PnL.
            usd_pnl_including_fees: Calculated USD PnL including fees.
            usd_fees: Calculated USD fees.
        """
        usd_pnl = None
        if trade['trade_type'] == 'option':
            usd_pnl = (self.instrument_live_prices[trade['instrument_name']] * self.instrument_live_prices[trade['currency']] \
                       - trade['price'] * trade['index_price']) \
                        * trade['amount']
        elif trade['trade_type'] == 'future':
            usd_pnl =  (self.instrument_live_prices[trade['instrument_name']] -  trade['price']) * trade['amount'] 
        usd_pnl = usd_pnl  * (1 if trade['direction'] == 'buy' else -1)

        usd_fees = trade['commission'] * trade['index_price']
        return usd_pnl, usd_pnl - usd_fees, usd_fees
        
    def update_pnl(self):
        """
        Updates PnL for all trades.
        """
        asyncio.run(self.update_live_prices())

        for idx, trade in self.trades.iterrows():
            usd_pnl, usd_pnl_including_fees, usd_fees = self.usd_pnl_by_trade(trade)
            self.trades.loc[idx, 'usd_pnl'] = usd_pnl
            self.trades.loc[idx, 'usd_pnl_including_fees'] = usd_pnl_including_fees
            self.trades.loc[idx, 'usd_fees'] = usd_fees
        
        positions = self.calculate_positions()
        return positions
    
    def calculate_positions(self, currency='usd'):
        if currency == 'usd':
            self.trades.loc[self.trades['trade_type'] == 'option', 'price'] = self.trades.loc[self.trades['trade_type'] == 'option', 'price']\
                                                                * (self.trades.loc[self.trades['trade_type'] == 'option', 'index_price'])
        positions = pd.DataFrame() 
        for instrument in self.trades['instrument_name'].unique():
            trades_subset = self.trades[self.trades['instrument_name'] == instrument].sort_values(by='timestamp', ascending=True).reset_index(drop=True)
            for idx, row in trades_subset.iterrows():
                trades_subset.loc[idx, 'buy'] = trades_subset[trades_subset['direction'] == 'buy'].loc[0:idx, 'amount'].sum()
                trades_subset.loc[idx, 'sell'] = trades_subset[trades_subset['direction'] == 'sell'].loc[0:idx, 'amount'].sum()
                trades_subset.loc[idx, 'long/short'] = 'long' if trades_subset.loc[idx, 'buy'] > trades_subset.loc[idx, 'sell'] else 'short'

                buy_amount_subset = trades_subset[trades_subset['direction'] == 'buy'].loc[0:idx, 'amount']
                buy_price_subset = trades_subset[trades_subset['direction'] == 'buy'].loc[0:idx, 'price']
                sell_amount_subset = trades_subset[trades_subset['direction'] == 'sell'].loc[0:idx, 'amount']
                sell_price_subset = trades_subset[trades_subset['direction'] == 'sell'].loc[0:idx, 'price']
                
                if not buy_amount_subset.empty:
                    trades_subset.loc[idx, 'avg_long'] = buy_amount_subset.dot(buy_price_subset) / buy_amount_subset.sum()
                else:
                    trades_subset.loc[idx, 'avg_long'] = 0

                if not sell_amount_subset.empty:
                    trades_subset.loc[idx, 'avg_short'] = sell_amount_subset.dot(sell_price_subset) / sell_amount_subset.sum()
                else:
                    trades_subset.loc[idx, 'avg_short'] = 0
                
            last_idx = trades_subset.index[-1]
            trades_subset.loc[last_idx, 'avg_long_to_short'] = trades_subset.loc[(trades_subset['buy'] < trades_subset['sell']).index[-1], 'avg_long']
            trades_subset.loc[last_idx, 'avg_short_to_long'] = trades_subset.loc[(trades_subset['sell'] < trades_subset['buy']).index[-1], 'avg_short']
            

            last_row = trades_subset.iloc[last_idx]
            if last_row['long/short'] == 'long':
                trades_subset.loc[last_idx, 'realized_pl'] = (last_row['avg_short'] - last_row['avg_long_to_short']) * last_row['sell'] 
            else:
                trades_subset.loc[last_idx, 'realized_pl'] = (last_row['avg_short_to_long'] - last_row['avg_long']) * last_row['buy']

            # print(trades_subset)
            live_price = self.instrument_live_prices[instrument] * (1 if last_row['trade_type'] == 'future' else self.instrument_live_prices[last_row['currency']])
            trades_subset.loc[last_idx, 'unrealized_pl'] = (live_price - last_row['avg_long']) * last_row['buy'] \
                                                         + (last_row['avg_short'] - live_price) * last_row['sell'] \
                                                         - trades_subset.loc[last_idx, 'realized_pl']
            
            # if trades_subset.loc[0, 'trade_type'] == 'option':
            #     trades_subset.loc[last_idx, 'realized_pl'] = trades_subset.loc[last_idx, 'realized_pl']  * self.instrument_live_prices[last_row['currency']]
            #     trades_subset.loc[last_idx, 'unrealized_pl'] = trades_subset.loc[last_idx, 'unrealized_pl'] * self.instrument_live_prices[last_row['currency']]

            positions = pd.concat([positions, pd.DataFrame(trades_subset.loc[last_idx]).T], axis=0)
            # print(trades_subset)

        return positions
        
        

if __name__ == "__main__":
    config = read_json('config.json')
    db_wrapper = DBWrapper(config['db_path'])
    deribit_wrapper = DeribitApiWrapper(config)
    start = datetime(2023, 8, 1)
    end = datetime.now()

    pnl_calc = PnLCalculator(config,
                             db_wrapper=db_wrapper,
                             deribit_wrapper=deribit_wrapper,
                             start_range=start,
                             end_range=end)
    
    positions = pnl_calc.calculate_positions()
    print(positions)
    print(positions['realized_pl'].sum())
    print(positions['unrealized_pl'].sum())