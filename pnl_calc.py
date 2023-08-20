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
        self.config = config
        self.trades = pd.DataFrame()
        self.deribit_wrapper = deribit_wrapper
        self.db_wrapper = db_wrapper
        self.start_calc_date = start_range
        self.end_calc_date = end_range
        self._load_trades()

        self.instrument_live_prices = {}

    def _calc_expiry(self, instrument_name):
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
        return side.split(' ')[1]
                   
    def _load_trades(self):

        async def load_transactions_from_deribit():
            for ccy in self.config['deribit']['currencies']:
                transactions_from_deribit = await self.deribit_wrapper.get_transaction_log(currency=ccy, count=1000)
                transactions_from_deribit = pd.DataFrame(transactions_from_deribit['result']['logs'])
                self.db_wrapper.save_to_db(transactions_from_deribit, table_name="transaction_logs")

        asyncio.run(load_transactions_from_deribit())
        
        transactions = self.db_wrapper.get_transactions_by_datetime_range(self.start_calc_date,
                                                                          self.end_calc_date)
        transactions = transactions[(transactions['type'] == 'trade') & (~transactions['instrument_name'].str.contains('_'))].reset_index(drop=True)
        transactions[['trade_type', 'expiry','strike','cp']] = pd.DataFrame(transactions['instrument_name'].apply(self._calc_expiry).to_list())
        transactions['direction'] = transactions['side'].apply(self._get_direction)
        transactions['timestamp'] = transactions['timestamp'].apply(int)
        transactions['datetime'] = transactions['timestamp'].apply(unix_ms_to_datetime)
        self.trades = transactions

    def _get_trades(self):
        return self.trades
    
    async def _calc_settlement_price(self, instrument):
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
        instruments = self.trades['instrument_name'].unique()
        ccy_list = self.trades['currency'].unique()
        tasks_instruments = [self._update_instrument_price(instrument) for instrument in instruments]
        tasks_ccy = [self._update_ccy_price(ccy) for ccy in ccy_list]

        batch_size = 25
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
        _, expiry, _, _ = self._calc_expiry(instrument)

        if isinstance(expiry, datetime) and expiry < datetime.now():
            # expired instrument
            return await self._calc_settlement_price(instrument)
        else:
            result = await self.deribit_wrapper.get_order_book_by_instrument(instrument)
            return result['result']['mark_price']

    async def _update_ccy_price(self, ccy):
        result = await self.deribit_wrapper.get_index_price(f"{ccy.lower()}_usd")
        return result['result']['index_price']


    def usd_pnl_by_trade(self, trade, include_fees=True):
        usd_pnl = None
        if trade['trade_type'] == 'option':
            usd_pnl = (self.instrument_live_prices[trade['instrument_name']] * self.instrument_live_prices[trade['currency']] \
                       - trade['price'] * trade['index_price']) \
                        * trade['amount']
        elif trade['trade_type'] == 'future':
            usd_pnl =  (self.instrument_live_prices[trade['instrument_name']] -  trade['price']) * trade['amount'] / trade['index_price']
        usd_pnl = usd_pnl  * (1 if trade['direction'] == 'buy' else -1)

        usd_fees = trade['commission'] * trade['index_price']
        return usd_pnl, usd_pnl - usd_fees, usd_fees
    
    def usd_fee_by_trade(self, trade):
        fee_usd = None
    
    def update_pnl(self):
        asyncio.run(self.update_live_prices())

        for idx, trade in self.trades.iterrows():
            usd_pnl, usd_pnl_including_fees, usd_fees = self.usd_pnl_by_trade(trade)
            self.trades.loc[idx, 'usd_pnl'] = usd_pnl
            self.trades.loc[idx, 'usd_pnl_including_fees'] = usd_pnl_including_fees
            self.trades.loc[idx, 'usd_fees'] = usd_fees
