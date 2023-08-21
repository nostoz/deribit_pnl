import streamlit as st
import pandas as pd
from deribit_api_wrapper import DeribitApiWrapper
from db_wrapper import DBWrapper
from pnl_calc import PnLCalculator
from utils import *
import asyncio
from datetime import timedelta

def main():
    st.title("Deribit PnL calculator")

    start = datetime(2023, 8, 1)
    end = datetime.now()
    date_range = st.date_input(
        "Date range for pnl calculations",
        (start, end),
        end - timedelta(weeks=52),
        end
    )
    
    if len(date_range) > 1:
        start_range = datetime.combine(date_range[0], datetime.min.time())
        end_range = datetime.combine(date_range[1], datetime.min.time())
        pnl_calc = PnLCalculator(config,
                             db_wrapper=db_wrapper,
                             deribit_wrapper=deribit_wrapper,
                             start_range=start_range,
                             end_range=end_range)
        positions = pnl_calc.update_pnl()
        raw_data_with_pnl = pnl_calc._get_trades()
        
        # filtered_data = raw_data_with_pnl[(raw_data_with_pnl['datetime'] >= datetime.combine(date_range[0], datetime.min.time())) \
        #                                   & (raw_data_with_pnl['datetime'] <= datetime.combine(date_range[1], datetime.min.time()))]

        col1, col2 = st.columns(2)
        with col1:
            st.write("PnL summary:")
            st.dataframe(raw_data_with_pnl.pivot_table(index='currency', values=['usd_pnl', 'usd_pnl_including_fees', 'usd_fees'], aggfunc=sum, margins=True))

        with col2:
            st.write("PnL realized/unrealized")
            st.dataframe(positions.pivot_table(index='currency', values=['realized_pl', 'unrealized_pl'], aggfunc=sum, margins=True))

        st.write("PnL by instrument:")
        st.dataframe(raw_data_with_pnl.pivot_table(index='instrument_name', values=['usd_pnl', 'usd_pnl_including_fees', 'usd_fees'], columns='currency', aggfunc=sum, margins=True))

        if st.button("Refresh PnL"):
            pnl_calc.update_pnl()

if __name__ == "__main__":
    config = read_json('config.json')
    db_wrapper = DBWrapper(config['db_path'])
    deribit_wrapper = DeribitApiWrapper(config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main()
