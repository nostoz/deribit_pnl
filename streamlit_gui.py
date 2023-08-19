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

    config = read_json('config.json')
    db_wrapper = DBWrapper(config['db_path'])
    deribit_wrapper = DeribitApiWrapper(config)

    start = datetime(2022, 8, 1)
    end = datetime.now()

    date_range = st.date_input(
        "Date range for pnl calculations",
        (start, end),
        start,
        end
    )

    pnl_calc = PnLCalculator(db_wrapper=db_wrapper,
                             deribit_wrapper=deribit_wrapper,
                             start_range=start,
                             end_range=end)
    
    pnl_calc.update_pnl()
    raw_data = pnl_calc._get_trades()

    filtered_data = raw_data[(raw_data['datetime'] >= datetime.combine(date_range[0], datetime.min.time())) & (raw_data['datetime'] <= datetime.combine(date_range[1], datetime.min.time()))]

    st.write("PnL summary:")
    st.dataframe(filtered_data.pivot_table(index='currency', values='usd_pnl', aggfunc=sum, margins=True))

    st.write("PnL by instrument:")
    st.dataframe(filtered_data.pivot_table(index='instrument_name', values='usd_pnl', columns='currency', aggfunc=sum, margins=True))

    if st.button("Refresh PnL"):
        pnl_calc.update_pnl()
    # st.dataframe(filtered_data.pivot_table(index=['currency', 'instrument_name'], values='usd_pnl', aggfunc=sum, margins=True))

    # positions = pd.concat([pd.DataFrame(deribit_wrapper.get_positions('BTC')['result']), 
    #                        pd.DataFrame(deribit_wrapper.get_positions('ETH')['result'])], 
    #                        axis=0)
    # st.dataframe(positions)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main()
