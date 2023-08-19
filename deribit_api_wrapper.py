from utils import * 
import asyncio
import websockets
import json
from datetime import datetime, timedelta

import pandas as pd

base_url = "wss://www.deribit.com/ws/api/v2"


class DeribitApiWrapper():
    def __init__(self, config) -> None:
        self.client_id = config['deribit']['client_id']
        self.client_secret = config['deribit']['client_secret']
        self.client_url = config['deribit']['client_url']
        self.json = {
            "jsonrpc" : "2.0",
            "id" : 1,
            "method" : None,
        }

    async def _private_api(self, request):
        options = {
            "grant_type" : "client_credentials",
            "client_id" : self.client_id,
            "client_secret" : self.client_secret
        }

        self.json["method"] = "public/auth"
        self.json["params"] = options

        async with websockets.connect(self.client_url) as websocket:
            await websocket.send(json.dumps(self.json))
            while websocket.open:
                response = await websocket.recv()
                
                # send a private subscription request
                if "private/subscribe" in request:
                    await websocket.send(request)
                    while websocket.open:
                        response = await websocket.recv()
                        response = json.loads(response)
                        print(response)

                # send a private method request
                else:
                    await websocket.send(request)
                    response = await websocket.recv()
                    response = json.loads(response)
                    break
            return response
        
    async def _public_api(self, request):
        async with websockets.connect(self.client_url) as websocket:
            await websocket.send(request)
            response = await websocket.recv()
            response = json.loads(response)
            return response

    async def public_sub(self, request):
        async with websockets.connect(self.client_url) as websocket:
            await websocket.send(request)
            while websocket.open:
                response = await websocket.recv()
                response = json.loads(response)
                print(response)
        
    async def _loop(self, api, request):
        # response = asyncio.get_event_loop().run_until_complete(
        #     api(json.dumps(request)))
        response = await api(json.dumps(request))
        return response
    
    def get_order_history_by_instrument(self, instrument_name):
       options = {
           "instrument_name" : instrument_name,
           "include_old" : True,
           "count" : 20
           }
       self.json["method"] = "private/get_order_history_by_instrument"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_user_trades_by_instrument(self, instrument_name, count = 100):
       options = {
           "instrument_name" : instrument_name,
           "count" : count
           }
       self.json["method"] = "private/get_user_trades_by_instrument"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_user_trades_by_currency(self, currency, count = 100):
       options = {
           "currency" : currency,
           "count" : count
           }
       self.json["method"] = "private/get_user_trades_by_currency"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_settlement_history_by_currency(self, currency, count = 100):
       options = {
           "currency" : currency,
           "count" : count
           }
       self.json["method"] = "private/get_settlement_history_by_currency"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_transaction_log(self, currency, count = 100):
       end = datetime.now()
       end_timestamp = datetime_to_unix_ms(end)
       start_timestamp = datetime_to_unix_ms(end - timedelta(weeks=52))
       options = {
           "currency" : currency,
           "count" : count,
           "start_timestamp" : start_timestamp,
           "end_timestamp" : end_timestamp
           }
       self.json["method"] = "private/get_transaction_log"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_positions(self, currency):
       options = {
           "currency" : currency
           }
       self.json["method"] = "private/get_positions"
       self.json["params"] = options
       return self._loop(self._private_api, self.json)
    
    def get_index_price(self, index_name):
        options = {"index_name" : index_name}
        self.json["method"] = "public/get_index_price"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
    
    def get_instrument_id(self, instrument_name):
        options = {
            "instrument_name" : instrument_name
            }
        self.json["method"] = "public/get_instrument"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
    
    def get_order_book_by_instrument_id(self, instrument_id, depth=1):
        options = {
            "instrument_id" : instrument_id,
            "depth" : depth
            }
        self.json["method"] = "public/get_order_book_by_instrument_id"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
    
    def get_order_book_by_instrument(self, instrument_name, depth=1):
        options = {
            "instrument_name" : instrument_name,
            "depth" : depth
            }
        self.json["method"] = "public/get_order_book"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
    
    def get_last_settlements_by_instrument(self, instrument_name, type='settlement'):
        options = {
            "instrument_name" : instrument_name,
            "type" : type
            }
        self.json["method"] = "public/get_last_settlements_by_instrument"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
    
    def get_delivery_prices(self, index_name, offset=None, count=10):
        options = {
            "index_name" : index_name,
            "count" : count,
            "offset" : offset
            }
        self.json["method"] = "public/get_delivery_prices"
        self.json["params"] = options
        return self._loop(self._public_api, self.json)
