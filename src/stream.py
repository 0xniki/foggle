import logging

from typing import Dict, List

from .feed import Feed
from .store.timescaledb.db import Database
from .exchanges.base import Exchange

class Stream:
    def __init__(self, feed: Feed, db: Database):
        self.feed = feed
        self.db = db

        self.exchanges = {}

        self._logger = logging.getLogger("stream")

    def add_exchanges(self, exchange: Dict[str, Exchange]):
        self.exchanges.update(exchange)

    async def subscribe_trades(self, exchange: str, contract: Dict) -> None:
        try:
            exchange = self.exchanges.get(exchange)
            if not exchange:
                self._logger.warning(f"{exchange} was not found in directory.")
                return
            await exchange.subscribe_trades(contract, self._trades_callback)
        except Exception as e:
            self._logger.error(e)

    async def subscribe_orderbook(self, exchange: str, contract: Dict) -> None:
        try:
            exchange = self.exchanges.get(exchange)
            if not exchange:
                self._logger.warning(f"{exchange} was not found in directory.")
                return
            await exchange.subscribe_orderbook(contract, self._orderbook_callback)
        except Exception as e:
            self._logger.error(e)

    async def subscribe_candles(self, exchange: str, contract: Dict, duration: str = '1 W', interval: str = '1 min'):
        try:
            exchange = self.exchanges.get(exchange)
            if not exchange:
                self._logger.warning(f"{exchange} was not found in directory.")
                return
            await exchange.subscribe_candles(contract=contract, 
                                             callback = self._candles_callback, 
                                             duration = duration, 
                                             interval = interval)
        except Exception as e:
            self._logger.error(e)

    async def _trades_callback(self, trades: List):
        await self.db.insert_trades(trades)
        # print(trades)

    async def _orderbook_callback(self, orderbook: Dict):
        await self.db.insert_orderbook(orderbook)
        # print(orderbook)

    async def _candles_callback(self, bars: Dict):
        await self.db.insert_candles(bars)
        # print(bars)
