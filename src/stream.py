import asyncio
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

    async def _trades_callback(self, trades: List):
        await self.db.insert_trades(trades)