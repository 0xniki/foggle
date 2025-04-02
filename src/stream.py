import logging
import asyncio
from typing import Dict, List, Callable, Optional, Any

from .feed import Feed
from .store.timescaledb.db import Database
from .exchanges.base import Exchange

class Stream:
    def __init__(self, feed: Feed, db: Database):
        self.feed = feed
        self.db = db
        self.exchanges = {}
        self._logger = logging.getLogger("stream")

    def add_exchanges(self, exchange: Dict[str, Exchange]) -> None:
        self.exchanges.update(exchange)

    def _get_exchange(self, exchange_name: str) -> Optional[Exchange]:
        """Helper method to get exchange and log warning if not found."""
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            self._logger.warning(f"{exchange_name} was not found in directory.")
        return exchange

    async def subscribe_trades(self, exchange: str, contract: Dict) -> None:
        """Subscribe to trades for a specific contract on an exchange."""
        try:
            exchange_obj = self._get_exchange(exchange)
            if not exchange_obj:
                return
                
            contract_info = self._format_contract_info(contract)
            self._logger.debug(f"Subscribing to trades for {contract_info} via {exchange}")
            
            await exchange_obj.subscribe_trades(contract, self._trades_callback)
            
            self._logger.info(f"Subscribed to trades for {contract_info} via {exchange}")
        except Exception as e:
            contract_info = self._format_contract_info(contract)
            self._logger.error(f"Error subscribing to trades for {contract_info} via {exchange}: {e}")

    async def subscribe_orderbook(self, exchange: str, contract: Dict) -> None:
        """Subscribe to orderbook for a specific contract on an exchange."""
        try:
            exchange_obj = self._get_exchange(exchange)
            if not exchange_obj:
                return
                
            contract_info = self._format_contract_info(contract)
            self._logger.debug(f"Subscribing to orderbook for {contract_info} via {exchange}")
            
            await exchange_obj.subscribe_orderbook(contract, self._orderbook_callback)
            
            self._logger.info(f"Subscribed to orderbook for {contract_info} via {exchange}")
        except Exception as e:
            contract_info = self._format_contract_info(contract)
            self._logger.error(f"Error subscribing to orderbook for {contract_info} via {exchange}: {e}")

    async def subscribe_candles(self, exchange: str, contract: Dict, duration: str = '1 W', interval: str = '1 min') -> None:
        """Subscribe to candles for a specific contract on an exchange."""
        try:
            exchange_obj = self._get_exchange(exchange)
            if not exchange_obj:
                return
                
            contract_info = self._format_contract_info(contract)
            self._logger.debug(f"Subscribing to {interval} candles for {contract_info} via {exchange} (duration: {duration})")
            
            await exchange_obj.subscribe_candles(
                contract=contract,
                callback=self._candles_callback,
                duration=duration,
                interval=interval
            )
            
            self._logger.info(f"Subscribed to {interval} candles for {contract_info} via {exchange}")
        except Exception as e:
            contract_info = self._format_contract_info(contract)
            self._logger.error(f"Error subscribing to {interval} candles for {contract_info} via {exchange}: {e}")
        
    async def subscribe_all(self, exchange: str, contract: Dict, duration: str = '1 W', interval: str = '1 min') -> None:
        """Subscribe to all available data streams for a contract simultaneously."""
        try:
            exchange_obj = self._get_exchange(exchange)
            if not exchange_obj:
                return

            contract_info = self._format_contract_info(contract)
                
            tasks = []

            tasks.append(self.subscribe_trades(exchange, contract))
            tasks.append(self.subscribe_orderbook(exchange, contract))
            tasks.append(self.subscribe_candles(exchange, contract, duration, interval))

            await asyncio.gather(*tasks)
            
        except Exception as e:
            contract_info = self._format_contract_info(contract)
            self._logger.error(f"Error in subscribe_all for {contract_info} via {exchange}: {e}")

    async def _trades_callback(self, trades: List) -> None:
        """Process incoming trades data."""
        await self.db.insert_trades(trades)
        # await self.feed.publish_trades(trades)

    async def _orderbook_callback(self, orderbook: Dict) -> None:
        """Process incoming orderbook data."""
        await self.db.insert_orderbook(orderbook)
        # await self.feed.publish_orderbook(orderbook)

    async def _candles_callback(self, bars: Dict) -> None:
        """Process incoming candle data."""
        await self.db.insert_candles(bars)
        # await self.feed.publish_candles(bars)

    def _format_contract_info(self, contract: Dict) -> str:
        """Format contract details for logging in a standardized way."""
        sec_type = contract.get('secType', '')
        symbol = contract.get('symbol', '')
        contract_exchange = contract.get('exchange', '')

        info = f"{sec_type}:{symbol} @ {contract_exchange}"

        if sec_type == 'OPT':
            expiration = contract.get('expiration', '')
            strike = contract.get('strike', '')
            right = contract.get('right', '')
            info = f"{sec_type}:{symbol} {expiration} {strike} {right} @ {contract_exchange}"
        elif sec_type == 'FUT':
            expiration = contract.get('expiration', '')
            info = f"{sec_type}:{symbol} {expiration} @ {contract_exchange}"

        currency = contract.get('currency', 'USD')
        if currency != 'USD':
            info += f" ({currency})"
            
        return info
