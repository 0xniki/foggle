import time
import logging

import eth_account
from eth_account.signers.local import LocalAccount

from typing import Dict, List, Callable

from .exchange import Exchange
from .info import Info
from .constants import MAINNET_API_URL


class HL:
    def __init__(self):
        self._logger = logging.getLogger("hl_async.hl")

        self.info: Info = None
        self.exchange: Exchange = None
        self.address: str = None

        self._candle_cache = {}

        # self.topics = {
        #     "trades": self.msg_callback,
        #     "l2Book": self.msg_callback,
        # }


    async def connectAsync(self, config: Dict = None) -> None:
        self._logger.info("Connecting...")
        address, info, exchange = await self._setup(MAINNET_API_URL, key=config["key"], address=config["public"])
        vault = config["vault"] or None

        if exchange.account_address != exchange.wallet.address:
            raise Exception("You should not create an agent using an agent")
        
        approve_result, agent_key = await exchange.approve_agent()
        if approve_result["status"] != "ok":
            self._logger.error(f"Approving agent failed: {approve_result}")
            return
        
        agent_account: LocalAccount = eth_account.Account.from_key(agent_key)
        self._logger.info(f"Running with agent address: {agent_account.address}")

        if vault:
            agent_exchange = Exchange(wallet=agent_account, base_url=MAINNET_API_URL, 
                                      vault_address=vault)
            self.address = vault
        else:
            agent_exchange = Exchange(wallet=agent_account, base_url=MAINNET_API_URL, 
                                      account_address=address)
            self.address = address
        
        self.info = info
        self.exchange = agent_exchange

        await self.info.initialize()
        await self.exchange.initialize()

        await exchange.shutdown()
        self._logger.info("Connected to HyperLiquid")

    async def _setup(self, base_url=None, key=None, address=None, skip_ws=False):
        account: LocalAccount = eth_account.Account.from_key(key)
        if address == "":
            address = account.address
        self._logger.info(f"Running with account address: {address}")
        if address != account.address:
            self._logger.info(f"Running with agent address: {account.address}")
        info = Info(base_url, skip_ws)
        user_state = await info.user_state(address)
        spot_user_state = await info.spot_user_state(address)
        margin_summary = user_state["marginSummary"]
        if float(margin_summary["accountValue"]) == 0 and len(spot_user_state["balances"]) == 0:
            self._logger.error("Not running the example because the provided account has no equity.")
            url = info.base_url.split(".", 1)[1]
            error_string = f"No accountValue:\nIf you think this is a mistake, make sure that {address} has a balance on {url}.\nIf address shown is your API wallet address, update the config to specify the address of your account, not the address of the API wallet."
            raise Exception(error_string)
        exchange = Exchange(wallet=account, base_url=base_url, account_address=address)
        return address, info, exchange

    async def disconnect(self) -> None:
        self._logger.info("Disconnecting...")
        if self.exchange:
            try:
                await self.exchange.close()
                self._logger.debug("Exchange API session closed")
            except Exception as e:
                self._logger.error(f"Error closing Exchange API session: {e}")

        if self.info:
            try:
                await self.info.close()
                await self.info.ws_manager.stop()
                self._logger.debug("Info session closed")
            except Exception as e:
                self._logger.error(f"Error closing Info API session: {e}")

        self._logger.info("Disconnected from HyperLiquid")

    async def subscribe_trades(self, contract: Dict, callback: Callable):
        self._logger.debug(f"Subscribing to trades for {contract['symbol']}")
        subscription = {f"type": "trades", "coin": contract['symbol']}

        # TEMPORARY
        async def forward_data(message):
            data = self._format_trade_data(message)
            await callback(data)
        try:
            await self.info.subscribe(subscription=subscription, callback=forward_data)
            self._logger.debug(f"Subscribed to trades for {contract['symbol']}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to trades for {contract['symbol']}: {e}")
    
    def _format_trade_data(self, message: List[Dict]) -> List[Dict]:
        formatted_trades = []
        
        for trade in message['data']:
            contract_info = {
                "symbol": trade['coin'],
                "secType": "PERP",
                "exchange": "HYPERLIQUID",
                "multiplier": self.info.get_max_leverage(name=trade['coin']),
                "currency": "USD",
            }

            formatted_trade = {
                "contract": contract_info,
                "timestamp": trade.get('time', 0),
                "price": float(trade.get('px', 0)),
                "qty": float(trade.get('sz', 0)),
                "side": trade.get('side', ''),  # 'B' for buy, 'A' for ask/sell
                "hash": trade.get('hash', ''),
                "tid": trade.get('tid', 0),
                "users": trade.get('users', [])
            }
            
            formatted_trades.append(formatted_trade)
        
        return formatted_trades

    async def subscribe_orderbook(self, contract: Dict, callback: Callable):
        self._logger.debug(f"Subscribing to orderbook for {contract['symbol']}")
        subscription = {"type": "l2Book", "coin": contract['symbol']}

        async def forward_data(message):
            data = self._format_orderbook_data(message)
            await callback(data)
        
        try:
            await self.info.subscribe(subscription=subscription, callback=forward_data)
            self._logger.debug(f"Subscribed to orderbook for {contract['symbol']}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to orderbook for {contract['symbol']}: {e}")

    def _format_orderbook_data(self, message: Dict) -> Dict:
        data = message.get('data', {})

        contract_info = {
            "symbol": data['coin'],
            "secType": "PERP",
            "exchange": "HYPERLIQUID",
            "multiplier": self.info.get_max_leverage(name=data['coin']),
            "currency": "USD",
        }

        formatted_data = {
            "contract": contract_info,
            "timestamp": data['time'],
            "bids": [],
            "asks": []
        }
        
        if 'levels' in data and len(data['levels']) == 2:
            # Bids are at index 0, asks at index 1
            bids = data['levels'][0]
            asks = data['levels'][1]
            
            formatted_data["bids"] = [
                {
                    "price": float(level['px']),
                    "qty": float(level['sz']),
                    "orders": level['n']
                } 
                for level in bids
            ]
            
            formatted_data["asks"] = [
                {
                    "price": float(level['px']),
                    "qty": float(level['sz']),
                    "orders": level['n']
                } 
                for level in asks
            ]
        
        return formatted_data

    async def subscribe_candles(self, contract: Dict, callback: Callable, duration: str = '1 W', 
                            interval: str = '1 min'):
        """
        Subscribe to candle/OHLC data for a hyperliquid market
        
        Args:
            contract: Contract dictionary with market details
            callback: Callback function to process received data
            duration: Not used for hyperliquid (only needed for IBKR)
            interval: Candle interval e.g. '1m', '5m', '15m', '1h', '4h', '1d'
        """
        self._logger.debug(f"Subscribing to candles for {contract['symbol']}")
        
        # Convert interval format from IBKR style to hyperliquid style
        hl_interval = self._convert_interval_format(interval)
        
        subscription = {
            "type": "candle", 
            "coin": contract['symbol'], 
            "interval": hl_interval
        }

        async def forward_data(message):
            data = self._format_candle_data(message, contract, interval)
            await callback(data)
        
        try:
            await self.info.subscribe(subscription=subscription, callback=forward_data)
            self._logger.debug(f"Subscribed to candles for {contract['symbol']}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to candles for {contract['symbol']}: {e}")

    def _convert_interval_format(self, ibkr_interval: str) -> str:
        """Convert IBKR interval format to hyperliquid format"""
        # Map common IBKR intervals to hyperliquid intervals
        interval_map = {
            '1 min': '1m',
            '5 mins': '5m',
            '15 mins': '15m',
            '1 hour': '1h',
            '4 hours': '4h',
            '1 day': '1d',
        }
        
        if ibkr_interval in interval_map:
            return interval_map[ibkr_interval]
        
        # If not found, try a simple conversion
        if 'min' in ibkr_interval:
            minutes = ibkr_interval.split(' ')[0]
            return f"{minutes}m"
        elif 'hour' in ibkr_interval:
            hours = ibkr_interval.split(' ')[0]
            return f"{hours}h"
        
        # Default to 1m if can't convert
        self._logger.warning(f"Unknown interval format: {ibkr_interval}, defaulting to 1m")
        return "1m"

    def _format_candle_data(self, message: Dict, contract: Dict, interval: str) -> Dict:
        """
        Format candle data to match IBKR format, handling the case where
        we might get updates for the same time period.
        """
        candle = message.get('data')
        if not candle or not isinstance(candle, dict):
            return None
        
        contract_info = {
            "symbol": contract['symbol'],
            "secType": "PERP",
            "exchange": "HYPERLIQUID",
            "multiplier": self.info.get_max_leverage(name=contract['symbol']),
            "currency": "USD",
        }
        
        # Create a cache key for this symbol and interval
        cache_key = f"{contract['symbol']}-{interval}"
        
        # Current candle timestamp and data
        current_time = candle['t']
        current_candle = {
            "time": current_time,
            "open": float(candle['o']),
            "high": float(candle['h']),
            "low": float(candle['l']),
            "close": float(candle['c']),
            "volume": float(candle['v'])
        }
        
        # Initialize the cache if needed
        if cache_key not in self._candle_cache:
            self._candle_cache[cache_key] = []
        
        candles = self._candle_cache[cache_key]
        
        # Logic similar to the Bybit handler
        if candles and candles[-1]["time"] == current_time:
            # Update the latest candle as it's for the same time period
            candles[-1] = current_candle
        else:
            # This is a new candle, add it to our list
            candles.append(current_candle)
            # Keep only the most recent 2 candles (or more if needed)
            if len(candles) > 2:
                candles.pop(0)
        
        # Create the formatted data structure
        formatted_data = {
            "contract": contract_info,
            "timestamp": int(time.time() * 1000),
            "interval": interval,
            "bars": list(candles)  # Create a copy of the list
        }

        return formatted_data

    # @staticmethod
    # def _setup_multi_sig_wallets():
    #     config_path = os.path.join(os.path.dirname(__file__), "config.json")
    #     with open(config_path) as f:
    #         config = orjson.load(f)

    #     authorized_user_wallets = []
    #     for wallet_config in config["multi_sig"]["authorized_users"]:
    #         account: LocalAccount = eth_account.Account.from_key(wallet_config["secret_key"])
    #         address = wallet_config["account_address"]
    #         if account.address != address:
    #             raise Exception(f"provided authorized user address {address} does not match private key")
    #         print("loaded authorized user for multi-sig", address)
    #         authorized_user_wallets.append(account)
    #     return authorized_user_wallets
    