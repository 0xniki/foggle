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

    async def subscribe_trades(self, symbol: str, callback: Callable):
        self._logger.debug(f"Subscribing to trades for {symbol}")
        subscription = {f"type": "trades", "coin": symbol}

        # TEMPORARY
        def forward_data(message):
            data = self._format_trade_data(message)
            callback(data)
        try:
            await self.info.subscribe(subscription=subscription, callback=forward_data)
            self._logger.info(f"Subscribed to trades for {symbol}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to trades for {symbol}: {e}")
    
    @staticmethod
    def _format_trade_data(message) -> List[Dict]:
        formatted_trades = []
        
        for trade in message['data']:
            contract_info = {
                "symbol": trade.get('coin', ''),
                "exchange": "HYPERLIQUID",
                "currency": "USD",
                "secType": "CRYPTO"
            }

            formatted_trade = {
                "contract": contract_info,
                "timestamp": trade.get('time', 0),
                "price": float(trade.get('px', 0)),
                "size": float(trade.get('sz', 0)),
                "side": trade.get('side', ''),  # 'B' for buy, 'A' for ask/sell
                "hash": trade.get('hash', ''),
                "tid": trade.get('tid', 0),
                "users": trade.get('users', [])
            }
            
            formatted_trades.append(formatted_trade)
        
        return formatted_trades

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
    