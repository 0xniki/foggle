import secrets
import logging

import eth_account
from eth_account.signers.local import LocalAccount

from .api import API
from .info import Info
from .constants import MAINNET_API_URL
from .signing import (
    CancelByCloidRequest,
    CancelRequest,
    ModifyRequest,
    OidOrCloid,
    OrderRequest,
    OrderType,
    OrderWire,
    ScheduleCancelAction,
    float_to_usd_int,
    get_timestamp_ms,
    order_request_to_order_wire,
    order_wires_to_order_action,
    sign_agent,
    sign_approve_builder_fee,
    sign_convert_to_multi_sig_user_action,
    sign_l1_action,
    sign_multi_sig_action,
    sign_spot_transfer_action,
    sign_usd_class_transfer_action,
    sign_usd_transfer_action,
    sign_withdraw_from_bridge_action,
)
from .types import Any, BuilderInfo, Cloid, List, Meta, Optional, SpotMeta, Tuple


class Exchange(API):
    # Default Max Slippage for Market Orders 5%
    DEFAULT_SLIPPAGE = 0.05

    def __init__(
        self,
        wallet: LocalAccount,
        base_url: Optional[str] = None,
        meta: Optional[Meta] = None,
        vault_address: Optional[str] = None,
        account_address: Optional[str] = None,
        spot_meta: Optional[SpotMeta] = None,
    ):
        super().__init__(base_url)
        self.wallet = wallet
        self.vault_address = vault_address
        self.account_address = account_address
        self.info = Info(base_url, True, meta, spot_meta)
        self._initialized = False
        
    async def initialize(self):
        """Initialize the Exchange instance with metadata"""
        if not self._initialized:
            await self.info.initialize()
            self._initialized = True

    async def _post_action(self, action, signature, nonce):
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.vault_address if action["type"] != "usdClassTransfer" else None,
        }
        logging.debug(payload)
        return await self.post("/exchange", payload)

    def _slippage_price(
        self,
        name: str,
        is_buy: bool,
        slippage: float,
        px: Optional[float] = None,
    ) -> float:
        """Calculate price with slippage (doesn't need to be async)"""
        coin = self.info.name_to_coin[name]
        if not px:
            # Since all_mids is async, we need to handle this case differently now
            # This will be addressed by the caller
            raise ValueError("Must provide px when using _slippage_price directly")

        asset = self.info.coin_to_asset[coin]
        # spot assets start at 10000
        is_spot = asset >= 10_000

        # Calculate Slippage
        px *= (1 + slippage) if is_buy else (1 - slippage)
        # We round px to 5 significant figures and 6 decimals for perps, 8 decimals for spot
        return round(float(f"{px:.5g}"), (6 if not is_spot else 8) - self.info.asset_to_sz_decimals[asset])

    async def order(
        self,
        name: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        order_type: OrderType,
        reduce_only: bool = False,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        await self.initialize()
        order: OrderRequest = {
            "coin": name,
            "is_buy": is_buy,
            "sz": sz,
            "limit_px": limit_px,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        if cloid:
            order["cloid"] = cloid
        return await self.bulk_orders([order], builder)

    async def bulk_orders(self, order_requests: List[OrderRequest], builder: Optional[BuilderInfo] = None) -> Any:
        await self.initialize()
        order_wires: List[OrderWire] = [
            order_request_to_order_wire(order, self.info.name_to_asset(order["coin"])) for order in order_requests
        ]
        timestamp = get_timestamp_ms()

        if builder:
            builder["b"] = builder["b"].lower()
        order_action = order_wires_to_order_action(order_wires, builder)

        signature = sign_l1_action(
            self.wallet,
            order_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )

        return await self._post_action(
            order_action,
            signature,
            timestamp,
        )

    async def modify_order(
        self,
        oid: OidOrCloid,
        name: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        order_type: OrderType,
        reduce_only: bool = False,
        cloid: Optional[Cloid] = None,
    ) -> Any:
        await self.initialize()
        modify: ModifyRequest = {
            "oid": oid,
            "order": {
                "coin": name,
                "is_buy": is_buy,
                "sz": sz,
                "limit_px": limit_px,
                "order_type": order_type,
                "reduce_only": reduce_only,
                "cloid": cloid,
            },
        }
        return await self.bulk_modify_orders_new([modify])

    async def bulk_modify_orders_new(self, modify_requests: List[ModifyRequest]) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        modify_wires = [
            {
                "oid": modify["oid"].to_raw() if isinstance(modify["oid"], Cloid) else modify["oid"],
                "order": order_request_to_order_wire(modify["order"], self.info.name_to_asset(modify["order"]["coin"])),
            }
            for modify in modify_requests
        ]

        modify_action = {
            "type": "batchModify",
            "modifies": modify_wires,
        }

        signature = sign_l1_action(
            self.wallet,
            modify_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )

        return await self._post_action(
            modify_action,
            signature,
            timestamp,
        )

    async def market_open(
        self,
        name: str,
        is_buy: bool,
        sz: float,
        px: Optional[float] = None,
        slippage: float = DEFAULT_SLIPPAGE,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        await self.initialize()
        # Get aggressive Market Price
        if px is None:
            mids = await self.info.all_mids()
            coin = self.info.name_to_coin[name]
            px = float(mids[coin])
            
        px = self._slippage_price(name, is_buy, slippage, px)
        # Market Order is an aggressive Limit Order IoC
        return await self.order(
            name, is_buy, sz, px, order_type={"limit": {"tif": "Ioc"}}, reduce_only=False, cloid=cloid, builder=builder
        )

    async def market_close(
        self,
        coin: str,
        sz: Optional[float] = None,
        px: Optional[float] = None,
        slippage: float = DEFAULT_SLIPPAGE,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        await self.initialize()
        address: str = self.wallet.address
        if self.account_address:
            address = self.account_address
        if self.vault_address:
            address = self.vault_address
            
        positions = await self.info.user_state(address)
        positions = positions["assetPositions"]
        
        for position in positions:
            item = position["position"]
            if coin != item["coin"]:
                continue
                
            szi = float(item["szi"])
            if not sz:
                sz = abs(szi)
                
            is_buy = True if szi < 0 else False
            
            # Get aggressive Market Price
            if px is None:
                mids = await self.info.all_mids()
                coin_key = self.info.name_to_coin[coin]
                px = float(mids[coin_key])
                
            px = self._slippage_price(coin, is_buy, slippage, px)
            
            # Market Order is an aggressive Limit Order IoC
            return await self.order(
                coin,
                is_buy,
                sz,
                px,
                order_type={"limit": {"tif": "Ioc"}},
                reduce_only=True,
                cloid=cloid,
                builder=builder,
            )

    async def cancel(self, name: str, oid: int) -> Any:
        await self.initialize()
        return await self.bulk_cancel([{"coin": name, "oid": oid}])

    async def cancel_by_cloid(self, name: str, cloid: Cloid) -> Any:
        await self.initialize()
        return await self.bulk_cancel_by_cloid([{"coin": name, "cloid": cloid}])

    async def bulk_cancel(self, cancel_requests: List[CancelRequest]) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        cancel_action = {
            "type": "cancel",
            "cancels": [
                {
                    "a": self.info.name_to_asset(cancel["coin"]),
                    "o": cancel["oid"],
                }
                for cancel in cancel_requests
            ],
        }
        signature = sign_l1_action(
            self.wallet,
            cancel_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )

        return await self._post_action(
            cancel_action,
            signature,
            timestamp,
        )

    async def bulk_cancel_by_cloid(self, cancel_requests: List[CancelByCloidRequest]) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()

        cancel_action = {
            "type": "cancelByCloid",
            "cancels": [
                {
                    "asset": self.info.name_to_asset(cancel["coin"]),
                    "cloid": cancel["cloid"].to_raw(),
                }
                for cancel in cancel_requests
            ],
        }
        signature = sign_l1_action(
            self.wallet,
            cancel_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )

        return await self._post_action(
            cancel_action,
            signature,
            timestamp,
        )

    async def schedule_cancel(self, time: Optional[int]) -> Any:
        """Schedules a time (in UTC millis) to cancel all open orders. The time must be at least 5 seconds after the current time.
        Once the time comes, all open orders will be canceled and a trigger count will be incremented. The max number of triggers
        per day is 10. This trigger count is reset at 00:00 UTC.

        Args:
            time (int): if time is not None, then set the cancel time in the future. If None, then unsets any cancel time in the future.
        """
        await self.initialize()
        timestamp = get_timestamp_ms()
        schedule_cancel_action: ScheduleCancelAction = {
            "type": "scheduleCancel",
        }
        if time is not None:
            schedule_cancel_action["time"] = time
        signature = sign_l1_action(
            self.wallet,
            schedule_cancel_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            schedule_cancel_action,
            signature,
            timestamp,
        )

    async def update_leverage(self, leverage: int, name: str, is_cross: bool = True) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        update_leverage_action = {
            "type": "updateLeverage",
            "asset": self.info.name_to_asset(name),
            "isCross": is_cross,
            "leverage": leverage,
        }
        signature = sign_l1_action(
            self.wallet,
            update_leverage_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            update_leverage_action,
            signature,
            timestamp,
        )

    async def update_isolated_margin(self, amount: float, name: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        amount = float_to_usd_int(amount)
        update_isolated_margin_action = {
            "type": "updateIsolatedMargin",
            "asset": self.info.name_to_asset(name),
            "isBuy": True,
            "ntli": amount,
        }
        signature = sign_l1_action(
            self.wallet,
            update_isolated_margin_action,
            self.vault_address,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            update_isolated_margin_action,
            signature,
            timestamp,
        )
        
    async def set_referrer(self, code: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        set_referrer_action = {
            "type": "setReferrer",
            "code": code,
        }
        signature = sign_l1_action(
            self.wallet,
            set_referrer_action,
            None,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            set_referrer_action,
            signature,
            timestamp,
        )

    async def create_sub_account(self, name: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        create_sub_account_action = {
            "type": "createSubAccount",
            "name": name,
        }
        signature = sign_l1_action(
            self.wallet,
            create_sub_account_action,
            None,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            create_sub_account_action,
            signature,
            timestamp,
        )

    async def usd_class_transfer(self, amount: float, to_perp: bool) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        str_amount = str(amount)
        if self.vault_address:
            str_amount += f" subaccount:{self.vault_address}"

        action = {
            "type": "usdClassTransfer",
            "amount": str_amount,
            "toPerp": to_perp,
            "nonce": timestamp,
        }
        signature = sign_usd_class_transfer_action(self.wallet, action, self.base_url == MAINNET_API_URL)
        return await self._post_action(
            action,
            signature,
            timestamp,
        )

    async def sub_account_transfer(self, sub_account_user: str, is_deposit: bool, usd: int) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        sub_account_transfer_action = {
            "type": "subAccountTransfer",
            "subAccountUser": sub_account_user,
            "isDeposit": is_deposit,
            "usd": usd,
        }
        signature = sign_l1_action(
            self.wallet,
            sub_account_transfer_action,
            None,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            sub_account_transfer_action,
            signature,
            timestamp,
        )
        
    async def sub_account_spot_transfer(self, sub_account_user: str, is_deposit: bool, token: str, amount: float) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        sub_account_transfer_action = {
            "type": "subAccountSpotTransfer",
            "subAccountUser": sub_account_user,
            "isDeposit": is_deposit,
            "token": token,
            "amount": str(amount),
        }
        signature = sign_l1_action(
            self.wallet,
            sub_account_transfer_action,
            None,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            sub_account_transfer_action,
            signature,
            timestamp,
        )

    async def vault_usd_transfer(self, vault_address: str, is_deposit: bool, usd: int) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        vault_transfer_action = {
            "type": "vaultTransfer",
            "vaultAddress": vault_address,
            "isDeposit": is_deposit,
            "usd": usd,
        }
        is_mainnet = self.base_url == MAINNET_API_URL
        signature = sign_l1_action(self.wallet, vault_transfer_action, None, timestamp, is_mainnet)
        return await self._post_action(
            vault_transfer_action,
            signature,
            timestamp,
        )

    async def usd_transfer(self, amount: float, destination: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        action = {"destination": destination, "amount": str(amount), "time": timestamp, "type": "usdSend"}
        is_mainnet = self.base_url == MAINNET_API_URL
        signature = sign_usd_transfer_action(self.wallet, action, is_mainnet)
        return await self._post_action(
            action,
            signature,
            timestamp,
        )

    async def spot_transfer(self, amount: float, destination: str, token: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        action = {
            "destination": destination,
            "amount": str(amount),
            "token": token,
            "time": timestamp,
            "type": "spotSend",
        }
        is_mainnet = self.base_url == MAINNET_API_URL
        signature = sign_spot_transfer_action(self.wallet, action, is_mainnet)
        return await self._post_action(
            action,
            signature,
            timestamp,
        )
        
    async def withdraw_from_bridge(self, amount: float, destination: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        action = {"destination": destination, "amount": str(amount), "time": timestamp, "type": "withdraw3"}
        is_mainnet = self.base_url == MAINNET_API_URL
        signature = sign_withdraw_from_bridge_action(self.wallet, action, is_mainnet)
        return await self._post_action(
            action,
            signature,
            timestamp,
        )

    async def approve_agent(self, name: Optional[str] = None) -> Tuple[Any, str]:
        await self.initialize()
        agent_key = "0x" + secrets.token_hex(32)
        account = eth_account.Account.from_key(agent_key)
        timestamp = get_timestamp_ms()
        is_mainnet = self.base_url == MAINNET_API_URL
        action = {
            "type": "approveAgent",
            "agentAddress": account.address,
            "agentName": name or "",
            "nonce": timestamp,
        }
        signature = sign_agent(self.wallet, action, is_mainnet)
        if name is None:
            del action["agentName"]

        return (
            await self._post_action(
                action,
                signature,
                timestamp,
            ),
            agent_key,
        )

    async def approve_builder_fee(self, builder: str, max_fee_rate: str) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()

        action = {"maxFeeRate": max_fee_rate, "builder": builder, "nonce": timestamp, "type": "approveBuilderFee"}
        signature = sign_approve_builder_fee(self.wallet, action, self.base_url == MAINNET_API_URL)
        return await self._post_action(action, signature, timestamp)
        
    async def convert_to_multi_sig_user(self, authorized_users: List[str], threshold: int) -> Any:
        await self.initialize()
        import json
        timestamp = get_timestamp_ms()
        authorized_users = sorted(authorized_users)
        signers = {
            "authorizedUsers": authorized_users,
            "threshold": threshold,
        }
        action = {
            "type": "convertToMultiSigUser",
            "signers": json.dumps(signers),
            "nonce": timestamp,
        }
        signature = sign_convert_to_multi_sig_user_action(self.wallet, action, self.base_url == MAINNET_API_URL)
        return await self._post_action(
            action,
            signature,
            timestamp,
        )

    async def multi_sig(self, multi_sig_user, inner_action, signatures, nonce, vault_address=None):
        await self.initialize()
        multi_sig_user = multi_sig_user.lower()
        multi_sig_action = {
            "type": "multiSig",
            "signatureChainId": "0x66eee",
            "signatures": signatures,
            "payload": {
                "multiSigUser": multi_sig_user,
                "outerSigner": self.wallet.address.lower(),
                "action": inner_action,
            },
        }
        is_mainnet = self.base_url == MAINNET_API_URL
        signature = sign_multi_sig_action(
            self.wallet,
            multi_sig_action,
            is_mainnet,
            vault_address,
            nonce,
        )
        return await self._post_action(
            multi_sig_action,
            signature,
            nonce,
        )

    async def use_big_blocks(self, enable: bool) -> Any:
        await self.initialize()
        timestamp = get_timestamp_ms()
        action = {
            "type": "evmUserModify",
            "usingBigBlocks": enable,
        }
        signature = sign_l1_action(
            self.wallet,
            action,
            None,
            timestamp,
            self.base_url == MAINNET_API_URL,
        )
        return await self._post_action(
            action,
            signature,
            timestamp,
        )

    async def shutdown(self) -> None:
        if self._initialized:
            # await self.info.ws_manager.stop()
            await self.info.close()
            await self.close()