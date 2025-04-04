from .api import API
from .types import (
    Any,
    Callable,
    Cloid,
    Meta,
    Optional,
    SpotMeta,
    SpotMetaAndAssetCtxs,
    Subscription,
    cast,
)
from .websocket_manager import WebsocketManager


class Info(API):
    def __init__(
        self,
        base_url: Optional[str] = None,
        skip_ws: Optional[bool] = False,
        meta: Optional[Meta] = None,
        spot_meta: Optional[SpotMeta] = None,
    ):
        super().__init__(base_url)
        self.ws_manager: Optional[WebsocketManager] = None
        if not skip_ws:
            self.ws_manager = WebsocketManager(self.base_url)
        
        self.coin_to_asset = {}
        self.name_to_coin = {}
        self.asset_to_sz_decimals = {}
        self.coin_to_max_leverage = {}
        self._initialized = False
    
    async def initialize(self, meta: Optional[Meta] = None, spot_meta: Optional[SpotMeta] = None):
        """Initialize the Info instance with metadata"""
        if self._initialized:
            return
            
        if self.ws_manager:
            await self.ws_manager.start()
            
        if meta is None:
            meta = await self.meta()

        if spot_meta is None:
            spot_meta = await self.spot_meta()

        # Process perp metadata
        for asset, asset_info in enumerate(meta["universe"]):
            self.coin_to_asset[asset_info["name"]] = asset
            self.name_to_coin[asset_info["name"]] = asset_info["name"]
            self.asset_to_sz_decimals[asset] = asset_info["szDecimals"]
            if "maxLeverage" in asset_info:
                self.coin_to_max_leverage[asset_info["name"]] = asset_info["maxLeverage"]

        # Process spot metadata (assets start at 10000)
        for spot_info in spot_meta["universe"]:
            asset = spot_info["index"] + 10000
            self.coin_to_asset[spot_info["name"]] = asset
            self.name_to_coin[spot_info["name"]] = spot_info["name"]
            base, quote = spot_info["tokens"]
            base_info = spot_meta["tokens"][base]
            quote_info = spot_meta["tokens"][quote]
            self.asset_to_sz_decimals[asset] = base_info["szDecimals"]
            name = f'{base_info["name"]}/{quote_info["name"]}'
            if name not in self.name_to_coin:
                self.name_to_coin[name] = spot_info["name"]
                
        self._initialized = True

    async def disconnect_websocket(self):
        if self.ws_manager is None:
            raise RuntimeError("Cannot call disconnect_websocket since skip_ws was used")
        else:
            await self.ws_manager.stop()

    async def user_state(self, address: str) -> Any:
        """Retrieve trading details about a user.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                assetPositions: [
                    {
                        position: {
                            coin: str,
                            entryPx: Optional[float string]
                            leverage: {
                                type: "cross" | "isolated",
                                value: int,
                                rawUsd: float string  # only if type is "isolated"
                            },
                            liquidationPx: Optional[float string]
                            marginUsed: float string,
                            positionValue: float string,
                            returnOnEquity: float string,
                            szi: float string,
                            unrealizedPnl: float string
                        },
                        type: "oneWay"
                    }
                ],
                crossMarginSummary: MarginSummary,
                marginSummary: MarginSummary,
                withdrawable: float string,
            }

            where MarginSummary is {
                    accountValue: float string,
                    totalMarginUsed: float string,
                    totalNtlPos: float string,
                    totalRawUsd: float string,
                }
        """
        return await self.post("/info", {"type": "clearinghouseState", "user": address})

    async def spot_user_state(self, address: str) -> Any:
        return await self.post("/info", {"type": "spotClearinghouseState", "user": address})

    async def open_orders(self, address: str) -> Any:
        """Retrieve a user's open orders.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns: [
            {
                coin: str,
                limitPx: float string,
                oid: int,
                side: "A" | "B",
                sz: float string,
                timestamp: int
            }
        ]
        """
        return await self.post("/info", {"type": "openOrders", "user": address})

    async def frontend_open_orders(self, address: str) -> Any:
        """Retrieve a user's open orders with additional frontend info.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns: [
            {
                children:
                    [
                        dict of frontend orders
                    ]
                coin: str,
                isPositionTpsl: bool,
                isTrigger: bool,
                limitPx: float string,
                oid: int,
                orderType: str,
                origSz: float string,
                reduceOnly: bool,
                side: "A" | "B",
                sz: float string,
                tif: str,
                timestamp: int,
                triggerCondition: str,
                triggerPx: float str
            }
        ]
        """
        return await self.post("/info", {"type": "frontendOpenOrders", "user": address})

    async def all_mids(self) -> Any:
        """Retrieve all mids for all actively traded coins.

        POST /info

        Returns:
            {
              ATOM: float string,
              BTC: float string,
              any other coins which are trading: float string
            }
        """
        return await self.post("/info", {"type": "allMids"})

    async def user_fills(self, address: str) -> Any:
        """Retrieve a given user's fills.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.

        Returns:
            [
              {
                closedPnl: float string,
                coin: str,
                crossed: bool,
                dir: str,
                hash: str,
                oid: int,
                px: float string,
                side: str,
                startPosition: float string,
                sz: float string,
                time: int
              },
              ...
            ]
        """
        return await self.post("/info", {"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        """Retrieve a given user's fills by time.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
            start_time (int): Unix timestamp in milliseconds
            end_time (Optional[int]): Unix timestamp in milliseconds

        Returns:
            [
              {
                closedPnl: float string,
                coin: str,
                crossed: bool,
                dir: str,
                hash: str,
                oid: int,
                px: float string,
                side: str,
                startPosition: float string,
                sz: float string,
                time: int
              },
              ...
            ]
        """
        return await self.post(
            "/info", {"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time}
        )

    async def meta(self) -> Meta:
        """Retrieve exchange perp metadata

        POST /info

        Returns:
            {
                universe: [
                    {
                        name: str,
                        szDecimals: int
                    },
                    ...
                ]
            }
        """
        return cast(Meta, await self.post("/info", {"type": "meta"}))

    async def meta_and_asset_ctxs(self) -> Any:
        """Retrieve exchange MetaAndAssetCtxs

        POST /info

        Returns:
            [
                {
                    universe: [
                        {
                            'name': str,
                            'szDecimals': int
                            'maxLeverage': int,
                            'onlyIsolated': bool,
                        },
                        ...
                    ]
                },
            [
                {
                    "dayNtlVlm": float string,
                    "funding": float string,
                    "impactPxs": Optional([float string, float string]),
                    "markPx": Optional(float string),
                    "midPx": Optional(float string),
                    "openInterest": float string,
                    "oraclePx": float string,
                    "premium": Optional(float string),
                    "prevDayPx": float string
                },
                ...
            ]
        """
        return await self.post("/info", {"type": "metaAndAssetCtxs"})

    async def spot_meta(self) -> SpotMeta:
        """Retrieve exchange spot metadata

        POST /info

        Returns:
            {
                universe: [
                    {
                        tokens: [int, int],
                        name: str,
                        index: int,
                        isCanonical: bool
                    },
                    ...
                ],
                tokens: [
                    {
                        name: str,
                        szDecimals: int,
                        weiDecimals: int,
                        index: int,
                        tokenId: str,
                        isCanonical: bool
                    },
                    ...
                ]
            }
        """
        return cast(SpotMeta, await self.post("/info", {"type": "spotMeta"}))

    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        """Retrieve exchange spot asset contexts
        POST /info
        Returns:
            [
                {
                    universe: [
                        {
                            tokens: [int, int],
                            name: str,
                            index: int,
                            isCanonical: bool
                        },
                        ...
                    ],
                    tokens: [
                        {
                            name: str,
                            szDecimals: int,
                            weiDecimals: int,
                            index: int,
                            tokenId: str,
                            isCanonical: bool
                        },
                        ...
                    ]
                },
                [
                    {
                        dayNtlVlm: float string,
                        markPx: float string,
                        midPx: Optional(float string),
                        prevDayPx: float string,
                        circulatingSupply: float string,
                        coin: str
                    }
                    ...
                ]
            ]
        """
        return cast(SpotMetaAndAssetCtxs, await self.post("/info", {"type": "spotMetaAndAssetCtxs"}))

    async def funding_history(self, name: str, startTime: int, endTime: Optional[int] = None) -> Any:
        """Retrieve funding history for a given coin

        POST /info

        Args:
            name (str): Coin to retrieve funding history for.
            startTime (int): Unix timestamp in milliseconds.
            endTime (int): Unix timestamp in milliseconds.

        Returns:
            [
                {
                    coin: str,
                    fundingRate: float string,
                    premium: float string,
                    time: int
                },
                ...
            ]
        """
        coin = self.name_to_coin[name]
        if endTime is not None:
            return await self.post(
                "/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime, "endTime": endTime}
            )
        return await self.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime})

    async def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        """Retrieve a user's funding history
        POST /info
        Args:
            user (str): Address of the user in 42-character hexadecimal format.
            startTime (int): Start time in milliseconds, inclusive.
            endTime (int, optional): End time in milliseconds, inclusive. Defaults to current time.
        Returns:
            List[Dict]: A list of funding history records, where each record contains:
                - user (str): User address.
                - type (str): Type of the record, e.g., "userFunding".
                - startTime (int): Unix timestamp of the start time in milliseconds.
                - endTime (int): Unix timestamp of the end time in milliseconds.
        """
        if endTime is not None:
            return await self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return await self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime})

    async def l2_snapshot(self, name: str) -> Any:
        """Retrieve L2 snapshot for a given coin

        POST /info

        Args:
            name (str): Coin to retrieve L2 snapshot for.

        Returns:
            {
                coin: str,
                levels: [
                    [
                        {
                            n: int,
                            px: float string,
                            sz: float string
                        },
                        ...
                    ],
                    ...
                ],
                time: int
            }
        """
        return await self.post("/info", {"type": "l2Book", "coin": self.name_to_coin[name]})

    async def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> Any:
        """Retrieve candles snapshot for a given coin

        POST /info

        Args:
            name (str): Coin to retrieve candles snapshot for.
            interval (str): Candlestick interval.
            startTime (int): Unix timestamp in milliseconds.
            endTime (int): Unix timestamp in milliseconds.

        Returns:
            [
                {
                    T: int,
                    c: float string,
                    h: float string,
                    i: str,
                    l: float string,
                    n: int,
                    o: float string,
                    s: string,
                    t: int,
                    v: float string
                },
                ...
            ]
        """
        req = {"coin": self.name_to_coin[name], "interval": interval, "startTime": startTime, "endTime": endTime}
        return await self.post("/info", {"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        """Retrieve the volume of trading activity associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                activeReferralDiscount: float string,
                dailyUserVlm: [
                    {
                        date: str,
                        exchange: str,
                        userAdd: float string,
                        userCross: float string
                    },
                ],
                feeSchedule: {
                    add: float string,
                    cross: float string,
                    referralDiscount: float string,
                    tiers: {
                        mm: [
                            {
                                add: float string,
                                makerFractionCutoff: float string
                            },
                        ],
                        vip: [
                            {
                                add: float string,
                                cross: float string,
                                ntlCutoff: float string
                            },
                        ]
                    }
                },
                userAddRate: float string,
                userCrossRate: float string
            }
        """
        return await self.post("/info", {"type": "userFees", "user": address})

    async def user_staking_summary(self, address: str) -> Any:
        """Retrieve the staking summary associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                delegated: float string,
                undelegated: float string,
                totalPendingWithdrawal: float string,
                nPendingWithdrawals: int
            }
        """
        return await self.post("/info", {"type": "delegatorSummary", "user": address})

    async def user_staking_delegations(self, address: str) -> Any:
        """Retrieve the user's staking delegations.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            [
                {
                    validator: string,
                    amount: float string,
                    lockedUntilTimestamp: int
                },
            ]
        """
        return await self.post("/info", {"type": "delegations", "user": address})

    async def user_staking_rewards(self, address: str) -> Any:
        """Retrieve the historic staking rewards associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            [
                {
                    time: int,
                    source: string,
                    totalAmount: float string
                },
            ]
        """
        return await self.post("/info", {"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        return await self.post("/info", {"type": "orderStatus", "user": user, "oid": oid})

    async def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        return await self.post("/info", {"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    async def query_referral_state(self, user: str) -> Any:
        return await self.post("/info", {"type": "referral", "user": user})

    async def query_sub_accounts(self, user: str) -> Any:
        return await self.post("/info", {"type": "subAccounts", "user": user})

    async def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        return await self.post("/info", {"type": "userToMultiSigSigners", "user": multi_sig_user})

    async def subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int:
        if subscription["type"] == "l2Book" or subscription["type"] == "trades" or subscription["type"] == "candle":
            subscription["coin"] = self.name_to_coin[subscription["coin"]]
        if self.ws_manager is None:
            raise RuntimeError("Cannot call subscribe since skip_ws was used")
        else:
            return await self.ws_manager.subscribe(subscription, callback)

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        if subscription["type"] == "l2Book" or subscription["type"] == "trades" or subscription["type"] == "candle":
            subscription["coin"] = self.name_to_coin[subscription["coin"]]
        if self.ws_manager is None:
            raise RuntimeError("Cannot call unsubscribe since skip_ws was used")
        else:
            return await self.ws_manager.unsubscribe(subscription, subscription_id)

    def name_to_asset(self, name: str) -> int:
        """Convert a name to an asset ID"""
        return self.coin_to_asset[self.name_to_coin[name]]

    def get_max_leverage(self, name: str) -> Optional[int]:
        """Get the max leverage for a given coin name"""
        return self.coin_to_max_leverage.get(self.name_to_coin.get(name, name), None)