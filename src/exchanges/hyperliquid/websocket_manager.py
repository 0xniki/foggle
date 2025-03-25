import orjson
import logging
import asyncio
from collections import defaultdict

import websockets

from .types import Any, Callable, Dict, List, Tuple, NamedTuple, Optional, Subscription, WsMsg

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])

def subscription_to_identifier(subscription: Subscription) -> str:
    mapping = {
        "allMids": lambda s: "allMids",
        "l2Book": lambda s: f'l2Book:{s["coin"].lower()}',
        "trades": lambda s: f'trades:{s["coin"].lower()}',
        "userEvents": lambda s: "userEvents",
        "userFills": lambda s: f'userFills:{s["user"].lower()}',
        "candle": lambda s: f'candle:{s["coin"].lower()},{s["interval"]}',
        "orderUpdates": lambda s: "orderUpdates",
        "userFundings": lambda s: f'userFundings:{s["user"].lower()}',
        "userNonFundingLedgerUpdates": lambda s: f'userNonFundingLedgerUpdates:{s["user"].lower()}',
        "webData2": lambda s: f'webData2:{s["user"].lower()}'
    }
    
    return mapping[subscription["type"]](subscription)

def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    channel = ws_msg["channel"]
    
    if channel == "pong":
        return "pong"
    
    mapping = {
        "allMids": lambda msg: "allMids",
        "l2Book": lambda msg: f'l2Book:{msg["data"]["coin"].lower()}',
        "trades": lambda msg: f'trades:{msg["data"][0]["coin"].lower()}' if len(msg["data"]) > 0 else None,
        "user": lambda msg: "userEvents",
        "userFills": lambda msg: f'userFills:{msg["data"]["user"].lower()}',
        "candle": lambda msg: f'candle:{msg["data"]["s"].lower()},{msg["data"]["i"]}',
        "orderUpdates": lambda msg: "orderUpdates",
        "userFundings": lambda msg: f'userFundings:{msg["data"]["user"].lower()}',
        "userNonFundingLedgerUpdates": lambda msg: f'userNonFundingLedgerUpdates:{msg["data"]["user"].lower()}',
        "webData2": lambda msg: f'webData2:{msg["data"]["user"].lower()}'
    }
    
    handler = mapping.get(channel)
    if handler:
        return handler(ws_msg)
    return None

class WebsocketManager:
    def __init__(self, base_url):
        self._logger = logging.getLogger("hl_async.ws_manager")
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)
        self.ws_url = "ws" + base_url[len("http"):] + "/ws"
        self.websocket = None
        self.stop_event = asyncio.Event()
        self.ping_task = None
        self.ws_task = None
        
    async def start(self):
        """Start the websocket connection and ping loop"""
        self.ws_task = asyncio.create_task(self.run_forever())
        self.ping_task = asyncio.create_task(self.send_ping())
        
    async def run_forever(self):
        """Maintain the websocket connection"""
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    self.websocket = websocket
                    self.ws_ready = True
                    
                    # Process queued subscriptions
                    for subscription, active_subscription in self.queued_subscriptions:
                        await self.subscribe(
                            subscription, 
                            active_subscription.callback, 
                            active_subscription.subscription_id
                        )
                    self.queued_subscriptions = []
                    
                    # Process incoming messages
                    while not self.stop_event.is_set():
                        try:
                            message = await websocket.recv()
                            await self.on_message(message)
                        except websockets.exceptions.ConnectionClosed:
                            break
                            
            except Exception as e:
                logging.error(f"WebSocket error: {e}")
                self.ws_ready = False
                
            if not self.stop_event.is_set():
                # Wait before reconnecting
                await asyncio.sleep(5)
    
    async def stop(self):
        """Stop the websocket connection and ping loop"""
        self.stop_event.set()
        if self.websocket:
            await self.websocket.close()
        
        if self.ping_task:
            await self.ping_task
        if self.ws_task:
            await self.ws_task
            
    async def send_ping(self):
        """Send periodic pings to keep the connection alive"""
        while not self.stop_event.is_set():
            try:
                if self.ws_ready and self.websocket:
                    logging.debug("Websocket sending ping")
                    await self.websocket.send(orjson.dumps({"method": "ping"}).decode('utf-8'))
            except Exception as e:
                logging.error(f"Error sending ping: {e}")
                
            await asyncio.sleep(50)
        
        logging.debug("Websocket ping sender stopped")
            
    async def on_message(self, message):
        """Handle incoming websocket messages"""
        if message == "Websocket connection established.":
            logging.debug(message)
            return
            
        logging.debug(f"on_message {message}")
        ws_msg = orjson.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
            
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
            
        active_subscriptions = self.active_subscriptions[identifier]
        if not active_subscriptions:
            print("Websocket message from an unexpected subscription:", message, identifier)
        else:
            for active_subscription in active_subscriptions:
                active_subscription.callback(ws_msg)
    
    async def subscribe(
        self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None
    ) -> int:
        """Subscribe to a websocket channel"""
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
            
        if not self.ws_ready:
            logging.debug("enqueueing subscription")
            self.queued_subscriptions.append((subscription, ActiveSubscription(callback, subscription_id)))
        else:
            logging.debug("subscribing")
            identifier = subscription_to_identifier(subscription)
            
            # Special handling for exclusive subscriptions
            if identifier in ("userEvents", "orderUpdates") and self.active_subscriptions[identifier]:
                raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
                
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
            await self.websocket.send(orjson.dumps({"method": "subscribe", "subscription": subscription}).decode('utf-8'))
            
        return subscription_id
        
    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        """Unsubscribe from a websocket channel"""
        if not self.ws_ready:
            raise NotImplementedError("Can't unsubscribe before websocket connected")
            
        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
        
        if not new_active_subscriptions:
            await self.websocket.send(orjson.dumps({"method": "unsubscribe", "subscription": subscription}).decode('utf-8'))
            
        self.active_subscriptions[identifier] = new_active_subscriptions
        return len(active_subscriptions) != len(new_active_subscriptions)