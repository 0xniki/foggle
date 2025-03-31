import os
import asyncio
import yaml
import signal
import logging

from dotenv import load_dotenv
from typing import Dict

from src.api_manager import APIManager
from src.store.timescaledb.db import Database
from src.newswatch.trading_econ import TradingEconomics
from src.exchanges.ibkr import util
from src.stream import Stream
from src.feed import Feed


class Foggle:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_sigterm)
            
        self.api_manager = APIManager()

        logging.info(f"Process ID: {os.getpid()}")

    async def run(self) -> None:
        config = load_config()

        feed = Feed()

        db = Database(config=config['TimescaleDB'])
        await db.init_pool()

        await self.api_manager.start(config)

        streams = Stream(feed, db)
        streams.add_exchanges(self.api_manager.exchanges)

        topics = {
            "commodity": ["crude-oil", "gold"],
            "united-states": ["stock-market", "government-bond-yield", "inflation-cpi"]
        }
        te = TradingEconomics(topics=topics, callback=db.insert_news_item)

        await self._wait_for_confirmation()

        await test(streams, te, db)

        try:
            task = asyncio.create_task(self.run_forever())
            await self._shutdown_event.wait()

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logging.info("Shutting down...")
                
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        await self.api_manager.shutdown()

    def _handle_sigterm(self, signum, frame):
        asyncio.get_event_loop().call_soon_threadsafe(self._shutdown_event.set)

    async def _wait_for_confirmation(self) -> None:
        while True:
            await asyncio.sleep(1)

            # if not self.feed.ready:
            #     continue

            break

    async def run_forever(self):
        while True:
            print("Trading loop iteration...")
            await asyncio.sleep(60)

def load_config(path: str = 'config.yml') -> Dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)
        for key in config:
            if isinstance(config[key], dict) and 'key' in config[key] and config[key]['key'] is not None:
                key_name = config[key]['key']
                config[key]['key'] = load_keys(key=key_name)
        return config
    
def load_keys(path: str = '.env', key: str = None) -> Dict:
    env_path = os.path.expanduser(path)
    load_dotenv(dotenv_path=env_path)
    return os.getenv(key)


async def test(stream: Stream, te: TradingEconomics, db: Database):
    te.start_scrape(interval=3600)

    news = await db.get_news_by_category("United States", "Stock Market", limit=2)

    print(news)

    aapl_stock = {
        "symbol": "AAPL",
        "secType": "STK",
        "exchange": "IEX",
        "currency": "USD"
        }
    
    nq_fut = {
        "symbol": "NQ",
        "secType": "FUT",
        "expiration": "20250620",
        "exchange": "CME",
        "currency": "USD"
        }

    nvda_opt = {
        "symbol": "NVDA",
        "secType": "OPT",
        "expiration": "20250404",
        "strike": "112",
        "right": "P",
        "exchange": "AMEX",
        "currency": "USD"
        }

    eth_perp = {
        "symbol": "ETH",
        "secType": "PERP",
        "exchange": "HYPERLIQUID",
        "currency": "USD"
        }
    
    eth_spot = {
        "symbol": "ETH",
        "secType": "CRYPTO",
        "exchange": "PAXOS",
        "currency": "USD"
        }

    # bars = await ibkr.reqHistoricalDataAsync(
    #     nvda_opt, endDateTime='', durationStr='30 D',
    #     barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True)
    # df = util.df(bars)
    # print(df)

    await stream.subscribe_trades(exchange="IBKR", contract=nq_fut)
    await stream.subscribe_orderbook(exchange="IBKR", contract=nq_fut)
    await stream.subscribe_trades(exchange="IBKR", contract=aapl_stock)
    await stream.subscribe_orderbook(exchange="IBKR", contract=aapl_stock)
    await stream.subscribe_trades(exchange="IBKR", contract=eth_spot)
    await stream.subscribe_orderbook(exchange="IBKR", contract=eth_spot)
    # await stream.subscribe_orderbook(exchange="IBKR", contract=nvda_opt)

    await stream.subscribe_trades(exchange="HyperLiquid", contract=eth_perp)
    await stream.subscribe_orderbook(exchange="HyperLiquid", contract=eth_perp)

    # res = await hyperliquid.info.open_orders("0x6d7823cd5c3d9dcd63e6a8021b475e0c7c94b291")
    # print(res)
    
    # await asyncio.sleep(2)
