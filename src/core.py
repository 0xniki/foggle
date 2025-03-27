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
from src.exchanges.ibkr import contract, util


class Foggle:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_sigterm)
            
        self.api_manager = APIManager()

        logging.info(f"Process ID: {os.getpid()}")

    async def run(self) -> None:
        config = load_config()

        db = Database(config=config['TimescaleDB'])
        await db.init_pool()

        await self.api_manager.start(config=config)

        topics = {
            "commodity": ["crude-oil", "gold"],
            "united-states": ["stock-market", "government-bond-yield", "inflation-cpi"]
        }
        te = TradingEconomics(topics=topics, callback=db.insert_news_item)
        ibkr = self.api_manager.exchanges["IBKR"]
        hyperliquid = self.api_manager.exchanges["HyperLiquid"]

        await test(ibkr, hyperliquid, te, db)

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


async def test(ibkr, hyperliquid, te, db):
    te.start_scrape(interval=3600)

    news = await db.get_news_by_category("United States", "Stock Market", limit=2)

    print(news)

    spy = contract.Future(symbol='MES', lastTradeDateOrContractMonth='202506', exchange='CME', currency='USD')
    nq = contract.Future(symbol='NQ', lastTradeDateOrContractMonth='202506', exchange='CME', currency='USD')

    bars = await ibkr.reqHistoricalDataAsync(
        spy, endDateTime='', durationStr='30 D',
        barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True)
    df = util.df(bars)
    print(df)
    
    def print_tick(data):
        print(f"New tick: {data}")

    ibkr.reqTickByTickData(contract=nq, tickType='AllLast', callback=db.insert_trades)
    await asyncio.sleep(2)

    # ibkr.reqTickByTickData(contract=spy, tickType='AllLast', callback=db.insert_trades)
    # await asyncio.sleep(2)

    # res = await hyperliquid.info.open_orders("0x6d7823cd5c3d9dcd63e6a8021b475e0c7c94b291")
    # print(res)
    
    # await asyncio.sleep(2)

    await hyperliquid.subscribe_trades(symbol="SOL", callback=db.insert_trades)
