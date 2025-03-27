import asyncio

from typing import Dict, List

from .feed import Feed
from .store.timescaledb.db import Database
from .exchanges.base import Exchange

class Stream:
    def __init__(self, feed: Feed, db: Database):
        self.feed = feed
        self.db = db

        self.exchanges = {}

    def add_exchange(self, exchanges: List[Dict][str, Exchange]):
        pass
    
