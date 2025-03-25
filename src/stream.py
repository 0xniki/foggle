import asyncio

from .feed import Feed

class Stream:
    def __init__(self, feed: Feed):
        self.feed = feed
