import asyncio
from typing import Callable, List, Optional, Any

class Connection(asyncio.Protocol):
    
    def __init__(self):
        self._data_callbacks: List[Callable[[bytes], Any]] = []
        self._disconnected_callbacks: List[Callable[[str], Any]] = []

        self.data_queue: asyncio.Queue = asyncio.Queue()

        self.disconnect_future: Optional[asyncio.Future] = None
        
        self.reset()
    
    def reset(self):
        self.transport = None
        self.numBytesSent = 0
        self.numMsgSent = 0
        self.disconnect_future = asyncio.get_event_loop().create_future()
    
    async def connectAsync(self, host: str, port: int) -> None:
        if self.transport:
            self.disconnect()
            await self.disconnect_future
        
        self.reset()
        loop = asyncio.get_event_loop()
        connect_future = loop.create_connection(lambda: self, host, port)
        self.transport, _ = await connect_future
    
    def disconnect(self) -> None:
        if self.transport:
            self.transport.write_eof()
            self.transport.close()
    
    def isConnected(self) -> bool:
        return self.transport is not None
    
    def sendMsg(self, msg: bytes) -> None:
        if self.transport:
            self.transport.write(msg)
            self.numBytesSent += len(msg)
            self.numMsgSent += 1
    
    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.transport = None
        msg = str(exc) if exc else ''
        
        for callback in self._disconnected_callbacks:
            callback(msg)

        if not self.disconnect_future.done():
            self.disconnect_future.set_result(msg)
    
    def data_received(self, data: bytes) -> None:
        self.data_queue.put_nowait(data)
        
        for callback in self._data_callbacks:
            callback(data)

    def add_data_callback(self, callback: Callable[[bytes], Any]) -> None:
        self._data_callbacks.append(callback)
    
    def remove_data_callback(self, callback: Callable[[bytes], Any]) -> None:
        self._data_callbacks.remove(callback)
    
    def add_disconnected_callback(self, callback: Callable[[str], Any]) -> None:
        self._disconnected_callbacks.append(callback)
    
    def remove_disconnected_callback(self, callback: Callable[[str], Any]) -> None:
        self._disconnected_callbacks.remove(callback)
    
    async def get_data(self) -> bytes:
        return await self.data_queue.get()