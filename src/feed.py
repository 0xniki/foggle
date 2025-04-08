import logging
from typing import Dict, Optional, List, Any, Tuple, Union
import numpy as np

from src.types import ContractDict
from src.utils.ring_buffer import RingBufferMultiDim

class Feed:
    def __init__(self, capacity: int = 1000):
        """
        Initialize a Feed object to store market data in ring buffers.
        
        Parameters:
        -----------
        capacity : int, optional
            The default capacity for all ring buffers. Default is 1000.
        """
        self._capacity = capacity
        self._trades: Dict[int, RingBufferMultiDim] = {}
        self._orderbook: Dict[int, RingBufferMultiDim] = {}
        self._candles: Dict[int, RingBufferMultiDim] = {}
        
        # self._contracts_cache: Dict[int, ContractDict] = {}
        self._logger = logging.getLogger("feed")

    @property
    def trades(self) -> Dict[int, RingBufferMultiDim]:
        """Get all trade buffers keyed by contract ID."""
        return self._trades
    
    @property
    def orderbook(self) -> Dict[int, RingBufferMultiDim]:
        """Get all orderbook buffers keyed by contract ID."""
        return self._orderbook
    
    @property
    def candles(self) -> Dict[int, RingBufferMultiDim]:
        """Get all candle buffers keyed by contract ID."""
        return self._candles
    
    def trades_by_id(self, con_id: int) -> RingBufferMultiDim:
        """
        Get the trade buffer for a specific contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID to look up.
            
        Returns:
        --------
        RingBufferMultiDim
            The ring buffer containing trades for the specified contract.
            
        Raises:
        -------
        KeyError
            If the contract ID is not found.
        """
        if con_id not in self._trades:
            raise KeyError(f"No trade data for contract ID {con_id}")
        return self._trades[con_id]
    
    def orderbook_by_id(self, con_id: int) -> RingBufferMultiDim:
        """
        Get the orderbook buffer for a specific contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID to look up.
            
        Returns:
        --------
        RingBufferMultiDim
            The ring buffer containing orderbook data for the specified contract.
            
        Raises:
        -------
        KeyError
            If the contract ID is not found.
        """
        if con_id not in self._orderbook:
            raise KeyError(f"No orderbook data for contract ID {con_id}")
        return self._orderbook[con_id]
    
    def candles_by_id(self, con_id: int) -> RingBufferMultiDim:
        """
        Get the candle buffer for a specific contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID to look up.
            
        Returns:
        --------
        RingBufferMultiDim
            The ring buffer containing candle data for the specified contract.
            
        Raises:
        -------
        KeyError
            If the contract ID is not found.
        """
        if con_id not in self._candles:
            raise KeyError(f"No candle data for contract ID {con_id}")
        return self._candles[con_id]

    def add_trade(self, con_id: int, trade_data: np.ndarray) -> None:
        """
        Add a new trade to the buffer for the specified contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID for this trade.
        trade_data : np.ndarray
            The trade data to add.
        """
        if con_id not in self._trades:
            # Create a new buffer for this contract with appropriate shape
            self._trades[con_id] = RingBufferMultiDim(
                shape=(self._capacity, trade_data.shape[0]), 
                dtype=trade_data.dtype
            )
            self._logger.info(f"Created new trade buffer for contract ID {con_id}")
        
        self._trades[con_id].append(trade_data)
    
    def add_orderbook_update(self, con_id: int, orderbook_data: np.ndarray) -> None:
        """
        Add a new orderbook update to the buffer for the specified contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID for this orderbook update.
        orderbook_data : np.ndarray
            The orderbook data to add.
        """
        if con_id not in self._orderbook:
            # Create a new buffer for this contract with appropriate shape
            self._orderbook[con_id] = RingBufferMultiDim(
                shape=(self._capacity, orderbook_data.shape[0]), 
                dtype=orderbook_data.dtype
            )
            self._logger.info(f"Created new orderbook buffer for contract ID {con_id}")
        
        self._orderbook[con_id].append(orderbook_data)
    
    def add_candle(self, con_id: int, candle_data: np.ndarray) -> None:
        """
        Add a new candle to the buffer for the specified contract ID.
        
        Parameters:
        -----------
        con_id : int
            The contract ID for this candle.
        candle_data : np.ndarray
            The candle data to add.
        """
        if con_id not in self._candles:
            # Create a new buffer for this contract with appropriate shape
            self._candles[con_id] = RingBufferMultiDim(
                shape=(self._capacity, candle_data.shape[0]), 
                dtype=candle_data.dtype
            )
            self._logger.info(f"Created new candle buffer for contract ID {con_id}")
        
        self._candles[con_id].append(candle_data)
    
    def has_data(self, con_id: int, data_type: str = "any") -> bool:
        """
        Check if data exists for the specified contract ID and data type.
        
        Parameters:
        -----------
        con_id : int
            The contract ID to check.
        data_type : str, optional
            The type of data to check for - 'trades', 'orderbook', 'candles', or 'any'.
            Default is 'any'.
            
        Returns:
        --------
        bool
            True if data exists, False otherwise.
        """
        if data_type == "trades":
            return con_id in self._trades and len(self._trades[con_id]) > 0
        elif data_type == "orderbook":
            return con_id in self._orderbook and len(self._orderbook[con_id]) > 0
        elif data_type == "candles":
            return con_id in self._candles and len(self._candles[con_id]) > 0
        elif data_type == "any":
            return (con_id in self._trades and len(self._trades[con_id]) > 0) or \
                   (con_id in self._orderbook and len(self._orderbook[con_id]) > 0) or \
                   (con_id in self._candles and len(self._candles[con_id]) > 0)
        else:
            raise ValueError(f"Unknown data_type: {data_type}. Must be one of 'trades', 'orderbook', 'candles', or 'any'.")
    
    def clear_data(self, con_id: Optional[int] = None, data_type: Optional[str] = None) -> None:
        """
        Clear data for the specified contract ID and data type.
        
        Parameters:
        -----------
        con_id : int, optional
            The contract ID to clear data for. If None, clear for all contracts.
            Default is None.
        data_type : str, optional
            The type of data to clear - 'trades', 'orderbook', 'candles', or None (all types).
            Default is None.
        """
        if con_id is None:
            # Clear all contracts
            if data_type == "trades" or data_type is None:
                self._trades.clear()
            if data_type == "orderbook" or data_type is None:
                self._orderbook.clear()
            if data_type == "candles" or data_type is None:
                self._candles.clear()
            self._logger.info(f"Cleared all {'data' if data_type is None else data_type} for all contracts")
        else:
            # Clear specific contract
            if data_type == "trades" or data_type is None:
                if con_id in self._trades:
                    del self._trades[con_id]
            if data_type == "orderbook" or data_type is None:
                if con_id in self._orderbook:
                    del self._orderbook[con_id]
            if data_type == "candles" or data_type is None:
                if con_id in self._candles:
                    del self._candles[con_id]
            self._logger.info(f"Cleared {'all data' if data_type is None else data_type} for contract ID {con_id}")
    
    def get_latest_data(self, con_id: int, data_type: str, n: int = 1) -> np.ndarray:
        """
        Get the latest N items of data for a specific contract and data type.
        
        Parameters:
        -----------
        con_id : int
            The contract ID to get data for.
        data_type : str
            The type of data to get - 'trades', 'orderbook', or 'candles'.
        n : int, optional
            The number of latest items to retrieve. Default is 1.
            
        Returns:
        --------
        np.ndarray
            An array containing the latest N items of requested data.
            
        Raises:
        -------
        KeyError
            If the contract ID is not found.
        ValueError
            If the data type is unknown.
        """
        if data_type == "trades":
            if con_id not in self._trades:
                raise KeyError(f"No trade data for contract ID {con_id}")
            buffer = self._trades[con_id]
        elif data_type == "orderbook":
            if con_id not in self._orderbook:
                raise KeyError(f"No orderbook data for contract ID {con_id}")
            buffer = self._orderbook[con_id]
        elif data_type == "candles":
            if con_id not in self._candles:
                raise KeyError(f"No candle data for contract ID {con_id}")
            buffer = self._candles[con_id]
        else:
            raise ValueError(f"Unknown data_type: {data_type}. Must be one of 'trades', 'orderbook', or 'candles'.")
        
        # Get the latest N items
        data = buffer.as_array()
        return data[-min(n, len(data)):]
