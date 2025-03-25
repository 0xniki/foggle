import asyncio
import asyncpg
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional


class Database:
    def __init__(self, dsn: str):
        """
        Initialize the database connector with a DSN connection string
        
        Args:
            dsn: PostgreSQL connection string
        """
        self.dsn = dsn
        self.pool = None
        self._contract_cache = {}  # in-memory cache of contracts to avoid repeated lookups

    async def init_pool(self):
        """Initialize the connection pool"""
        self.pool = await asyncpg.create_pool(self.dsn)
        
    async def close(self):
        """Close all connections"""
        if self.pool:
            await self.pool.close()
    
    async def get_or_create_contract(self, contract_data: Dict[str, Any]) -> int:
        """
        Get a contract ID from the database or create it if it doesn't exist
        
        Args:
            contract_data: Contract details
            
        Returns:
            contract_id: The ID of the contract
        """
        # Create a cache key
        cache_key = (
            contract_data.get('symbol', ''),
            contract_data.get('secType', ''),
            contract_data.get('exchange', ''),
            contract_data.get('expiration', '')
        )
        
        # Check cache first
        if cache_key in self._contract_cache:
            return self._contract_cache[cache_key]
        
        # If not in cache, check database
        async with self.pool.acquire() as conn:
            # Build query based on contract type
            if contract_data.get('secType') == 'FUT':
                # For futures, include expiration
                contract = await conn.fetchrow(
                    """
                    SELECT id FROM contracts 
                    WHERE symbol = $1 AND sec_type = $2 AND exchange = $3 
                    AND currency = $4 AND expiration = $5
                    """,
                    contract_data.get('symbol'),
                    contract_data.get('secType'),
                    contract_data.get('exchange'),
                    contract_data.get('currency'),
                    contract_data.get('expiration')
                )
            else:
                # For crypto and stocks without expiration
                contract = await conn.fetchrow(
                    """
                    SELECT id FROM contracts 
                    WHERE symbol = $1 AND sec_type = $2 AND exchange = $3 
                    AND currency = $4 AND expiration IS NULL
                    """,
                    contract_data.get('symbol'),
                    contract_data.get('secType'),
                    contract_data.get('exchange'),
                    contract_data.get('currency')
                )
            
            if contract:
                contract_id = contract['id']
            else:
                # Insert new contract
                contract_id = await conn.fetchval(
                    """
                    INSERT INTO contracts 
                    (symbol, sec_type, exchange, currency, multiplier, expiration, strike, option_right)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id
                    """,
                    contract_data.get('symbol'),
                    contract_data.get('secType'),
                    contract_data.get('exchange'),
                    contract_data.get('currency'),
                    contract_data.get('multiplier', 1),  # Default multiplier to 1
                    contract_data.get('expiration'),
                    contract_data.get('strike'),
                    contract_data.get('right')
                )
            
            # Update cache
            self._contract_cache[cache_key] = contract_id
            return contract_id

    async def insert_trades(self, trades: List[Dict[str, Any]]):
        """
        Insert trade data into the trades table
        
        Args:
            trades: List of trade data
        """
        if not trades:
            return
            
        # Group trades by contract to reduce number of contract lookups
        trades_by_contract = {}
        for trade in trades:
            contract_key = str(trade['contract'])  # Use the contract dict as a key
            if contract_key not in trades_by_contract:
                trades_by_contract[contract_key] = []
            trades_by_contract[contract_key].append(trade)
        
        # Process each contract group
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for contract_key, contract_trades in trades_by_contract.items():
                    # Get or create contract once per group
                    contract_data = contract_trades[0]['contract']
                    contract_id = await self.get_or_create_contract(contract_data)
                    
                    # Prepare values for bulk insert
                    values = []
                    for trade in contract_trades:
                        # Convert unix milliseconds to timestamptz
                        timestamp = datetime.fromtimestamp(trade['timestamp'] / 1000.0)
                        
                        # Default trade_id to timestamp if not provided
                        trade_id = trade.get('tid', int(trade['timestamp']))
                        
                        # Default to BUY side if not specified
                        side = trade.get('side', 'BUY')
                        # Map 'A' to 'SELL' and 'B' to 'BUY' if needed
                        if side == 'A':
                            side = 'SELL'
                        elif side == 'B':
                            side = 'BUY'
                        
                        values.append((
                            timestamp,
                            contract_id,
                            float(trade['price']),
                            float(trade['size']),
                            side,
                            'MARKET',  # Assuming all are market trades
                            trade_id
                        ))
                    
                    # Use executemany for bulk insert
                    await conn.executemany(
                        """
                        INSERT INTO trades (time, contract_id, price, quantity, side, type, trade_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (time, contract_id, trade_id) DO NOTHING
                        """,
                        values
                    )


async def main():
    
    # Example trade data
    trades_batch1 = [
        {'contract': {'symbol': 'NQ', 'expiration': '202506', 'exchange': 'CME', 'currency': 'USD', 'secType': 'FUT'}, 
         'timestamp': 1742875381380, 'price': 20338.0, 'size': 1.0},
        {'contract': {'symbol': 'NQ', 'expiration': '202506', 'exchange': 'CME', 'currency': 'USD', 'secType': 'FUT'}, 
         'timestamp': 1742875381380, 'price': 20338.0, 'size': 2.0}
    ]
    
    trades_batch2 = [
        {'contract': {'symbol': 'MES', 'expiration': '202506', 'exchange': 'CME', 'currency': 'USD', 'secType': 'FUT'}, 
         'timestamp': 1742875382279, 'price': 5807.25, 'size': 1.0}
    ]
    
    trades_batch3 = [
        {'contract': {'symbol': 'SOL', 'exchange': 'HYPERLIQUID', 'currency': 'USD', 'secType': 'CRYPTO'}, 
         'timestamp': 1742875379873, 'price': 138.18, 'size': 1.9, 'side': 'A', 
         'hash': '0xf5a52df6d598821fecc004203821ce02030600adc5313b9dcc48a29f5abbda0f', 
         'tid': 54155166059875, 
         'users': ['0x4b25eee404202497f35d2a42f8d560f3c8a8fc91', '0xcdd8ded304e66bfd08fb8d71996c703649d12b9f']}
    ]
    
    # Initialize database connection
    db = Database('postgresql://postgres:password@localhost:5432/postgres')
    await db.init_pool()
    
    try:
        # Process each batch of trades
        await db.insert_trades(trades_batch1)
        await db.insert_trades(trades_batch2)
        await db.insert_trades(trades_batch3)
        
        print("Trade data inserted successfully")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())