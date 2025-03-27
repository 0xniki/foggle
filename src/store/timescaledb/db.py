import asyncio
import asyncpg
import hashlib

from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional


class Database:
    def __init__(self, config: Dict):
        """
        Initialize the database connector with a DSN connection string
        
        Args:
            dsn: PostgreSQL connection string
        """
        user = config.get('user')
        password = config.get('key')
        host = config.get('host')
        port = config.get('port')
        database = config.get('database')

        self.dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.pool = None
        self._contract_cache = {}

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
            contract_key = str(trade['contract'])
            if contract_key not in trades_by_contract:
                trades_by_contract[contract_key] = []
            trades_by_contract[contract_key].append(trade)
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for contract_key, contract_trades in trades_by_contract.items():
                    contract_data = contract_trades[0]['contract']
                    contract_id = await self.get_or_create_contract(contract_data)

                    values = []
                    for trade in contract_trades:
                        timestamp = datetime.fromtimestamp(trade['timestamp'] / 1000.0)
                        
                        values.append((
                            timestamp,
                            contract_id,
                            float(trade['price']),
                            float(trade['size']),
                            trade.get('side'),
                            trade.get('type'),
                            trade.get('tid')
                        ))

                    await conn.executemany(
                        """
                        INSERT INTO trades (time, contract_id, price, quantity, side, type, trade_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (time, contract_id) DO NOTHING
                        """,
                        values
                    )

    async def insert_news_item(self, source: str, news_data: Dict):
        """
        Insert economic news data with duplicate prevention
        
        Args:
            source: Source name
            news_data: News data dictionary with category, item, timestamp, etc.
        """
        async with self.pool.acquire() as conn:
            # Get or create category
            category_id = await self._get_or_create_category(
                conn, 
                news_data['category'], 
                news_data['item']
            )
            
            # Convert timestamp to datetime if needed
            if isinstance(news_data['timestamp'], (int, float)):
                timestamp = datetime.fromtimestamp(news_data['timestamp'])
            else:
                timestamp = news_data['timestamp']
            
            content = news_data.get('content', '')

            content_hash = stable_hash(content)
            
            # Check if we already have this content stored recently (within last 24 hours)
            existing_news = await conn.fetchval(
                """
                SELECT 1 FROM news_items
                WHERE category_id = $1 
                AND content_hash = $2
                AND time > NOW() - INTERVAL '24 hours'
                LIMIT 1
                """,
                category_id, content_hash
            )
            
            if existing_news:
                # Skip insertion if duplicate found
                return None
                    
            # Generate a unique news ID
            news_id = hash(f"{source}:{news_data['category']}:{news_data['item']}:{timestamp}")
            
            # Insert the news item with content hash
            return await conn.fetchval(
                """
                INSERT INTO news_items 
                (time, news_id, title, content, source, category_id, content_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING news_id
                """,
                timestamp,
                news_id,
                news_data.get('title', ''),
                content,
                source,
                category_id,
                content_hash
            )

    async def _get_or_create_category(self, conn, main_category, subcategory=None):
        """Get or create category and subcategory"""
        formatted_main = self._format_category_name(main_category)

        main_cat_id = await conn.fetchval(
            "SELECT id FROM news_categories WHERE name = $1",
            formatted_main
        )
        
        if not main_cat_id:
            main_cat_id = await conn.fetchval(
                "INSERT INTO news_categories (name) VALUES ($1) RETURNING id",
                formatted_main
            )
        
        if not subcategory:
            return main_cat_id

        formatted_sub = self._format_category_name(subcategory)

        sub_cat_id = await conn.fetchval(
            "SELECT id FROM news_categories WHERE name = $1 AND parent_id = $2",
            formatted_sub, main_cat_id
        )
        
        if not sub_cat_id:
            sub_cat_id = await conn.fetchval(
                "INSERT INTO news_categories (name, parent_id) VALUES ($1, $2) RETURNING id",
                formatted_sub, main_cat_id
            )
        
        return sub_cat_id

    def _format_category_name(self, name):
        """Convert hyphenated lowercase names to proper title case"""
        words = name.replace('-', ' ').split()
        return ' '.join(word.capitalize() for word in words)

    async def get_subcategories(self, category_name):
        """
        Get all subcategories for a given main category
        
        Args:
            category_name: Name of the main category
            
        Returns:
            List of subcategory objects with id and name
        """
        formatted_name = self._format_category_name(category_name)
        
        async with self.pool.acquire() as conn:
            # First get the ID of the main category
            main_cat_id = await conn.fetchval(
                "SELECT id FROM news_categories WHERE name = $1",
                formatted_name
            )
            
            if not main_cat_id:
                return []
            
            # Then get all subcategories
            subcategories = await conn.fetch(
                """
                SELECT id, name 
                FROM news_categories 
                WHERE parent_id = $1
                ORDER BY name
                """,
                main_cat_id
            )
            
            return [dict(sub) for sub in subcategories]

    async def get_news_by_category(self, category_name, subcategory_name=None, limit=50, offset=0):
        """
        Get news items for a specific category and optional subcategory
        
        Args:
            category_name: Name of the main category
            subcategory_name: Name of the subcategory (optional)
            limit: Maximum number of results to return
            offset: Pagination offset
            
        Returns:
            List of news items
        """
        formatted_cat = self._format_category_name(category_name)
        
        async with self.pool.acquire() as conn:
            if subcategory_name:
                formatted_sub = self._format_category_name(subcategory_name)
                
                # Get category ID for the subcategory
                category_id = await conn.fetchval(
                    """
                    SELECT sc.id
                    FROM news_categories sc
                    JOIN news_categories mc ON sc.parent_id = mc.id
                    WHERE mc.name = $1 AND sc.name = $2
                    """,
                    formatted_cat, formatted_sub
                )
                
                if not category_id:
                    return []
            else:
                # Get category ID for the main category
                category_id = await conn.fetchval(
                    "SELECT id FROM news_categories WHERE name = $1",
                    formatted_cat
                )
                
                if not category_id:
                    return []
            
            # Query news items for the category
            news_items = await conn.fetch(
                """
                SELECT ni.time, ni.title, ni.content, ni.source, 
                    nc.name as category_name
                FROM news_items ni
                JOIN news_categories nc ON ni.category_id = nc.id
                WHERE ni.category_id = $1
                ORDER BY ni.time DESC
                LIMIT $2 OFFSET $3
                """,
                category_id, limit, offset
            )
            
            return [dict(item) for item in news_items]

    async def get_latest_news(self, limit=10):
        """
        Get the latest news items across all categories
        
        Args:
            limit: Maximum number of results to return
            
        Returns:
            List of news items with category information
        """
        async with self.pool.acquire() as conn:
            news_items = await conn.fetch(
                """
                SELECT ni.time, ni.title, ni.content, ni.source,
                    nc.name as category_name,
                    (SELECT parent.name 
                        FROM news_categories parent 
                        WHERE parent.id = nc.parent_id) as main_category_name
                FROM news_items ni
                JOIN news_categories nc ON ni.category_id = nc.id
                ORDER BY ni.time DESC
                LIMIT $1
                """,
                limit
            )
            
            return [dict(item) for item in news_items]

def stable_hash(text):

    """Create a stable hash that will be consistent across runs"""

    if not text:

        return 0

    # Get first 200 chars and create a consistent hash

    text_sample = text[:200].encode('utf-8')

    # Use MD5 for speed (we just need consistency, not cryptographic security)

    return int(hashlib.md5(text_sample).hexdigest(), 16) % (2**63)