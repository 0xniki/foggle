import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup

from typing import Dict, List

class TradingEconomics:
    BASE_URL = "https://tradingeconomics.com/"

    def __init__(self, topics: Dict[str, List[str]]):
        self.logger = logging.getLogger(__name__)
        self.topics = topics
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self._task = None
        
    def _construct_url(self, category, item):
        return f"{self.BASE_URL}{category}/{item}"
    
    async def scrape_item(self, category, item):
        url = self._construct_url(category, item)
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        html = await response.text()
                        return self._extract_data(html, category, item)
                    else:
                        self.logger.error(f"Failed to retrieve {url}, status code: {response.status}")
                        return None
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {str(e)}")
                return None
    
    def _extract_data(self, html, category, item):
        """Extract data from HTML content."""
        soup = BeautifulSoup(html, 'html.parser')
        data = {
            "category": category,
            "item": item,
            "timestamp": asyncio.get_event_loop().time(),
            "data": {}
        }
        
        # Extract historical description
        historical_desc = soup.find('div', id='historical-desc')
        if historical_desc:
            title_tag = historical_desc.find(['h2', 'h3'])
            if title_tag:
                data["data"]["title"] = title_tag.text.strip()
            
            paragraphs = historical_desc.find_all('p')
            data["data"]["description"] = " ".join([p.text.strip() for p in paragraphs])
        
        # Extract statistics
        stats_div = soup.find('div', id='stats')
        if stats_div:
            stats_table = stats_div.find('table')
            if stats_table:
                rows = stats_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].text.strip().lower().replace(' ', '_')
                        value = cells[1].text.strip()
                        data["data"][key] = value
        
        # Extract latest value
        latest_div = soup.find('div', {'class': 'table-responsive'})
        if latest_div:
            data["data"]["latest_values"] = {}
            table = latest_div.find('table')
            if table:
                rows = table.find_all('tr')
                headers = [th.text.strip().lower().replace(' ', '_') for th in rows[0].find_all('th')] if rows else []
                
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) == len(headers):
                        for i, header in enumerate(headers):
                            data["data"]["latest_values"][header] = cells[i].text.strip()
        
        return data
    
    async def scrape_all(self):
        tasks = []
        
        for category, items in self.topics.items():
            for item in items:
                tasks.append(self.scrape_item(category, item))
        
        # Use gather to run all scraping tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Filter out None values (failed scrapes)
        results = [data for data in results if data]
        
        for data in results:
            pass
            # print(data)
        
        return results
    
    async def _scrape_loop(self, interval: int = 3600):
        while True:
            try:
                self.logger.debug("Scraping all topics")
                results = await self.scrape_all()
                self.logger.info(f"Scraped {len(results)} items successfully")
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")

            await asyncio.sleep(interval)
    
    def start_scrape(self, interval: int = 3600):
        """Start scraping in the background as a task"""
        if self._task is None or self._task.done():
            self.logger.debug("Creating background scraping task")
            self._task = asyncio.create_task(self._scrape_loop(interval))
        return self._task
    
    def stop_scrape(self):
        """Stop the background scraping task"""
        if self._task and not self._task.done():
            self.logger.info("Stopping background scraping task")
            self._task.cancel()