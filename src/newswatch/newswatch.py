import asyncio
import logging

from typing import List, Dict, Any

class NewsWatch:
    
    def __init__(self, data_handler=None):
        self.logger = logging.getLogger(__name__)
        self.scrapers = {}
        self.tasks = {}
        self.data_handler = data_handler
    
    def register_scraper(self, name: str, scraper) -> None:
        """Register a scraper with the NewsWatch"""
        if name in self.scrapers:
            self.logger.warning(f"Overwriting existing scraper: {name}")
        
        self.scrapers[name] = scraper
        self.logger.info(f"Registered scraper: {name}")
    
    def unregister_scraper(self, name: str) -> None:
        """Unregister a scraper and stop its tasks if running"""
        if name not in self.scrapers:
            self.logger.warning(f"Scraper not found: {name}")
            return
        
        self.stop_scraper(name)
        del self.scrapers[name]
        self.logger.info(f"Unregistered scraper: {name}")
    
    async def _scrape_loop(self, name: str, interval: int) -> None:
        """Background loop for periodic scraping"""
        scraper = self.scrapers[name]
        
        while True:
            try:
                self.logger.debug(f"Running scraper: {name}")
                results = await scraper.scrape_all()
                self.logger.info(f"Scraper {name} completed with {len(results)} items")
                
                # Handle the scraped data
                if self.data_handler:
                    await self.data_handler(name, results)
                
            except asyncio.CancelledError:
                self.logger.info(f"Scraper {name} task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in scraper {name}: {str(e)}")
            
            await asyncio.sleep(interval)
    
    def start_scraper(self, name: str, interval: int = 3600) -> None:
        """Start a scraper in the background"""
        if name not in self.scrapers:
            self.logger.error(f"Scraper not registered: {name}")
            return
        
        if name in self.tasks and not self.tasks[name].done():
            self.logger.warning(f"Scraper {name} already running")
            return
        
        self.logger.info(f"Starting scraper: {name} with interval {interval}s")
        self.tasks[name] = asyncio.create_task(self._scrape_loop(name, interval))
    
    def stop_scraper(self, name: str) -> None:
        """Stop a running scraper"""
        if name not in self.tasks or self.tasks[name].done():
            self.logger.warning(f"Scraper {name} not running")
            return
        
        self.logger.info(f"Stopping scraper: {name}")
        self.tasks[name].cancel()
    
    def start_all(self, interval: int = 3600) -> None:
        """Start all registered scrapers"""
        for name in self.scrapers:
            self.start_scraper(name, interval)
    
    def stop_all(self) -> None:
        """Stop all running scrapers"""
        for name in list(self.tasks.keys()):
            self.stop_scraper(name)
    
    async def run_once(self, name: str) -> List[Dict[str, Any]]:
        """Run a scraper once and return results"""
        if name not in self.scrapers:
            self.logger.error(f"Scraper not registered: {name}")
            return []
        
        try:
            results = await self.scrapers[name].scrape_all()
            
            # Handle the scraped data
            if self.data_handler:
                await self.data_handler(name, results)
                
            return results
        except Exception as e:
            self.logger.error(f"Error running scraper {name}: {str(e)}")
            return []