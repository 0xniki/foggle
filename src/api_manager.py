import os
import importlib
import inspect
import asyncio
import logging

from typing import Dict, List, Any, Optional, Type
from asyncio.tasks import Task
from src.exchanges.base import Exchange


class APIManager:
    
    def __init__(self):
        self._tasks: List[Task] = []
        self._logger = logging.getLogger("api_manager")

        self.exchanges: Dict[str, Exchange] = {}
        
    async def start(self, config: Dict):
        # Load exchanges from config
        await self._load_exchanges(config)
        
        # Start monitoring exchanges
        monitoring_task = asyncio.create_task(self._monitor_exchanges())
        
        self._tasks.append(monitoring_task)
    
    async def _load_exchanges(self, config: Dict):
        exchanges_path = os.path.join('src', 'exchanges')
        available_modules = {}
        module_to_path = {}
        
        if not os.path.exists(exchanges_path):
            self._logger.error(f"Exchanges path {exchanges_path} does not exist")
            return

        for folder in os.listdir(exchanges_path):
            folder_path = os.path.join(exchanges_path, folder)

            if folder.startswith('__') or folder.startswith('.') or not os.path.isdir(folder_path):
                continue
            
            available_modules[folder.lower()] = f"src.exchanges.{folder}"
            
            for item in os.listdir(folder_path):
                if not item.endswith('.py') or item.startswith('__') or item.startswith('.'):
                    continue
                
                module_name = item[:-3]
                module_path = f"src.exchanges.{folder}.{module_name}"
                available_modules[module_name.lower()] = module_path
                available_modules[f"{folder.lower()}.{module_name.lower()}"] = module_path
                
                module_to_path[module_name.lower()] = folder.lower()
        
        for exchange_name, exchange_config in config.items():
            if not isinstance(exchange_config, dict) or exchange_config.get('type') != 'EXCHANGE':
                continue

            module_name = exchange_config.get('module_name', exchange_name.lower())
            module_name = module_name.lower()
            
            module_path = None

            if module_name in available_modules:
                module_path = available_modules[module_name]
                self._logger.info(f"Found module at {module_path}")
            
            
            if not module_path:
                self._logger.error(f"Could not find module '{module_name}' for exchange {exchange_name}.")
                continue
            
            try:
                module = importlib.import_module(module_path)
                
                exchange_class = None
                candidates = [module_name.upper(), exchange_name.upper()]
                
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and (obj.__module__ == module_path or obj.__module__.startswith(f"{module_path}.")):
                        if name.upper() in candidates:
                            exchange_class = obj
                            break
                
                exchange = exchange_class()
                
                if asyncio.iscoroutinefunction(getattr(exchange, "connectAsync", None)):
                    await exchange.connectAsync(exchange_config)
                elif asyncio.iscoroutinefunction(getattr(exchange, "connect", None)):
                    await exchange.connect(exchange_config)
                elif hasattr(exchange, "connect"):
                    exchange.connect(exchange_config)
                else:
                    self._logger.warning(f"Exchange {exchange_name} does not have a connect method")
                    continue
                
                self.exchanges[exchange_name] = exchange
                
            except Exception as e:
                self._logger.error(f"Error loading exchange {exchange_name}: {e}", exc_info=True)
    
    async def _monitor_exchanges(self):
        pass
    
    async def shutdown(self):
        self._logger.info("Shutting down all exchanges")
        
        for name, exchange in self.exchanges.items():
            try:
                if asyncio.iscoroutinefunction(getattr(exchange, "disconnect", None)):
                    await exchange.disconnect()
                elif hasattr(exchange, "disconnect"):
                    exchange.disconnect()
            except Exception as e:
                self._logger.error(f"Error disconnecting from {name}: {e}")
        
        for task in self._tasks:
            task.cancel()

        self._logger.info("All exchanges shut down")