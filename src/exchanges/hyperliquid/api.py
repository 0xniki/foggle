import orjson
import aiohttp
import logging
from orjson import JSONDecodeError

from .constants import MAINNET_API_URL
from .error import ClientError, ServerError
from .types import Any

class API:
    def __init__(self, base_url=None):
        self.base_url = base_url or MAINNET_API_URL
        self._session = None
        self._logger = logging.getLogger(__name__)

    def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                json_serialize=self._json_serialize
            )
        return self._session

    @staticmethod
    def _json_serialize(obj):
        # Convert orjson's bytes output to string
        return orjson.dumps(obj).decode('utf-8')

    async def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.base_url + url_path
        
        session = self._ensure_session()
        async with session.post(url, json=payload) as response:
            await self._handle_exception(response)
            try:
                return await response.json(loads=orjson.loads)
            except ValueError:
                text = await response.text()
                return {"error": f"Could not parse JSON: {text}"}

    async def _handle_exception(self, response):
        status_code = response.status
        if status_code < 400:
            return
            
        text = await response.text()
        
        if 400 <= status_code < 500:
            try:
                err = orjson.loads(text)
            except JSONDecodeError:
                raise ClientError(status_code, None, text, None, response.headers)
                
            if err is None:
                raise ClientError(status_code, None, text, None, response.headers)
                
            error_data = err.get("data")
            raise ClientError(status_code, err["code"], err["msg"], response.headers, error_data)
            
        raise ServerError(status_code, text)
        
    async def close(self):
        """Close the aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()