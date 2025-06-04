import asyncio
import aiohttp
import uuid
from typing import Callable, Dict, List, Literal, Any, Optional

class Client:
    def __init__(self, dispatcher_url: str, concurrency: int = 10):
        self.dispatcher_url = dispatcher_url.rstrip("/")
        self.concurrency = concurrency

    async def _send_single_request(self, session, payload):
        async with session.post(f"{self.dispatcher_url}/dispatch", json=payload) as resp:
            return await resp.json()

    async def do_batch_request(
        self,
        *,
        base_url: str,
        endpoint: str,
        params_builder: Callable[[dict], Dict],
        headers: Dict,
        data: Dict[Any, dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_builder: Optional[Callable[[dict], Any]] = None,
        response_handler: Optional[Callable[[dict, dict], Any]] = None,
    ) -> Dict[dict, Any]:

        semaphore = asyncio.Semaphore(self.concurrency)
        results = None
        if response_handler:
            results: Dict[dict, Any] = {}
        responses: Dict[dict, Any] = {}
        

        async with aiohttp.ClientSession() as session:
            async def worker(key, value):
                async with semaphore:
                    payload = {
                        "id": str(uuid.uuid4()),
                        "base_url": base_url,
                        "endpoint": endpoint,
                        "params": params_builder(value),
                        "headers": headers,
                        "method": method,
                        "body": body_builder(value) if (method == 'POST' and body_builder) else None,
                    }
                    response = await self._send_single_request(session, payload)
                    if response_handler:
                        result = response_handler(response, value) if response_handler else response
                        results[key] = result
                    else:
                        responses[key] = result

            tasks = [asyncio.create_task(worker(key, value)) for key, value in data.items()]
            await asyncio.gather(*tasks)
        if results:
            return results
        return responses