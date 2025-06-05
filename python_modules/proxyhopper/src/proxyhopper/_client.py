import asyncio
import logging
import sys
import time
import aiohttp
import uuid
import tqdm.asyncio
from typing import Callable, Dict, List, Literal, Any, Optional, TypeVar, Union, overload

T1 = TypeVar('T1')
T2 = TypeVar('T2')

class Client:

    class BatchedRequestStatistics():
        
        def __init__(self):
            self._failures = 0
            self._attempts = 0
            self._total_wait_time = 0
            self.t_start = None
            self.t_end = None
        
        def record_start(self):
            self.t_start = time.time()

        def record_end(self):
            self.t_end = time.time()
        
        def record_attempt(self, failure:bool, wait_time:float):
            self._attempts += 1
            self._total_wait_time += wait_time
            self._failures += failure
        
        def present_report(self) -> str:
            assert self.t_start is not None
            assert self.t_end is not None
            return str(f"""
            Batch Request Statistics:
            Time elapsed: {self.t_end - self.t_start:.2f}s
            Requests sent: {self._attempts}
            Failures: {self._failures}
            Average wait time: {self._total_wait_time/float(self._attempts):.2f}s
            """).strip()

    def __init__(
            self,
            *,
            host:str = 'http://localhost:8000',
            concurrency: int = 10,
            log_level:Literal['NOTSET','DEBUG','INFO','WARN','ERROR','CRITICAL']='INFO',
            record_statistics: bool = False
    ):
        self.host = host.rstrip("/")
        self.concurrency = concurrency
        self.record_statistics = record_statistics
        if self.record_statistics:
            self.statistics = Client.BatchedRequestStatistics()

        # Setup logger
        self.logger = logging.getLogger('proxyhopper')
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler(sys.stdout)

        handler.setFormatter(formatter)
        # Clear existing handlers and add new one
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(handler)

        self.logger.info(f"Logger initialized with level {log_level}")

    async def _send_single_request(
            self,
            session:aiohttp.ClientSession,
            payload:Dict,
            on_failure:Literal['ignore', 'fail']
        ) -> Dict:
        t0 = time.time()
        success = True
        try:
            self.logger.debug(f'Sending request to dispatcher.  Payload: {payload}')
            async with session.post(f"{self.host}/dispatch", json=payload) as resp:
                self.logger.debug(f'Got response: {resp.status} - {resp.content_type}')
                output = await resp.json()
                self.logger.debug(f'Output: {output}')
                return output
        except Exception as e:
            success = False
            if on_failure == 'fail':
                raise e
            elif on_failure == 'ignore':
                self.logger.warning(f'Request failed.  Payload: {payload}')
                return {}
            else:
                raise ValueError(f'Unexpected value for parameter on_failure: {on_failure}')
        finally:
            if self.record_statistics:
                self.statistics.record_attempt(not success, time.time() - t0)
    
    @overload
    async def send_batched_requests(
        self,
        *,
        target_url: str,
        endpoint: str,
        param_factory: Optional[Callable[[dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[dict], Any]] = None,
        response_handler: Callable[[dict, dict], T2],
        on_failure:Literal['ignore', 'fail']
    ) -> Dict[T1, T2]:
        ...

    @overload
    async def send_batched_requests(
        self,
        *,
        target_url: str,
        endpoint: str,
        param_factory: Optional[Callable[[dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[dict], Any]] = None,
        on_failure:Literal['ignore', 'fail']
    ) -> Dict[T1, dict]:
        ...

    async def send_batched_requests(
        self,
        *,
        target_url: str,
        endpoint: str,
        param_factory: Optional[Callable[[dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[dict], Any]] = None,
        response_handler: Optional[Callable[[dict, dict], T2]] = None,
        on_failure:Literal['ignore', 'fail']
    ) -> Union[Dict[T1, dict],Dict[T1, T2]]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: Dict[T1, T2] = {}
        if response_handler:
            results = {}
        responses: Dict[T1, Dict] = {}        

        if self.record_statistics:
            self.statistics.record_start()

        async with aiohttp.ClientSession() as session:
            async def worker(key, value):
                async with semaphore:
                    payload = {
                        "id": str(uuid.uuid4()),
                        "target_url": target_url,
                        "endpoint": endpoint,
                        "params": param_factory(value) if (param_factory) else None,
                        "headers": headers,
                        "method": method,
                        "body": body_factory(value) if (method == 'POST' and body_factory) else None,
                    }

                    response = await self._send_single_request(session, payload, on_failure)
                    if response_handler:
                        result = response_handler(response, value) if response_handler else response
                        results[key] = result
                    else:
                        responses[key] = response

            tasks = [asyncio.create_task(worker(key, value)) for key, value in data.items()]
            # await asyncio.gather(*tasks)
            for task in tqdm.asyncio.tqdm.as_completed(tasks, smoothing=0.1, total=len(data)):
                await task

        if self.record_statistics:
            self.statistics.record_end()
            self.logger.info(self.statistics.present_report())
        if response_handler:
            return results
        return responses