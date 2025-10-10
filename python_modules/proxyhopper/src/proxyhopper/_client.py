import asyncio
import logging
import sys
import time
import aiohttp
import uuid
import tqdm.asyncio
from typing import Awaitable, Callable, Dict, List, Literal, Any, Optional, TypeVar, Union, overload

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
            avg_wait_time = f'{self._total_wait_time/float(self._attempts):.2f}s' if self._attempts>0 else 'n/a'
            return str(f"""
            Batch Request Statistics:
            Time elapsed: {self.t_end - self.t_start:.2f}s
            Requests sent: {self._attempts}
            Failures: {self._failures}
            Average wait time: {avg_wait_time}
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
    
    def _construct_payload(
            self,
            *,
            target_url:str,
            endpoint:str,
            method:Literal['GET', 'POST'],
            params:Optional[Dict[str,str]] = None,
            headers:Optional[Dict[str,str]] = None,
            body:Optional[Dict[str,Any]] = None

    ) -> Dict[str, Any]:
        payload = {
            "id": str(uuid.uuid4()),
            "target_url": target_url,
            "endpoint": endpoint,
            "params": params,
            "headers": headers,
            "method": method,
            "body": body,
        }
        return payload

    async def _send_single_request_async(
            self,
            payload:Dict,
            on_failure:Literal['ignore', 'fail']
        ) -> Dict:
        t0 = time.time()
        success = True
        try:
            self.logger.debug(f'Sending request to dispatcher.  Payload: {payload}')
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.host}/dispatch", json=payload) as resp:
                    self.logger.debug(f'Got response: {resp.status} - {resp.content_type}')
                    output = await resp.json()
                    #self.logger.debug(f'Output: {output}')
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
    
    async def send_single_request_async(
            self,
            *,
            target_url: str,
            endpoint: str,
            method: Literal['GET', 'POST'] = 'GET',
            headers: Optional[Dict] = None,
            params: Optional[Dict] = None,
            body: Optional[Any] = None,
            on_failure:Literal['ignore', 'fail'],

    ) -> Dict:
        if self.record_statistics:
            self.statistics.record_start()
        
        async with aiohttp.ClientSession() as session:
            payload = self._construct_payload(
                target_url=target_url,
                endpoint=endpoint,
                method=method,
                params=params,
                headers=headers,
                body=body
            )
            response = await self._send_single_request_async(payload, on_failure)
        
        if self.record_statistics:
            self.statistics.record_end()
            self.logger.info(self.statistics.present_report())
        
        return response
    
    @overload
    async def send_batched_requests_async(
        self,
        *,
        target_url: str,
        endpoint: Optional[str],
        endpoint_factory: Optional[Callable[[Dict], str]],
        param_factory: Optional[Callable[[Dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, Dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[Dict], Any]] = None,
        response_handler: Callable[[Dict, Dict], Awaitable[T2]],
        on_failure:Literal['ignore', 'fail'],
        progress_reporting:Literal['bar','text','off'] = 'text'
    ) -> Dict[T1, T2]:
        ...

    @overload
    async def send_batched_requests_async(
        self,
        *,
        target_url: str,
        endpoint: Optional[str],
        endpoint_factory: Optional[Callable[[Dict], str]],
        param_factory: Optional[Callable[[Dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, Dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[Dict], Any]] = None,
        on_failure:Literal['ignore', 'fail'],
        progress_reporting:Literal['bar','text','off'] = 'text'
    ) -> Dict[T1, Dict]:
        ...

    async def send_batched_requests_async(
        self,
        *,
        target_url: str,
        endpoint: Optional[str] = None,
        endpoint_factory: Optional[Callable[[Dict], str]] = None,
        param_factory: Optional[Callable[[Dict], Dict]],
        headers: Optional[Dict] = None,
        data: Dict[T1, Dict],
        method: Literal['GET', 'POST'] = 'GET',
        body_factory: Optional[Callable[[Dict], Any]] = None,
        response_handler: Optional[Callable[[Dict, Dict], Awaitable[T2]]] = None,
        on_failure:Literal['ignore', 'fail'],
        progress_reporting:Literal['bar','text','off'] = 'text'
    ) -> Union[Dict[T1, Dict],Dict[T1, T2]]:
        semaphore = asyncio.Semaphore(self.concurrency)
        results: Dict[T1, T2] = {}
        if response_handler:
            results = {}
        responses: Dict[T1, Dict] = {}       

        if endpoint is None and endpoint_factory is None:
            raise ValueError(f'endpoint and endpoint_factory parameters cannot both be None.  One must be passed')
        if endpoint and endpoint_factory:
            raise ValueError(f'Only one of endpoint and endpoint_factory can be passed to the function')

        if self.record_statistics:
            self.statistics.record_start()
        async def worker(key, value):
            async with semaphore:

                endpoint_ = endpoint
                if endpoint_factory:
                    endpoint_ = endpoint_factory(value)

                payload = {
                    "id": str(uuid.uuid4()),
                    "target_url": target_url,
                    "endpoint": endpoint_,
                    "params": param_factory(value) if (param_factory) else None,
                    "headers": headers,
                    "method": method,
                    "body": body_factory(value) if (method == 'POST' and body_factory) else None,
                }

                response = await self._send_single_request_async(payload, on_failure)
                if response_handler:
                    result = await response_handler(response, value) if response_handler else response
                    results[key] = result
                else:
                    responses[key] = response

        tasks = [asyncio.create_task(worker(key, value)) for key, value in data.items()]
        # await asyncio.gather(*tasks)
        tasks_completed = 0
        for task in tqdm.asyncio.tqdm.as_completed(tasks, smoothing=0.1, total=len(data), disable=(progress_reporting != 'bar')):
            await task
            # Track tasks completed for progress reporting purposes
            tasks_completed+=1
            if progress_reporting == 'text' and tasks_completed%max(len(data)//20,1) == 0:
                self.logger.info(f'Completed {tasks_completed}/{len(data)} requests')

        if self.record_statistics:
            self.statistics.record_end()
            self.logger.info(self.statistics.present_report())
        if response_handler:
            return results
        return responses