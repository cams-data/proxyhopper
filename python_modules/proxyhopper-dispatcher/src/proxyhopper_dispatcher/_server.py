from collections import defaultdict, deque
import os
import sys
from typing import Dict, Deque, Generator, List, Literal, Optional, Set, Tuple, Any, Union, overload
from pydantic import BaseModel, Field, PrivateAttr
from ._config import TargetUrlConfig
import time
import asyncio
import time
import logging
from aiohttp import web, ClientSession, ClientTimeout
from ._config import ProxyhopperConfig, TargetUrlConfig
from datetime import datetime, timedelta, timezone

class QuarantineEntry(BaseModel):
    """
    Data class holding data for a quarantined proxy
    """
    until: float = 0
    strikes: int = 0
    next_duration: float = 2

class TargetContext(BaseModel):
    queue: Deque[Tuple[dict, Any]] = Field(default_factory=deque)
    last_used: Dict[str, float] = Field(default_factory=lambda: defaultdict(float))
    quarantine: Dict[str, QuarantineEntry] = Field(default_factory=lambda: defaultdict(QuarantineEntry))
    in_use_proxies: Set[str] = Field(default_factory=set)

    def report(self) -> str:
        return f"\t{len(self.queue)} requests in queue.\n\t{len([x for x in self.quarantine.values() if x.until > time.time()])} proxies in quarantine."

class DispatcherContext(BaseModel):
    """
    Context providing data for dispatcher server
    """
    proxies: List[str]
    target_urls: List[str]
    _target_contexts:Dict[str, TargetContext] = PrivateAttr()

    start_time:datetime = datetime(1970,1,1,tzinfo=timezone.utc)
    requests_sent:int = 0
    five_hundred_errors:int = 0
    retries:int = 0
    

    def model_post_init(self, context):
        self._target_contexts = {target_url:TargetContext() for target_url in self.target_urls}
    
    @overload
    def __getitem__(
            self,
            indices:str
    ) -> TargetContext:
        ...

    @overload
    def __getitem__(
            self,
            indices:tuple
    ) -> Dict[str,TargetContext]:
        ...
    
    def __getitem__(
            self,
            indices:Union[str,tuple]
    ) -> Union[TargetContext,Dict[str,TargetContext]]:
        if isinstance(indices, str):
            return self._target_contexts[indices]
        if isinstance(indices, tuple):
            return {key:self._target_contexts[key] for key in indices}
        raise TypeError(f'Subscripting DispatcherContext with a {type(indices)} is not supported.')

    def iter_target_contexts(self) -> Generator[Tuple[str, TargetContext], None, None]:
        for target_url, ctx in self._target_contexts.items():
            yield (target_url, ctx)

class DispatcherServer:
    def __init__(
            self,
            config: ProxyhopperConfig,
            log_level:Literal['NOTSET','DEBUG','INFO','WARN','ERROR','CRITICAL']='INFO',
            log_output='stdout',
            **kwargs
        ):
        self.config = config
        self.ctx = DispatcherContext(
            proxies=config.proxies,
            target_urls=[key for key in config.target_urls]
        )
        self._background_tasks:Set[asyncio.Task] = set()

        # Setup logger
        self.logger = logging.getLogger('proxyhopper')
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        if log_output == 'file':
            log_filename = os.path.join(os.getcwd(), 'proxyhopper.log')
            handler = logging.FileHandler(log_filename)
        else:
            handler = logging.StreamHandler(sys.stdout)

        handler.setFormatter(formatter)
        # Clear existing handlers and add new one
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(handler)

        self.logger.info(f"Logger initialized with level {log_level} and output {log_output}")

        self._proxies_are_fake = bool(kwargs.get('proxies_are_fake', False))

        self.logger.info(f"Dispatcher initialized.  Configured to hit {len(self.ctx.target_urls)} target(s) using {len(self.ctx.proxies)} proxies.")

    def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/dispatch", self.dispatch_request)
        app.router.add_get("/health-check", self.health_check)
        app.router.add_get("/targets", self.targets)
        app.on_startup.append(self._start_background_tasks)
        app.on_cleanup.append(self._cleanup_background_tasks)
        app.on_shutdown.append(lambda app: asyncio.create_task(self.shutdown()))
        self.logger.info('Created app! Awaiting dispatch requests.')
        self.ctx.start_time = datetime.now(timezone.utc)
        return app
    
    async def health_check(self, request: web.Request) -> web.Response:
        return web.Response(status=200)
    
    async def targets(self, request: web.Request) -> web.Response:
        """
        Endpoint that returns json containing information on each of the configured end points
        """
        output = {}
        for target_url, target_ctx in self.ctx.iter_target_contexts():
            output[target_url] = {
                'proxies_quarantined':len([x for x in target_ctx.quarantine.values() if x.until > time.time()]),
                'requests_queued':len(target_ctx.queue)
            }
        return web.json_response(data = output, status=200)

    async def dispatch_request(self, request: web.Request) -> web.Response:
        payload = await request.json() # Grab requests json
        payload.setdefault("retries", 0) # Populate a retries value
        payload.setdefault("queue_time", time.time())
        target_url = payload["target_url"]

        if target_url not in self.config.target_urls:
            return web.json_response({'error':f'Base url: "{target_url}" has not been configured for dispatch server.  Please add it to the config.' } ,status=400)        

        target_ctx = self.ctx[target_url]

        target_ctx.queue.append((payload, asyncio.get_event_loop().create_future()))
        self.logger.debug(f'Received request to: {target_url}.  Queue now contains {len(target_ctx.queue)} request(s)!')
        return await target_ctx.queue[-1][1]

    async def _start_background_tasks(self, app):
        app["dispatcher"] = asyncio.create_task(self._request_dispatcher())
        app["health_checker"] = asyncio.create_task(self._health_checker())

    async def _cleanup_background_tasks(self, app):
        app["dispatcher"].cancel()
        app["health_checker"].cancel()
        await app["dispatcher"]
        await app["health_checker"]
    
    def get_next_proxy(self, target_ctx: TargetContext, min_interval: float) -> Tuple[Optional[str], float]:
        eligible_proxies = [
            p for p in self.ctx.proxies
            if time.time() > target_ctx.quarantine[p].until
            and time.time() - target_ctx.last_used[p] >= min_interval
            and p not in target_ctx.in_use_proxies
        ]
        if not eligible_proxies:
            return (None, min([q.until for q in target_ctx.quarantine.values()]))
        return (min(eligible_proxies, key=lambda p: target_ctx.last_used[p]),0)
    
    async def shutdown(self):
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
    
    async def _health_checker(self):
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError as e:
                return
            targets_report = "\n".join(['- ' + target_url + ':\n' + target_ctx.report() for target_url, target_ctx in self.ctx.iter_target_contexts()])
            self.logger.info(f"""
{'='*13} Health Check {'='*13}
    System time: {datetime.now()}
    Dispatcher alive for: {(datetime.now(timezone.utc) - self.ctx.start_time)}
    Sent {self.ctx.requests_sent} requests.
    Received {self.ctx.five_hundred_errors} 5XX errors.
    Performed {self.ctx.retries} retries.
    Target Info:
    {targets_report}
{'-'*40}"""
            )
            

    async def _request_dispatcher(self):
        while True:
            for target_url, target_ctx in self.ctx.iter_target_contexts():
                if not target_ctx.queue:
                    continue
                payload, result_future = target_ctx.queue[0]
                min_interval = self.config.target_urls[target_url].min_request_interval
                proxy, next_available = self.get_next_proxy(target_ctx, min_interval)
                if proxy:
                    target_ctx.queue.popleft()
                    target_ctx.in_use_proxies.add(proxy)
                    task = asyncio.create_task(self._handle_request(payload, result_future, target_url, proxy))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                elif 'max_wait_time' in payload and payload['max_wait_time'] < next_available - time.time():
                    target_ctx.queue.popleft()
                    result_future.set_result(web.json_response({"error": "A proxy will not be released from quarantine before the wait time expires"}, status=429))
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError as e:
                return


    async def _handle_request(self, payload, result_future, target_url, proxy):
        try:
            endpoint = payload["endpoint"]
            method = payload["method"]
            params = payload.get("params", {})
            headers = payload.get("headers", {})
            body = payload.get("body")
            timeout_seconds = payload.get("timeout_seconds")
            retries = payload.get("retries", 0)

            full_url = f"{target_url.rstrip('/')}/{endpoint.lstrip('/')}"
            proxy_url = f"http://{proxy}"
            if self._proxies_are_fake:
                proxy_url = None

            self.logger.debug(f'Sending request to: {full_url} using proxy: {proxy}.  Params: {params}.  Body: {body}.  After {time.time() - payload["queue_time"]:.2f}s in queue.')
            t0 = time.time()
        
            timeout = ClientTimeout(total=timeout_seconds) if timeout_seconds else None
            async with ClientSession(timeout=timeout) as session:
                if method == "GET":
                    async with session.get(full_url, params=params, headers=headers, proxy=proxy_url) as resp:
                        status = resp.status
                        if resp.content_type == 'application/json':
                            result = await resp.json()
                        elif resp.content_type == 'text/html':
                            result = await resp.text()
                        else:
                            raise Exception(f'Received unhandled content type: {resp.content_type}')
                elif method == "POST":
                    async with session.post(full_url, json=body, headers=headers, proxy=proxy_url) as resp:
                        status = resp.status
                        if resp.content_type == 'application/json':
                            result = await resp.json()
                        elif resp.content_type == 'text/html':
                            result = await resp.text()
                        else:
                            raise Exception(f'Received unhandled content type: {resp.content_type}')
                else:
                    result = {"error": "Unsupported method"}
                    status = 400
            
            self.logger.debug(f'Received response.  Time elapsed: {time.time() - t0:.2f}s.  Status: {status}.')
            self.ctx.requests_sent += 1
            target_ctx = self.ctx[target_url]
            
            target_ctx.last_used[proxy] = time.time()
            target_ctx.in_use_proxies.discard(proxy)
            if 500 <= status < 600:
                self.logger.warning(f'Request returned {status} error:\n{result}.  Retries: {payload["retries"]}')
                target_ctx.quarantine[proxy].strikes += 1
                self.ctx.five_hundred_errors +=1
                if target_ctx.quarantine[proxy].strikes >= self.config.max_quarantine_strikes:
                    self.logger.warning(f'Proxy has exceeded maximum strikes.  Placing it into quarantine for {target_ctx.quarantine[proxy].next_duration} seconds.')
                    target_ctx.quarantine[proxy].until = time.time() + target_ctx.quarantine[proxy].next_duration
                    target_ctx.quarantine[proxy].next_duration = target_ctx.quarantine[proxy].next_duration*2
                    target_ctx.quarantine[proxy].strikes = 0
                    
                if retries < self.config.max_retries:
                    payload["retries"] = retries + 1
                    self.ctx.retries += 1
                    target_ctx.queue.appendleft((payload, result_future))
                else:
                    self.logger.warning(f'Request exceeded maximum retries.  Passing along 500 error.')
                    sys.stdout.flush()
                    result_future.set_result(web.json_response(result, status=status))
            else:
                target_ctx.quarantine[proxy].strikes = 0
                target_ctx.quarantine[proxy].next_duration = max(target_ctx.quarantine[proxy].next_duration/2,2)
                result_future.set_result(web.json_response(result))
        except Exception as e:
            try:
                target_ctx = self.ctx[target_url] # Set target_ctx in case it was not set before the earlier exception occurred
                target_ctx.last_used[proxy] = time.time()
                target_ctx.in_use_proxies.discard(proxy)
                result_future.set_result(web.json_response({"error": str(e)}))
                self.logger.error(e)
            except Exception as internal_e:
                print("Error while handling exception:", internal_e)
                sys.stdout.flush()
                result_future.set_result(web.json_response({"error": str(e), "internal": str(internal_e)}))
                self.logger.error(e)
                self.logger.error(internal_e)