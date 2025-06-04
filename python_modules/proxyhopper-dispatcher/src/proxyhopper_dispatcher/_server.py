from collections import defaultdict, deque
import os
import sys
from typing import Dict, Deque, Generator, List, Set, Tuple, Any
from pydantic import BaseModel, Field, PrivateAttr
from ._config import BaseUrlConfig
import time
import asyncio
import time
import logging
from aiohttp import web, ClientSession, ClientTimeout
from ._config import ProxyhopperConfig, BaseUrlConfig


class QuarantineEntry(BaseModel):
    """
    Data class olding data for a quarantined proxy
    """
    until: float = 0
    strikes: int = 0
    permanent: bool = False

class DispatcherUrlContext(BaseModel):
    queue: Deque[Tuple[dict, Any]] = Field(default_factory=deque)
    last_used: Dict[str, float] = Field(default_factory=lambda: defaultdict(float))
    quarantine: Dict[str, QuarantineEntry] = Field(default_factory=lambda: defaultdict(QuarantineEntry))
    in_use_proxies: Set[str] = Field(default_factory=set)

class DispatcherContext(BaseModel):
    """
    Context providing data for dispatcher server
    """
    proxies: List[str]
    base_urls: List[str]
    _url_contexts:Dict[str, DispatcherUrlContext] = PrivateAttr()
    

    def model_post_init(self, context):
        self._url_contexts = {base_url:DispatcherUrlContext() for base_url in self.base_urls}
    
    def __getitem__(self, indices) -> DispatcherUrlContext:
        if isinstance(indices, str):
            return self._url_contexts[indices]
        if isinstance(indices, tuple):
            return {key:self._url_contexts[key] for key in indices}

    def iter_url_contexts(self) -> Generator[Tuple[str, DispatcherUrlContext], None, None]:
        for base_url, ctx in self._url_contexts.items():
            yield (base_url, ctx)

class DispatcherServer:
    def __init__(self, config: ProxyhopperConfig, log_level='INFO', log_output='stdout', **kwargs):
        self.config = config
        self.ctx = DispatcherContext(
            proxies=config.proxies,
            base_urls=[key for key in config.base_urls]
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

    def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/dispatch", self.dispatch_request)
        app.on_startup.append(self._start_background_tasks)
        app.on_cleanup.append(self._cleanup_background_tasks)
        app.on_shutdown.append(lambda app: asyncio.create_task(self.shutdown()))
        self.logger.info('Created app! Awaiting dispatch requests.')
        return app

    async def dispatch_request(self, request: web.Request) -> web.Response:
        payload = await request.json() # Grab requests json
        payload.setdefault("retries", 0) # Populate a retries value
        payload.setdefault("queue_time", time.time())
        base_url = payload["base_url"]

        if base_url not in self.config.base_urls:
            return web.json({'error':f'Base url: "{base_url}" has not been configured for dispatch server.  Please add it to the config.' } ,status=400)        

        url_ctx = self.ctx[base_url]

        url_ctx.queue.append((payload, asyncio.get_event_loop().create_future()))
        self.logger.debug(f'Received request to: {base_url}.  Queue now contains {len(url_ctx.queue)} request(s)!')
        return await url_ctx.queue[-1][1]

    async def _start_background_tasks(self, app):
        app["dispatcher"] = asyncio.create_task(self._request_dispatcher())

    async def _cleanup_background_tasks(self, app):
        app["dispatcher"].cancel()
        await app["dispatcher"]
    
    def get_next_proxy(self, url_ctx: DispatcherUrlContext, min_interval: float):
        eligible_proxies = [
            p for p in self.ctx.proxies
            if not url_ctx.quarantine[p].permanent
            and time.time() > url_ctx.quarantine[p].until
            and time.time() - url_ctx.last_used[p] >= min_interval
            and p not in url_ctx.in_use_proxies
        ]
        if not eligible_proxies:
            return None
        return min(eligible_proxies, key=lambda p: url_ctx.last_used[p])
    
    async def shutdown(self):
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def _request_dispatcher(self):
        while True:
            for base_url, url_ctx in self.ctx.iter_url_contexts():
                if not url_ctx.queue:
                    continue
                payload, result_future = url_ctx.queue[0]
                min_interval = self.config.base_urls.get(base_url, BaseUrlConfig()).min_request_interval
                proxy = self.get_next_proxy(url_ctx, min_interval)
                if proxy:
                    self.logger.debug(f'Popped request from queue!  Time spent in queue: {time.time() - payload["queue_time"]:.2f}s')
                    url_ctx.queue.popleft()
                    url_ctx.in_use_proxies.add(proxy)
                    task = asyncio.create_task(self._handle_request(payload, result_future, base_url, proxy))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                elif not any(
                    not url_ctx.quarantine[p].permanent and time.time() > url_ctx.quarantine[p].until
                    for p in self.ctx.proxies
                ):
                    url_ctx.queue.popleft()
                    result_future.set_result(web.json_response({"error": "All proxies are currently in quarantine"}, status=429))
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError as e:
                return


    async def _handle_request(self, payload, result_future, base_url, proxy):
        try:
            endpoint = payload["endpoint"]
            method = payload["method"]
            params = payload.get("params", {})
            headers = payload.get("headers", {})
            body = payload.get("body")
            timeout_seconds = payload.get("timeout_seconds")
            retries = payload.get("retries", 0)

            full_url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            proxy_url = f"http://{proxy}"
            if self._proxies_are_fake:
                proxy_url = None

            self.logger.debug(f'Sending request to: {full_url} using proxy: {proxy}.  Params: {params}.  Body: {body}')
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

            url_ctx = self.ctx[base_url]
            
            url_ctx.last_used[proxy] = time.time()
            url_ctx.in_use_proxies.discard(proxy)
            if 500 <= status < 600:
                url_ctx.quarantine[proxy].strikes += 1
                if url_ctx.quarantine[proxy].strikes >= self.config.max_quarantine_strikes:
                    url_ctx.quarantine[proxy].permanent = True
                else:
                    url_ctx.quarantine[proxy].until = time.time() + self.config.quarantine_time
                if retries < self.config.max_retries:
                    payload["retries"] = retries + 1
                    url_ctx.queue.appendleft((payload, result_future))
                else:
                    print('Ending here')
                    sys.stdout.flush()
                    result_future.set_result(web.json_response({"error": str(e)}))
            else:
                url_ctx.quarantine[proxy].strikes = 0
                result_future.set_result(web.json_response(result))
        except Exception as e:
            try:
                url_ctx = self.ctx[base_url] # Set url_ctx in case it was not set before the earlier exception occurred
                url_ctx.last_used[proxy] = time.time()
                url_ctx.in_use_proxies.discard(proxy)
                result_future.set_result(web.json_response({"error": str(e)}))
            except Exception as internal_e:
                print("Error while handling exception:", internal_e)
                sys.stdout.flush()
                result_future.set_result(web.json_response({"error": str(e), "internal": str(internal_e)}))