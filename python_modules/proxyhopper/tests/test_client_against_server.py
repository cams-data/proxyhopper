pytest_plugins = ["pytest_asyncio"]

import pytest
import asyncio
from aiohttp import web
from proxyhopper import Client
from proxyhopper_dispatcher import DispatcherServer
from proxyhopper_dispatcher import ProxyhopperConfig, TargetUrlConfig
import tempfile


@pytest.fixture
async def test_server(aiohttp_server):
    # Define a simple echo endpoint for testing
    async def echo_handler(request):
        data = await request.json()
        return web.json_response({"received": data})

    app = web.Application()
    app.router.add_post("/echo", echo_handler)
    return await aiohttp_server(app)


@pytest.fixture
async def dispatcher_server(aiohttp_server, test_server):
    # Temporary proxy list — test server acts like a proxy target here
    config = ProxyhopperConfig(
        proxies=[f"localhost:{test_server.port}"],
        target_urls={
            f"http://localhost:{test_server.port}": TargetUrlConfig(
                min_request_interval=0.0
            )
        },
        max_retries=1,
        max_quarantine_strikes=1,
        quarantine_time=1.0
    )
    dispatcher = DispatcherServer(config, log_level='DEBUG')
    app = dispatcher.create_app()
    return await aiohttp_server(app)


@pytest.mark.asyncio
async def test_do_batch_request(dispatcher_server, test_server):
    dispatcher_url = f"http://localhost:{dispatcher_server.port}"
    client = Client(dispatcher_url, concurrency=2)

    test_data = {i :{"value": i} for i in range(5)}

    def params_builder(item):
        return {}

    def body_builder(item):
        return {"echo": item["value"]}

    def response_handler(response, data):
        return response.get("received", {})

    results = await client.send_batched_requests(
        target_url=f"http://localhost:{test_server.port}",
        endpoint="/echo",
        param_factory=params_builder,
        headers={"Content-Type": "application/json"},
        data=test_data,
        method="POST",
        body_factory=body_builder,
        response_handler=response_handler,
        on_failure='ignore'
    )

    assert isinstance(results, dict)
    assert len(results) == 5
    for key in results:
        assert results[key]["echo"] == test_data[key]["value"]
