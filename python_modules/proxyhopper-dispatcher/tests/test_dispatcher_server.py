import asyncio
import time
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, loop_context
import pytest_asyncio
from proxyhopper_dispatcher import DispatcherServer
from proxyhopper_dispatcher import ProxyhopperConfig, TargetUrlConfig

@pytest.fixture
def proxyhopper_config():
    return ProxyhopperConfig(
        proxies=[
    '123.123.123.123:8800', # Fake proxy
],
        quarantine_time=1,
        max_quarantine_strikes=2,
        target_urls={
            "https://httpbin.org": TargetUrlConfig(min_request_interval=0)
        }
    )

@pytest_asyncio.fixture
async def test_client(proxyhopper_config):
    server = DispatcherServer(config=proxyhopper_config, log_level='DEBUG', proxies_are_fake=True)
    app = server.create_app()
    async with TestServer(app) as test_server:
        async with TestClient(test_server) as client:
            yield client

@pytest.mark.asyncio
async def test_dispatch_success(test_client):
    payload = {
        "target_url": "https://httpbin.org",
        "endpoint": "get",
        "method": "GET",
        "params": {},
        "headers": {},
    }
    resp = await test_client.post("/dispatch", json=payload)
    json = await resp.json()
    assert resp.status == 200
    assert "url" in json

@pytest.mark.asyncio
async def test_quarantine_on_500(test_client):
    payload = {
        "target_url": "https://httpbin.org",
        "endpoint": "status/500",
        "method": "GET",
        "params": {},
        "headers": {},
    }

    # First 500 response → 1 strike
    resp1 = await test_client.post("/dispatch", json=payload)
    res1 = await resp1.json()
    assert resp1.status == 429
    assert res1.get("error") is None or isinstance(res1, dict)

    # Wait for dispatcher to process and update state
    await asyncio.sleep(1.2)

    # Second 500 response → permanently quarantined
    resp2 = await test_client.post("/dispatch", json=payload)
    res2 = await resp2.json()
    assert resp2.status == 429

    # Ensure proxy is now marked as permanent for that target_url
    ctx = test_client.server.app["dispatcher"].get_coro().cr_frame.f_locals["self"].ctx
    proxy_status = ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"]
    assert proxy_status.strikes >= 2
    assert proxy_status.permanent is True

@pytest.mark.asyncio
async def test_recover_from_quarantine(test_client, proxyhopper_config):
    payload = {
        "target_url": "https://httpbin.org",
        "endpoint": "status/500",
        "method": "GET",
        "params": {},
        "headers": {},
    }

    # Cause one strike (not permanent)
    resp = await test_client.post("/dispatch", json=payload)
    await resp.json()

    # Check temporary quarantine
    ctx = test_client.server.app["dispatcher"].get_coro().cr_frame.f_locals["self"].ctx
    await asyncio.sleep(0.2)
    assert ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"].until > 0

    # Wait for quarantine to end
    await asyncio.sleep(proxyhopper_config.quarantine_time + 0.1)
    assert ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"].until <= time.time()