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
        max_retries=1,
        target_urls={
            "https://httpbin.org": TargetUrlConfig(min_request_interval=0)
        }
    )

@pytest_asyncio.fixture
async def test_client(proxyhopper_config):
    server = DispatcherServer(config=proxyhopper_config, log_level='INFO', proxies_are_fake=True)
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
    assert resp.content_type == 'application/json'
    assert "url" in json

@pytest.mark.asyncio
async def test_health_check(test_client):
    resp = await test_client.get("/health-check")
    assert resp.status == 200

@pytest.mark.asyncio
async def test_targets(test_client):
    resp = await test_client.get("/targets")
    assert resp.status == 200
    assert resp.content_type == 'application/json'
    json = await resp.json()
    assert [x for x in json][0] == 'https://httpbin.org'

@pytest.mark.asyncio
async def test_quarantine_on_500(test_client):
    payload = {
        "target_url": "https://httpbin.org",
        "endpoint": "status/500",
        "method": "GET",
        "params": {},
        "headers": {}
    }

    # First 500 response
    resp1 = await test_client.post("/dispatch", json=payload)
    assert resp1.status == 500

    # Wait for dispatcher to process and update state
    await asyncio.sleep(1.2)

    # Second 500 response
    resp2 = await test_client.post("/dispatch", json=payload)
    assert resp2.status == 500

    # Ensure proxy will have 8 second next duration
    ctx = test_client.server.app["dispatcher"].get_coro().cr_frame.f_locals["self"].ctx
    proxy_status = ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"]
    assert proxy_status.next_duration == 8

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
    await resp.text()

    # Check temporary quarantine
    ctx = test_client.server.app["dispatcher"].get_coro().cr_frame.f_locals["self"].ctx
    await asyncio.sleep(0.2)
    assert ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"].next_duration == 4

    # Make correct request
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

    # Check temporary quarantine
    ctx = test_client.server.app["dispatcher"].get_coro().cr_frame.f_locals["self"].ctx
    await asyncio.sleep(0.2)
    assert ctx["https://httpbin.org"].quarantine["123.123.123.123:8800"].next_duration == 2