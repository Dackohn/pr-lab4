# test_app.py
import pytest
import os
from typing import Dict
from fastapi import FastAPI
from httpx import AsyncClient
from asgi_lifespan import LifespanManager

#
# IMPORTANT:
# We import the app.py exactly ONCE per test session.
# But because app.py loads ROLE and FOLLOWERS at import-time,
# we must patch the environment BEFORE importing it.
#

def create_app_with_env(role: str, followers: str = "", quorum: int = 1):
    """
    Helper: reload app.py with fresh environment variables.
    This gives us a clean leader or follower instance for each test.
    """
    os.environ["ROLE"] = role
    os.environ["FOLLOWERS"] = followers
    os.environ["WRITE_QUORUM"] = str(quorum)
    os.environ["MIN_DELAY_MS"] = "0"
    os.environ["MAX_DELAY_MS"] = "0"
    os.environ["REPLICATION_TIMEOUT_S"] = "1.0"
    os.environ["GLOBAL_REQUEST_TIMEOUT_S"] = "1.0"

    # Force re-import to reload globals
    import importlib
    import app
    importlib.reload(app)
    return app.app, app.store


@pytest.mark.asyncio
async def test_health_leader():
    app, store = create_app_with_env("leader", followers="")
    store.clear()

    async with LifespanManager(app), AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["role"] == "leader"
        assert body["ok"] is True


@pytest.mark.asyncio
async def test_health_follower():
    app, store = create_app_with_env("follower")
    store.clear()

    async with LifespanManager(app), AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/health")
        assert r.status_code == 200
        assert r.json()["role"] == "follower"


@pytest.mark.asyncio
async def test_put_and_get_leader():
    app, store = create_app_with_env("leader", followers="")
    store.clear()

    async with LifespanManager(app), AsyncClient(app=app, base_url="http://test") as client:
        # Write
        r = await client.put("/kv/foo", json={"value": "bar"})
        assert r.status_code == 200

        # Read
        r = await client.get("/kv/foo")
        assert r.status_code == 200
        assert r.json()["value"] == "bar"


@pytest.mark.asyncio
async def test_put_fails_on_follower():
    app, store = create_app_with_env("follower")
    store.clear()

    async with LifespanManager(app), AsyncClient(app=app, base_url="http://test") as client:
        r = await client.put("/kv/key", json={"value": "x"})
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_replication_works_to_follower():
    """
    Simulates:
    - leader writes
    - leader calls follower /replicate
    """
    # follower app
    follower_app, follower_store = create_app_with_env("follower")
    follower_store.clear()

    # leader app with follower URL pointed at the *follower_app*
    # We use httpx' ability to mount two ASGI apps together
    leader_app, leader_store = create_app_with_env(
        "leader",
        followers="http://testfollower",
        quorum=1
    )
    leader_store.clear()

    # Combine both apps in one test client using mounts
    class MultiApp:
        def __init__(self, apps):
            self.apps = apps

        async def __call__(self, scope, receive, send):
            base = scope["path"].startswith("/replicate")
            if base:
                await self.apps["follower"](scope, receive, send)
            else:
                await self.apps["leader"](scope, receive, send)

    multi = MultiApp({"leader": leader_app, "follower": follower_app})

    async with LifespanManager(leader_app), \
               LifespanManager(follower_app), \
               AsyncClient(app=multi, base_url="http://test") as client:

        # Write to leader
        r = await client.put("/kv/key1", json={"value": "hello"})
        assert r.status_code == 200

        # Check follower store
        r2 = await client.get("/replicate/key1")  # incorrect path check: we must inspect store directly
        # Instead directly check follower_store:
        assert follower_store.get("key1") == "hello"


@pytest.mark.asyncio
async def test_quorum_failure():
    """
    Leader has 2 followers but quorum=2.
    One follower always fails (invalid URL) -> quorum cannot be reached.
    """
    # follower that works
    follower_app, follower_store = create_app_with_env("follower")
    follower_store.clear()

    # leader configured with:
    # - one real follower
    # - one fake follower (will timeout)
    leader_app, leader_store = create_app_with_env(
        "leader",
        followers="http://testfollower,http://badfollower",
        quorum=2
    )
    leader_store.clear()

    class MultiApp:
        def __init__(self, apps):
            self.apps = apps

        async def __call__(self, scope, receive, send):
            if "testfollower" in scope["headers"][0][1].decode():
                await self.apps["follower"](scope, receive, send)
            else:
                # fake follower -> simulate timeout / no response
                # do nothing (connection will hang)
                await asyncio.sleep(2)
                await send({
                    "type": "http.response.start",
                    "status": 500,
                    "headers": []
                })
                await send({
                    "type": "http.response.body",
                    "body": b""
                })

    multi = MultiApp({"leader": leader_app, "follower": follower_app})

    async with LifespanManager(leader_app), \
               LifespanManager(follower_app), \
               AsyncClient(app=multi, base_url="http://test") as client:
        
        r = await client.put("/kv/zzz", json={"value": "qqq"})
        assert r.status_code == 500
        assert "quorum" in r.text.lower()
