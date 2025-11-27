# app.py
import os
import random
import asyncio
from typing import Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

# ----- Config from environment -----
ROLE = os.getenv("ROLE", "leader")  # "leader" or "follower"

FOLLOWERS_RAW = os.getenv("FOLLOWERS", "")
FOLLOWER_URLS = [u.strip() for u in FOLLOWERS_RAW.split(",") if u.strip()] if ROLE == "leader" else []

WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "1"))     # 1..5 for 5 followers
MIN_DELAY_MS = int(os.getenv("MIN_DELAY_MS", "0"))
MAX_DELAY_MS = int(os.getenv("MAX_DELAY_MS", "1000"))

REPLICATION_TIMEOUT_S = float(os.getenv("REPLICATION_TIMEOUT_S", "2.0"))
GLOBAL_REQUEST_TIMEOUT_S = float(os.getenv("GLOBAL_REQUEST_TIMEOUT_S", "5.0"))

app = FastAPI(title="KV Store with Single-Leader Replication (Semi-sync)")

# Simple in-memory store
store: Dict[str, str] = {}


class ValuePayload(BaseModel):
    value: str


@app.get("/health")
async def health():
    return {
        "role": ROLE,
        "write_quorum": WRITE_QUORUM,
        "followers": FOLLOWER_URLS,
        "ok": True,
    }


@app.get("/kv/{key}")
async def get_key(key: str):
    if key not in store:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": store[key]}


@app.put("/kv/{key}")
async def put_key(key: str, payload: ValuePayload):
    """
    Writes are accepted only by the leader.
    Semi-synchronous replication: wait for WRITE_QUORUM followers before responding.
    """
    if ROLE != "leader":
        raise HTTPException(status_code=400, detail="Writes allowed only on leader")

    # 1) Apply locally
    store[key] = payload.value

    # 2) If no followers or quorum <= 0, just return
    if not FOLLOWER_URLS or WRITE_QUORUM <= 0:
        return {"status": "ok", "replicated_to": 0}

    # 3) Replicate concurrently with random delays
    async def replicate_to_follower(follower_url: str) -> bool:
        # simulate network lag
        await asyncio.sleep(random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000.0)
        try:
            async with httpx.AsyncClient(timeout=REPLICATION_TIMEOUT_S) as client:
                r = await client.post(
                    f"{follower_url}/replicate/{key}",
                    json={"value": payload.value},
                )
                r.raise_for_status()
            return True
        except Exception:
            return False

    tasks = [asyncio.create_task(replicate_to_follower(u)) for u in FOLLOWER_URLS]

    successes = 0
    try:
        # Wait until we either:
        # - reach the write quorum, or
        # - all replication tasks finish, or timeout
        async for finished in _as_completed_with_timeout(tasks, GLOBAL_REQUEST_TIMEOUT_S):
            ok = await finished
            if ok:
                successes += 1
                if successes >= WRITE_QUORUM:
                    break
    except asyncio.TimeoutError:
        # timed out waiting; just count what we got so far
        pass

    if successes < WRITE_QUORUM:
        raise HTTPException(
            status_code=500,
            detail=f"Replication quorum not reached (successes={successes}, required={WRITE_QUORUM})",
        )

    return {"status": "ok", "replicated_to": successes}


async def _as_completed_with_timeout(tasks, timeout: float):
    """
    Helper: like asyncio.as_completed but with global timeout.
    """
    done_iter = asyncio.as_completed(tasks, timeout=timeout)
    async for t in _aiter(done_iter):
        yield t


async def _aiter(iterable):
    for item in iterable:
        yield item


@app.post("/replicate/{key}")
async def replicate(key: str, payload: ValuePayload):
    """
    Replication endpoint on followers: leader calls this.
    Followers simply apply the write.
    """
    if ROLE != "follower":
        raise HTTPException(status_code=400, detail="Only followers accept /replicate")
    store[key] = payload.value
    return {"status": "ok"}


@app.get("/debug/store")
async def debug_store():
    """
    For testing / experiment: inspect the whole store.
    DO NOT expose this in a real system.
    """
    return {"role": ROLE, "store": store}
