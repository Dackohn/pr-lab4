import os
import random
import json
import asyncio
from typing import Dict, List

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# -------------------------
# Environment configuration
# -------------------------
ROLE = os.getenv("ROLE", "leader")
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "3"))
MIN_DELAY_MS = int(os.getenv("MIN_DELAY_MS", "0"))
MAX_DELAY_MS = int(os.getenv("MAX_DELAY_MS", "1000"))
REPLICATION_TIMEOUT_S = float(os.getenv("REPLICATION_TIMEOUT_S", "2.0"))
GLOBAL_REQUEST_TIMEOUT_S = float(os.getenv("GLOBAL_REQUEST_TIMEOUT_S", "5.0"))

followers_env = os.getenv("FOLLOWERS", "")
FOLLOWER_URLS: List[str] = [x.strip() for x in followers_env.split(",") if x.strip()] if ROLE == "leader" else []

# Shared in-memory KV store
store: Dict[str, str] = {}

# -------------------------
# Persistent storage paths
# -------------------------
DATA_DIR = "/data"
NODE_FILE = os.path.join(DATA_DIR, f"{ROLE}_store.json")

def save_store_to_disk():
    """Persist the current store to /data/<role>_store.json."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(NODE_FILE, "w") as f:
            json.dump(store, f, indent=2)
    except Exception as e:
        print(f"[{ROLE}] Error saving store: {e}")

# -------------------------
# A single global HTTPX client
# (not killed when requests handler ends)
# -------------------------
CLIENT = httpx.AsyncClient(timeout=REPLICATION_TIMEOUT_S)

# -------------------------
# API setup
# -------------------------
app = FastAPI()


class ValuePayload(BaseModel):
    value: str


@app.get("/health")
async def health():
    return {
        "ok": True,
        "role": ROLE,
        "write_quorum": WRITE_QUORUM,
        "followers": FOLLOWER_URLS,
    }


@app.get("/kv/{key}")
async def get_key(key: str):
    if key not in store:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": store[key]}


# ----------------------------------------------------
# FIXED REPLICATION LOGIC — Full implementation
# ----------------------------------------------------
@app.put("/kv/{key}")
async def put_key(key: str, payload: ValuePayload):
    if ROLE != "leader":
        raise HTTPException(status_code=400, detail="Writes allowed only on leader")

    # 1. Write locally
    store[key] = payload.value
    save_store_to_disk()

    follower_count = len(FOLLOWER_URLS)
    quorum = min(WRITE_QUORUM, follower_count)

    # No followers -> nothing to replicate
    if follower_count == 0 or quorum <= 0:
        return {"status": "ok", "replicated_to": 0}

    async def replicate_to_follower(url: str) -> bool:
        # simulate network lag
        await asyncio.sleep(random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000.0)
        try:
            r = await CLIENT.post(
                f"{url}/replicate/{key}",
                json={"value": payload.value},
            )
            r.raise_for_status()
            return True
        except Exception:
            return False

    # Fire off tasks for all followers
    tasks = [asyncio.create_task(replicate_to_follower(u)) for u in FOLLOWER_URLS]

    # -------------------------
    # FULL SYNC CASE (quorum == follower_count)
    # -------------------------
    if quorum == follower_count:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = sum(1 for r in results if isinstance(r, bool) and r)

        if successes < quorum:
            raise HTTPException(
                status_code=500,
                detail=f"Full replication failed (success={successes}, required={quorum})",
            )
        return {"status": "ok", "replicated_to": successes}

    # -------------------------
    # SEMI-SYNC CASE (quorum < follower_count)
    # -------------------------
    successes = 0
    pending = set(tasks)
    start_time = asyncio.get_event_loop().time()

    while pending and successes < quorum:
        time_left = GLOBAL_REQUEST_TIMEOUT_S - (asyncio.get_event_loop().time() - start_time)
        if time_left <= 0:
            break

        done, pending = await asyncio.wait(
            pending,
            timeout=time_left,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not done:
            break

        for t in done:
            try:
                ok = await t
            except Exception:
                ok = False

            if ok:
                successes += 1
                if successes >= quorum:
                    break

    if successes < quorum:
        raise HTTPException(
            status_code=500,
            detail=f"Replication quorum not reached (success={successes}, required={quorum})",
        )

    # background tasks keep running using global CLIENT → improves eventual consistency
    return {"status": "ok", "replicated_to": successes}


# ----------------------------------------------------
# FOLLOWER ENDPOINT
# ----------------------------------------------------
@app.post("/replicate/{key}")
async def replicate_key(key: str, payload: ValuePayload):
    store[key] = payload.value
    save_store_to_disk()
    return {"status": "ok"}


@app.get("/debug/store")
async def debug_store():
    return {"role": ROLE, "store": store}
