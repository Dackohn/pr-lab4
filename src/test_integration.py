# tests/test_integration.py
import time
import requests

LEADER = "http://localhost:8080"
FOLLOWERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
    "http://localhost:8084",
    "http://localhost:8085",
]


def wait_until_healthy(url, timeout=30.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{url}/health", timeout=2.0)
            if r.status_code == 200 and r.json().get("ok"):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"{url} not healthy in time")


def test_write_replication_basic():
    wait_until_healthy(LEADER)

    key = f"test-key-{time.time()}"
    value = "hello-world"

    # write to leader
    r = requests.put(f"{LEADER}/kv/{key}", json={"value": value}, timeout=5.0)
    assert r.status_code == 200, r.text

    # read from leader
    r = requests.get(f"{LEADER}/kv/{key}", timeout=2.0)
    assert r.status_code == 200
    assert r.json()["value"] == value

    # eventually, followers should have it (they might take some ms)
    for follower in FOLLOWERS:
        wait_until_healthy(follower)
        r = requests.get(f"{follower}/kv/{key}", timeout=5.0)
        assert r.status_code == 200
        assert r.json()["value"] == value
