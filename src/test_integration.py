# integration_test.py
import time
import requests
import pytest

LEADER = "http://localhost:8080"
FOLLOWERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
    "http://localhost:8084",
    "http://localhost:8085",
]

TIMEOUT = 60.0   # seconds to wait for cluster to become healthy


# -----------------------------------------------------------
# Utility: wait until each service responds /health
# -----------------------------------------------------------
def wait_for_health(url):
    start = time.time()
    while time.time() - start < TIMEOUT:
        try:
            r = requests.get(f"{url}/health", timeout=2)
            if r.status_code == 200 and r.json().get("ok"):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Service {url} did not become healthy")


# -----------------------------------------------------------
# TEST 1: Cluster is healthy
# -----------------------------------------------------------
@pytest.mark.integration
def test_cluster_health():
    wait_for_health(LEADER)
    for f in FOLLOWERS:
        wait_for_health(f)


# -----------------------------------------------------------
# TEST 2: Write to leader and read back from leader
# -----------------------------------------------------------
@pytest.mark.integration
def test_write_read_leader():
    wait_for_health(LEADER)

    key = "key_test_1"
    val = "hello_world"

    r = requests.put(f"{LEADER}/kv/{key}", json={"value": val})
    assert r.status_code == 200

    r = requests.get(f"{LEADER}/kv/{key}")
    assert r.status_code == 200
    assert r.json()["value"] == val


# -----------------------------------------------------------
# TEST 3: Replication works on all followers (eventually)
# -----------------------------------------------------------
@pytest.mark.integration
def test_replication_to_followers():
    key = "key_test_2"
    val = "replication_test_value"

    # Write to leader
    r = requests.put(f"{LEADER}/kv/{key}", json={"value": val})
    assert r.status_code == 200

    # Followers may take a moment — retry for ~5 seconds
    for f in FOLLOWERS:
        for _ in range(20):
            r = requests.get(f"{f}/kv/{key}")
            if r.status_code == 200 and r.json()["value"] == val:
                break
            time.sleep(0.25)
        else:
            raise AssertionError(f"Follower {f} did not replicate key {key}")


# -----------------------------------------------------------
# TEST 4: Quorum logic works — write fails if quorum not reached
# (Requires WRITE_QUORUM > number of *working* followers)
# -----------------------------------------------------------
@pytest.mark.integration
def test_quorum_behavior():
    """
    This test assumes WRITE_QUORUM is set higher than 0.
    
    If WRITE_QUORUM is higher than available followers
    (for example, set WRITE_QUORUM=6), the write MUST fail.
    
    If WRITE_QUORUM is valid (1–5) then the write must succeed.
    """

    wait_for_health(LEADER)

    # Read quorum value from leader
    r = requests.get(f"{LEADER}/health")
    assert r.status_code == 200
    quorum = r.json()["write_quorum"]

    key = "key_test_quorum"
    val = "some_value"

    r = requests.put(f"{LEADER}/kv/{key}", json={"value": val})

    if quorum <= 5:  
        # Valid quorum → should succeed
        assert r.status_code == 200
    else:
        # Impossible quorum → must fail
        assert r.status_code != 200


# -----------------------------------------------------------
# TEST 5: Full consistency after several writes
# -----------------------------------------------------------
@pytest.mark.integration
def test_full_consistency():
    # Perform multiple writes
    for i in range(10):
        key = f"k_{i}"
        val = f"v_{i}"
        r = requests.put(f"{LEADER}/kv/{key}", json={"value": val})
        assert r.status_code == 200

    # Compare entire stores
    leader_store = requests.get(f"{LEADER}/debug/store").json()["store"]

    for f in FOLLOWERS:
        follower_store = requests.get(f"{f}/debug/store").json()["store"]
        # Followers may lag, retry for a bit
        start = time.time()
        while time.time() - start < 5:
            if follower_store == leader_store:
                break
            time.sleep(0.2)
            follower_store = requests.get(f"{f}/debug/store").json()["store"]
        assert follower_store == leader_store, f"Follower {f} is inconsistent"
