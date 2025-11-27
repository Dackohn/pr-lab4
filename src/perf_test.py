# perf_experiment.py
import asyncio
import os
import subprocess
import sys
import time
from statistics import mean

import httpx
import numpy as np
import matplotlib.pyplot as plt

LEADER = "http://localhost:8080"
FOLLOWERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
    "http://localhost:8084",
    "http://localhost:8085",
]

QUORUM_VALUES = [1, 2, 3, 4, 5]  # test these write quorum values


# ---------- Docker orchestration helpers ----------

def docker_compose_up(write_quorum: int):
    env = os.environ.copy()
    env["WRITE_QUORUM"] = str(write_quorum)
    print(f"\n=== Starting cluster with WRITE_QUORUM={write_quorum} ===")
    subprocess.run(["docker-compose", "down", "-v"], env=env,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    result = subprocess.run(
        ["docker-compose", "up", "-d", "--build"],
        env=env,
    )
    if result.returncode != 0:
        print("docker-compose up failed", file=sys.stderr)
        sys.exit(1)


def docker_compose_down():
    env = os.environ.copy()
    subprocess.run(["docker-compose", "down", "-v"], env=env)


def wait_until_healthy(url: str, timeout: float = 60.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(f"{url}/health", timeout=2.0)
            if r.status_code == 200 and r.json().get("ok"):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Service {url} not healthy within {timeout} seconds")


def wait_for_cluster_ready():
    print("Waiting for leader + followers to become healthy...")
    wait_until_healthy(LEADER)
    for f in FOLLOWERS:
        wait_until_healthy(f)
    print("Cluster is healthy.")


# ---------- Load generation ----------

async def write_key(client: httpx.AsyncClient, key: str, value: str) -> float:
    start = time.perf_counter()
    r = await client.put(f"{LEADER}/kv/{key}", json={"value": value})
    latency = time.perf_counter() - start
    r.raise_for_status()
    return latency


async def run_load(num_writes=100, concurrent=10):
    """
    num_writes total writes, at most `concurrent` in flight.
    keys: 10 distinct keys (0..9).
    """
    latencies = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        sem = asyncio.Semaphore(concurrent)

        async def worker(i: int):
            async with sem:
                key = f"k{i % 10}"
                value = f"v-{i}"
                return await write_key(client, key, value)

        tasks = [asyncio.create_task(worker(i)) for i in range(num_writes)]
        for t in asyncio.as_completed(tasks):
            latencies.append(await t)
    return latencies


def fetch_store(url: str):
    r = httpx.get(f"{url}/debug/store", timeout=10.0)
    r.raise_for_status()
    return r.json()["store"]


# ---------- Plotting (new full version) ----------

def plot_latency_stats(quorums, all_latencies):
    """
    quorums: list of quorum values tested, e.g. [1,2,3,4,5]
    all_latencies: list of lists of latencies per quorum
    """

    means, medians, p95s, p99s = [], [], [], []

    for lats in all_latencies:
        arr = np.array(lats)
        means.append(arr.mean())
        medians.append(np.median(arr))
        p95s.append(np.percentile(arr, 95))
        p99s.append(np.percentile(arr, 99))

    plt.figure(figsize=(10, 6))

    plt.plot(quorums, means, marker='o', label="mean")
    plt.plot(quorums, medians, marker='o', label="median")
    plt.plot(quorums, p95s, marker='o', label="p95")
    plt.plot(quorums, p99s, marker='o', label="p99")

    plt.title("Quorum vs Latency (mean, median, p95, p99)\nRandom delay in range [0ms, 1000ms]")
    plt.xlabel("Quorum value")
    plt.ylabel("Latency (s)")
    plt.grid(True, alpha=0.3)

    plt.xticks(quorums, [f"Q={q}" for q in quorums])

    plt.legend()
    plt.tight_layout()
    plt.savefig("quorum_vs_latency_full.png")
    plt.show()
    print("Saved plot as quorum_vs_latency_full.png")


# ---------- Main experiment ----------

def main():
    all_latency_lists = []
    consistency_results = {}

    for q in QUORUM_VALUES:
        docker_compose_up(q)
        try:
            wait_for_cluster_ready()

            print(f"Running load test for WRITE_QUORUM={q}...")
            latencies = asyncio.run(run_load(num_writes=100, concurrent=10))
            all_latency_lists.append(latencies)

            avg_lat = mean(latencies)
            print(f"WRITE_QUORUM={q}: "
                  f"avg={avg_lat:.4f}s, "
                  f"min={min(latencies):.4f}s, "
                  f"max={max(latencies):.4f}s")

            # Consistency check
            leader_store = fetch_store(LEADER)
            follower_stores = [fetch_store(u) for u in FOLLOWERS]

            follower_equalities = [
                (i + 1, fs == leader_store) for i, fs in enumerate(follower_stores)
            ]
            consistency_results[q] = follower_equalities

            for i, eq in follower_equalities:
                print(f"  Follower {i} store == leader: {eq}")

        finally:
            docker_compose_down()

    # Plot everything together
    plot_latency_stats(QUORUM_VALUES, all_latency_lists)

    # Print summary for report
    print("\n=== SUMMARY ===")
    for q, lats in zip(QUORUM_VALUES, all_latency_lists):
        print(f"WRITE_QUORUM={q}: avg latency={mean(lats):.4f}s")
        for follower_idx, eq in consistency_results[q]:
            print(f"  follower {follower_idx} equal to leader: {eq}")


if __name__ == "__main__":
    main()
