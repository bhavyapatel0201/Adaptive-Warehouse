"""Workload generator: executes analytical queries and records their behaviour.

For every execution we capture, via EXPLAIN (ANALYZE, FORMAT JSON):

    latency_ms   - actual execution time
    planning_ms  - time the planner spent choosing a plan
    used_seqscan - whether the plan fell back to a sequential scan
    plan_hash    - a fingerprint of the plan's operator shape

`plan_hash` is what makes "plan regression" measurable. We record the *shape*
of the plan (node types + the relations they touch) but deliberately ignore
row counts and timings, so the hash only changes when the optimizer genuinely
picks a different strategy -- which is exactly the regression event the
proposal wants to count.

Run standalone (useful for a quick smoke test):
    python -m src.workload.generator --workload product --seconds 20
"""
import argparse
import hashlib
import json
import random
import time

from src.db import connect
from src.workload.queries import get_pool


def _walk_plan(node, parts):
    """Recursively collect (node type, relation) pairs to fingerprint plan shape."""
    parts.append(node.get("Node Type", "?"))
    rel = node.get("Relation Name")
    if rel:
        parts.append(rel)
    idx = node.get("Index Name")
    if idx:
        parts.append(idx)
    for child in node.get("Plans", []) or []:
        _walk_plan(child, parts)


def _plan_fingerprint(plan_json):
    """Stable hash of plan structure (ignores timings and row estimates)."""
    parts = []
    _walk_plan(plan_json["Plan"], parts)
    joined = "|".join(parts)
    return hashlib.sha1(joined.encode()).hexdigest()[:16], joined


def _has_seqscan(shape: str) -> bool:
    """True if the plan contains any sequential scan (on a real table)."""
    return "Seq Scan" in shape


def execute_measured(cur, query):
    """Run one query under EXPLAIN ANALYZE and return its metrics."""
    params = query["params"]()
    sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query["sql"]
    cur.execute(sql, params)
    result = cur.fetchone()[0]
    # psycopg2 may hand back a parsed list or a JSON string depending on version
    if isinstance(result, str):
        result = json.loads(result)
    plan = result[0]

    plan_hash, shape = _plan_fingerprint(plan)
    return {
        "query_name": query["name"],
        "latency_ms": plan.get("Execution Time", 0.0),
        "planning_ms": plan.get("Planning Time", 0.0),
        "used_seqscan": _has_seqscan(shape),
        "plan_hash": plan_hash,
    }


def run_workload(workload: str, seconds: float, run_id=None, stop_event=None,
                 verbose: bool = True):
    """Continuously execute queries from a pool for `seconds`.

    If `run_id` is given, every sample is written to the query_sample table.
    If `stop_event` is given, the loop exits early when it is set.
    Returns the list of collected samples.
    """
    pool = get_pool(workload)
    samples = []
    conn = connect(autocommit=True)
    deadline = time.time() + seconds
    try:
        with conn.cursor() as cur:
            while time.time() < deadline:
                if stop_event is not None and stop_event.is_set():
                    break
                query = random.choice(pool)
                try:
                    m = execute_measured(cur, query)
                except Exception as exc:            # keep the run alive
                    if verbose:
                        print(f"  [query error] {query['name']}: {exc}")
                    continue
                samples.append(m)
                if run_id is not None:
                    cur.execute(
                        """INSERT INTO query_sample
                           (run_id, query_name, latency_ms, planning_ms,
                            used_seqscan, plan_hash)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (run_id, m["query_name"], m["latency_ms"],
                         m["planning_ms"], m["used_seqscan"], m["plan_hash"]),
                    )
    finally:
        conn.close()

    if verbose and samples:
        lat = sorted(s["latency_ms"] for s in samples)
        seq = sum(1 for s in samples if s["used_seqscan"])
        print(f"  workload[{workload}]: {len(samples)} queries, "
              f"median {lat[len(lat)//2]:.1f} ms, "
              f"p95 {lat[int(len(lat)*0.95) - 1]:.1f} ms, "
              f"seqscan {seq}/{len(samples)}")
    return samples


def main():
    ap = argparse.ArgumentParser(description="Run an analytical workload.")
    ap.add_argument("--workload", required=True,
                    choices=["product", "customer", "temporal"])
    ap.add_argument("--seconds", type=float, default=20)
    args = ap.parse_args()
    run_workload(args.workload, args.seconds)


if __name__ == "__main__":
    main()
