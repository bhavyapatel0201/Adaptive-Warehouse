"""Experiment harness: runs one or more configurations end-to-end.

A single "run" = one config (C1..C12) executed in one mode ('static' or
'adaptive'):

    1. open an experiment_run row
    2. reset the buffer cache so results aren't distorted by warm caches
    3. start the ETL injector thread at the configured rate
    4. drive the analytical workload, logging every query to query_sample
    5. stop ETL, close out the run

The cache-flush step addresses the "cache pollution" experimental risk from
the proposal. We cannot drop the OS page cache from inside a container, so we
do what is possible from SQL (DISCARD ALL + a large unrelated scan to evict
warm pages). This is documented honestly rather than overstated.

Examples
--------
Single config, static mode, 30 s:
    python -m src.harness.run_experiment --config C3 --mode static --seconds 30

Whole matrix (all 12 configs), static mode:
    python -m src.harness.run_experiment --all --mode static --seconds 30

Repeat each config 3x for confidence intervals:
    python -m src.harness.run_experiment --all --mode static --seconds 30 --repeat 3
"""
import argparse
import threading
import time

from src.config import EXPERIMENT_MATRIX
from src.db import connect
from src.etl.injector import ETLInjector
from src.workload.generator import run_workload


def _find_config(config_id: str):
    for c in EXPERIMENT_MATRIX:
        if c["config_id"].upper() == config_id.upper():
            return c
    raise ValueError(
        f"Unknown config '{config_id}'. Expected C1..C{len(EXPERIMENT_MATRIX)}."
    )


def flush_caches(verbose: bool = True):
    """Best-effort cache reset between runs.

    DISCARD ALL clears session state and cached plans. Evicting the shared
    buffer pool properly requires pg_prewarm/OS access, so we additionally
    scan a large unrelated relation to push hot pages out. Not a perfect
    cold cache -- but consistent across every run, which is what matters for
    a fair static-vs-adaptive comparison.
    """
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DISCARD ALL;")
            # Touch a broad slice of the fact table to displace warm pages.
            cur.execute("SELECT count(*) FROM fact_sales;")
    except Exception as exc:
        if verbose:
            print(f"  [cache flush warning] {exc}")
    finally:
        conn.close()


def start_run(config, mode, notes=None):
    """Insert an experiment_run row and return its run_id."""
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO experiment_run
                   (config_id, mode, workload, etl_rate, notes)
                   VALUES (%s,%s,%s,%s,%s) RETURNING run_id""",
                (config["config_id"], mode, config["workload"],
                 config["etl_rate"], notes),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def finish_run(run_id):
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE experiment_run SET finished_at = now() WHERE run_id = %s",
                (run_id,),
            )
    finally:
        conn.close()


def run_config(config, mode: str, seconds: float, verbose: bool = True):
    """Execute one configuration end-to-end. Returns (run_id, samples)."""
    label = (f"{config['config_id']} [{mode}] "
             f"workload={config['workload']} etl={config['etl_rate']}")
    if verbose:
        print(f"\n=== {label} ===")

    # Every run must start from the SAME physical design as the static
    # baseline, otherwise an adaptive run would inherit indexes built by a
    # previous run and the comparison would be invalid.
    try:
        from src.adaptive.engine import reset_physical_design
        reset_physical_design(verbose=verbose)
    except ImportError:
        pass

    flush_caches(verbose=verbose)
    run_id = start_run(config, mode)

    stop = threading.Event()
    injector = ETLInjector(config["etl_rate"], stop, verbose=verbose)
    injector.start()

    adaptive_engine = None
    if mode == "adaptive":
        # Phase 3 plugs in here. Imported lazily so Phase 2 runs standalone.
        try:
            from src.adaptive.engine import AdaptiveEngine
            adaptive_engine = AdaptiveEngine(run_id, stop, verbose=verbose)
            adaptive_engine.start()
        except ImportError:
            print("  [warn] adaptive engine not implemented yet (Phase 3); "
                  "running without adaptation.")

    try:
        samples = run_workload(config["workload"], seconds,
                               run_id=run_id, stop_event=stop, verbose=verbose)
    finally:
        stop.set()
        injector.join(timeout=15)
        if adaptive_engine is not None:
            adaptive_engine.join(timeout=15)
        finish_run(run_id)

    return run_id, samples


def main():
    ap = argparse.ArgumentParser(description="Run warehouse experiments.")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--config", help="single config id, e.g. C3")
    group.add_argument("--all", action="store_true", help="run all 12 configs")
    ap.add_argument("--mode", default="static", choices=["static", "adaptive"])
    ap.add_argument("--seconds", type=float, default=30,
                    help="workload duration per run (default 30)")
    ap.add_argument("--repeat", type=int, default=1,
                    help="repetitions per config, for confidence intervals")
    args = ap.parse_args()

    configs = EXPERIMENT_MATRIX if args.all else [_find_config(args.config)]

    started = time.time()
    total = len(configs) * args.repeat
    done = 0
    for rep in range(args.repeat):
        for cfg in configs:
            if args.repeat > 1:
                print(f"\n--- repetition {rep + 1}/{args.repeat} ---")
            run_config(cfg, args.mode, args.seconds)
            done += 1
            print(f"  progress: {done}/{total} runs complete")

    mins = (time.time() - started) / 60
    print(f"\nAll runs finished in {mins:.1f} min. "
          f"Query the experiment_run / query_sample tables for results.")


if __name__ == "__main__":
    main()
