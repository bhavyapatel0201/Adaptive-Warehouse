"""Run the complete experiment matrix: all 12 configs x both modes.

This is the Phase 4 driver. It exists because getting a *fair* static-vs-
adaptive comparison requires strict discipline between runs, and doing that
by hand across 24+ runs is where mistakes creep in. Before every single run
this script:

  1. drops all engine-created indexes  -> both modes start from the identical
                                          static baseline physical design
  2. trims ETL-injected rows + VACUUMs -> every run sees the same table size,
                                          with no dead-tuple bloat left behind
                                          by the previous run's cleanup

Skipping either step invalidates the comparison. Bloat in particular is
insidious: a large DELETE without VACUUM leaves pages full of dead tuples
that slow the *next* run's sequential scans, which then looks like an
"adaptive improvement" that is really just cleanup.

Usage
-----
Full matrix, 3 repetitions each (recommended for the report):
    python -m src.harness.run_matrix --seconds 30 --repeat 3

Quick smoke test of the whole matrix:
    python -m src.harness.run_matrix --seconds 10 --repeat 1

One workload only:
    python -m src.harness.run_matrix --workload customer --seconds 30 --repeat 3

Estimated wall time = configs x modes x repeat x (seconds + ~20s overhead).
The full 12x2x3 at 30s is roughly 50-60 minutes, so run it when you can
leave the machine alone -- background activity adds measurement noise.
"""
import argparse
import subprocess
import sys
import time

from src.config import ADAPTIVE, EXPERIMENT_MATRIX
from src.harness.run_experiment import run_config


def _reset_between_runs(verbose=True):
    """Restore identical starting conditions before a run."""
    from src.adaptive.engine import reset_physical_design
    reset_physical_design(verbose=False)
    # reset_data is a module with a confirmation prompt; call it non-interactively
    subprocess.run(
        [sys.executable, "-m", "src.harness.reset_data", "--yes"],
        capture_output=not verbose,
        check=False,
    )


def main():
    ap = argparse.ArgumentParser(description="Run the full experiment matrix.")
    ap.add_argument("--seconds", type=float, default=30,
                    help="workload duration per run (default 30)")
    ap.add_argument("--repeat", type=int, default=3,
                    help="repetitions per config per mode (default 3)")
    ap.add_argument("--workload", choices=["product", "customer", "temporal"],
                    help="limit to a single workload (default: all)")
    ap.add_argument("--modes", default="static,adaptive",
                    help="comma-separated modes (default: static,adaptive)")
    args = ap.parse_args()

    configs = EXPERIMENT_MATRIX
    if args.workload:
        configs = [c for c in configs if c["workload"] == args.workload]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    # The engine waits one poll interval before its first decision, then needs
    # time for the new index to actually serve queries. If the run ends around
    # the time the index appears, adaptive mode gets measured mostly in its
    # pre-adaptation state and improvements look far smaller than they are.
    poll = ADAPTIVE["poll_interval_s"]
    if "adaptive" in modes and args.seconds < poll * 3:
        print(f"WARNING: --seconds {args.seconds:.0f} is short relative to the "
              f"adaptive poll interval ({poll}s).")
        print(f"         The engine cannot make its first decision until {poll}s "
              f"in, so most queries")
        print(f"         will run before any index exists and gains will be "
              f"understated.")
        print(f"         Use --seconds {poll * 4:.0f} or more for results worth "
              f"reporting.\n")

    total = len(configs) * len(modes) * args.repeat
    est_min = total * (args.seconds + 20) / 60
    print(f"Matrix: {len(configs)} configs x {len(modes)} modes x "
          f"{args.repeat} reps = {total} runs")
    print(f"Estimated time: ~{est_min:.0f} minutes\n")

    started = time.time()
    done = 0
    for rep in range(args.repeat):
        for cfg in configs:
            for mode in modes:
                _reset_between_runs(verbose=False)
                run_config(cfg, mode, args.seconds)
                done += 1
                elapsed = (time.time() - started) / 60
                remaining = (elapsed / done) * (total - done) if done else 0
                print(f"  progress: {done}/{total} runs "
                      f"({elapsed:.1f} min elapsed, ~{remaining:.0f} min left)")

    # Leave the database in the baseline state.
    _reset_between_runs(verbose=False)

    mins = (time.time() - started) / 60
    print(f"\nMatrix complete in {mins:.1f} min.")
    print("Next steps:")
    print("  python -m src.harness.report")
    print("  python -m analysis.plots")


if __name__ == "__main__":
    main()
