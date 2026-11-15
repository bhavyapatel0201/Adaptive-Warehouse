"""The adaptive optimization engine: the closed control loop.

Runs as a background thread alongside the workload and the ETL injector.
On each cycle it:

    OBSERVE  -- read pg_stat_statements and pg_stat_user_tables
    DECIDE   -- rank candidate (table, column) pairs; apply policy thresholds
    ACT      -- build the winning index with guarded, non-blocking DDL
    RECORD   -- log the action (and its cost) to adaptive_action

Design choices worth defending in the report
--------------------------------------------
* **Rule-based, not learned.** The decision layer is an explicit threshold
  policy. It is auditable ("why did it build this index?" has a one-line
  answer), deterministic enough to reproduce, and cannot spend the project's
  remaining weeks failing to converge. Bao-style learned optimization is
  cited as related work, not reimplemented.

* **One index per cycle.** Building several at once would make it impossible
  to attribute a latency change to a specific adaptation, and would spike
  write amplification against the concurrent ETL stream.

* **A cooldown after each build.** Newly built indexes need the workload to
  actually exercise them before the next decision is made; without a cooldown
  the engine reacts to stale telemetry and over-indexes.

* **Bounded index count.** ADAPTIVE["max_indexes"] caps cumulative overhead,
  directly addressing the proposal's concern that adaptation cost must stay
  constrained.

* **Eviction of unused indexes.** Once the cap is reached, the engine drops
  its own least-used index to make room. This is what makes the system
  genuinely adaptive to *shifting* workloads rather than merely additive.
"""
import threading
import time

from src.config import ADAPTIVE
from src.db import connect
from src.adaptive import executor
from src.adaptive.monitor import (collect_candidates, reset_statement_stats,
                                  snapshot_scan_stats)


class AdaptiveEngine(threading.Thread):
    """Background closed-loop optimizer."""

    def __init__(self, run_id, stop_event: threading.Event, verbose: bool = True):
        super().__init__(daemon=True)
        self.run_id = run_id
        self.stop_event = stop_event
        self.verbose = verbose
        self.actions = []
        self.cycles = 0

    # -- logging ---------------------------------------------------------
    def _log_action(self, conn, action_type, target, reason, duration_ms, ok=True):
        self.actions.append({
            "action_type": action_type, "target": target,
            "reason": reason, "duration_ms": duration_ms, "succeeded": ok,
        })
        if self.run_id is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO adaptive_action
                       (run_id, action_type, target, reason, duration_ms, succeeded)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (self.run_id, action_type, target, reason, duration_ms, ok),
                )
        except Exception as exc:
            if self.verbose:
                print(f"  [action log error] {exc}")

    # -- policy ----------------------------------------------------------
    def _choose(self, candidates):
        """Apply thresholds and return the single best candidate, or None."""
        for c in candidates:
            if c["calls"] < ADAPTIVE["min_calls"]:
                continue
            if c["seq_ratio"] < ADAPTIVE["seqscan_threshold"]:
                continue
            # Selectivity gate: an index on a low-cardinality column cannot
            # beat a sequential scan, so building one would add ETL write
            # overhead for no query benefit. Unknown selectivity (None) is
            # allowed through rather than blocking adaptation outright.
            sel = c.get("selectivity")
            if sel is not None and sel > ADAPTIVE["max_selectivity"]:
                continue
            return c
        return None

    def _make_room(self, conn):
        """Drop our least-used index if we've hit the cap. Returns True if freed."""
        with conn.cursor() as cur:
            mine = executor.list_adaptive_indexes(cur)
        if len(mine) < ADAPTIVE["max_indexes"]:
            return True
        victim = mine[0]                      # already sorted by scans ascending
        try:
            ms = executor.drop_index(conn, victim["index"])
            self._log_action(conn, "drop_index", victim["index"],
                             f"evicted: only {victim['scans']} scans, "
                             f"index cap {ADAPTIVE['max_indexes']} reached", ms)
            if self.verbose:
                print(f"  [adaptive] dropped {victim['index']} "
                      f"(unused, {victim['scans']} scans)")
            return True
        except Exception as exc:
            self._log_action(conn, "drop_failed", victim["index"], str(exc)[:200],
                             None, ok=False)
            return False

    # -- partition adaptation --------------------------------------------
    def _adapt_partitions(self, conn):
        """Keep the partition layout ahead of the ingestion frontier.

        Runs before index adaptation each cycle because it is both cheaper
        and more urgent: an unprovisioned frontier silently corrupts the
        physical design (rows pile into DEFAULT and then *block* the fix),
        whereas a missing index merely costs latency.
        """
        from src.adaptive import partitions as P

        parent = ADAPTIVE.get("partitioned_table", "fact_sales")
        try:
            with conn.cursor() as cur:
                stranded = P.get_default_contents(cur, parent)
        except Exception as exc:
            if self.verbose:
                print(f"  [partition inspect error] {exc}")
            return

        # 1. Recovery path: rows already stranded in DEFAULT. Do this first,
        #    since their presence blocks provisioning for the same range.
        for year, n in sorted(stranded.items()):
            try:
                P.ensure_date_dimension(conn, year)
                moved, ms = P.drain_default(conn, parent, year)
                self._log_action(
                    conn, "drain_default", f"{parent}_{year}",
                    f"migrated {moved} rows stranded in DEFAULT "
                    f"(blocks partition creation until cleared)", ms)
                if self.verbose:
                    print(f"  [adaptive] drained {moved} rows -> "
                          f"{parent}_{year} in {ms:.0f} ms")
            except Exception as exc:
                self._log_action(conn, "drain_failed", f"{parent}_{year}",
                                 str(exc)[:200], None, ok=False)
                if self.verbose:
                    print(f"  [adaptive] drain aborted: {str(exc)[:90]}")

        # 2. Preferred path: provision the next partition before it is needed.
        #    Re-evaluated *after* any drain, because draining a year creates
        #    that year's partition and so changes what is still pending.
        try:
            with conn.cursor() as cur:
                pending = P.needs_provisioning(cur, parent)
        except Exception:
            return

        if pending:
            try:
                added = P.ensure_date_dimension(conn, pending)
                ms = P.provision_partition(conn, parent, pending)
                self._log_action(
                    conn, "provision_partition", f"{parent}_{pending}",
                    f"ingestion frontier within "
                    f"{ADAPTIVE['partition_lead_days']}d of range end; "
                    f"created empty partition (+{added} dim_date rows)", ms)
                if self.verbose:
                    print(f"  [adaptive] provisioned {parent}_{pending} "
                          f"in {ms:.0f} ms (empty, no data movement)")
            except Exception as exc:
                self._log_action(conn, "provision_failed",
                                 f"{parent}_{pending}", str(exc)[:200],
                                 None, ok=False)
                if self.verbose:
                    print(f"  [adaptive] provision aborted: {str(exc)[:90]}")

    # -- thread body -----------------------------------------------------
    def run(self):
        conn = connect(autocommit=True)
        try:
            # Start from a clean telemetry slate so decisions reflect only
            # this run's workload, not whatever ran before it.
            with conn.cursor() as cur:
                reset_statement_stats(cur)
                # Baseline for delta-based scan ratios: without this the
                # engine would judge tables on their lifetime scan history
                # rather than the workload running right now.
                scan_baseline = snapshot_scan_stats(cur)

            # Let the workload warm up before the first decision, otherwise
            # we act on a handful of unrepresentative samples.
            self.stop_event.wait(ADAPTIVE["poll_interval_s"])

            while not self.stop_event.is_set():
                self.cycles += 1
                # Partition layout first: cheap, and an unprovisioned
                # frontier degrades the design in a way indexes cannot fix.
                if ADAPTIVE.get("adapt_partitions", True):
                    self._adapt_partitions(conn)
                try:
                    with conn.cursor() as cur:
                        candidates = collect_candidates(
                            cur, min_calls=ADAPTIVE["min_calls"],
                            scan_baseline=scan_baseline)
                    pick = self._choose(candidates)

                    if pick is None:
                        if self.verbose:
                            print("  [adaptive] no candidate met thresholds")
                    elif self._make_room(conn):
                        target = f"{pick['table']}({pick['column']})"
                        reason = (f"seq_ratio={pick['seq_ratio']}, "
                                  f"weight={pick['weight_ms']}ms, "
                                  f"calls={pick['calls']}")
                        try:
                            name, ms = executor.create_index(
                                conn, pick["table"], pick["column"])
                            self._log_action(conn, "create_index", target, reason, ms)
                            if self.verbose:
                                print(f"  [adaptive] built {name} in {ms:.0f} ms "
                                      f"({reason})")
                        except Exception as exc:
                            # Lock/statement timeouts land here. This is the
                            # designed safe failure: skip the adaptation,
                            # record why, and keep serving queries.
                            self._log_action(conn, "timeout_abort", target,
                                             str(exc)[:200], None, ok=False)
                            if self.verbose:
                                print(f"  [adaptive] aborted {target}: "
                                      f"{str(exc)[:90]}")
                except Exception as exc:
                    if self.verbose:
                        print(f"  [adaptive cycle error] {exc}")

                # Cooldown: let the new physical design be exercised before
                # the next decision.
                self.stop_event.wait(ADAPTIVE["poll_interval_s"])
        finally:
            conn.close()
            if self.verbose:
                created = sum(1 for a in self.actions
                              if a["action_type"] == "create_index")
                dropped = sum(1 for a in self.actions
                              if a["action_type"] == "drop_index")
                aborted = sum(1 for a in self.actions
                              if a["action_type"] in ("timeout_abort",
                                                      "provision_failed",
                                                      "drain_failed"))
                provisioned = sum(1 for a in self.actions
                                  if a["action_type"] == "provision_partition")
                drained = sum(1 for a in self.actions
                              if a["action_type"] == "drain_default")
                print(f"  adaptive: {self.cycles} cycles, {created} built, "
                      f"{dropped} evicted, {provisioned} partitions "
                      f"provisioned, {drained} drained, {aborted} aborted")


def reset_physical_design(verbose: bool = True):
    """Drop all engine-created indexes, restoring the static baseline design."""
    conn = connect(autocommit=True)
    try:
        return executor.drop_all_adaptive_indexes(conn, verbose=verbose)
    finally:
        conn.close()
