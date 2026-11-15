"""Generate the comparison charts for the final report.

Reads the experiment log tables and writes publication-ready PNGs to
analysis/figures/. Every chart answers a specific question from the proposal:

  fig1_latency_by_etl.png      Does latency degrade as ETL ingestion rises,
                               and does adaptation hold it back?
                               (the core RQ: static vs adaptive under load)

  fig2_per_query_improvement.png  WHERE does adaptation help? This is the
                               most important figure -- it shows the benefit
                               is concentrated in selective drill-downs and
                               absent for broad scans, which an aggregate
                               number hides entirely.

  fig3_seqscan_and_plans.png   Did the physical design actually change?
                               Sequential-scan share and plan diversity.
                               CAVEAT: used_seqscan flags a plan if ANY node
                               is a sequential scan. The dimension tables are
                               small enough that Postgres always scans them
                               sequentially, so this metric sits near 100%
                               for both modes even when the fact table has
                               switched to a bitmap index scan. Read the
                               plan-diversity panel (right) as the real
                               signal that plans changed, and treat the
                               seq-scan panel as context, not as evidence
                               that adaptation failed.

  fig5_partition_adaptation.png Proactive provisioning vs reactive draining
                               -- the cost asymmetry between adapting ahead
                               of the workload and recovering after the fact.

  fig4_adaptation_overhead.png The cost side of the ledger: how much time
                               went into building indexes, and how many
                               adaptations were aborted by the lock guard.

Run:
    python -m analysis.plots
    python -m analysis.plots --outdir analysis/figures
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")            # no display in a container/headless machine
import matplotlib.pyplot as plt

from src.db import connect

# Consistent colours so every figure reads the same way.
C_STATIC = "#4C72B0"
C_ADAPTIVE = "#DD8452"
C_POS = "#55A868"
C_NEG = "#C44E52"

RATE_ORDER = ["zero", "low", "medium", "high"]
WORKLOADS = ["product", "customer", "temporal"]


def fetch(sql, params=None):
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return cols, cur.fetchall()
    finally:
        conn.close()


def _save(fig, outdir, name):
    path = os.path.join(outdir, name)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {path}")
    return path


# ---------------------------------------------------------------------
# Figure 1: latency vs ETL rate, per workload, static vs adaptive
# ---------------------------------------------------------------------
def fig_latency_by_etl(outdir):
    _, rows = fetch("""
        SELECT r.workload, r.etl_rate, r.mode,
               avg(s.latency_ms) AS avg_ms,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY s.latency_ms) AS med_ms
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY r.workload, r.etl_rate, r.mode
    """)
    if not rows:
        print("  (skipping fig1: no data)")
        return None

    data = {(w, e, m): (a, md) for w, e, m, a, md in rows}
    present = [w for w in WORKLOADS if any(k[0] == w for k in data)]
    if not present:
        return None

    fig, axes = plt.subplots(1, len(present), figsize=(5.2 * len(present), 4.2),
                             sharey=False)
    if len(present) == 1:
        axes = [axes]

    for ax, wl in zip(axes, present):
        rates = [r for r in RATE_ORDER if any(k[0] == wl and k[1] == r for k in data)]
        x = range(len(rates))
        width = 0.38
        static_vals = [data.get((wl, r, "static"), (0, 0))[1] for r in rates]
        adapt_vals = [data.get((wl, r, "adaptive"), (0, 0))[1] for r in rates]

        ax.bar([i - width / 2 for i in x], static_vals, width,
               label="static", color=C_STATIC)
        if any(adapt_vals):
            ax.bar([i + width / 2 for i in x], adapt_vals, width,
                   label="adaptive", color=C_ADAPTIVE)

        ax.set_title(f"{wl}-centric workload")
        ax.set_xticks(list(x))
        ax.set_xticklabels(rates)
        ax.set_xlabel("ETL ingestion rate")
        ax.set_ylabel("median query latency (ms)")
        ax.grid(axis="y", alpha=0.3)
        ax.legend()

    fig.suptitle("Query latency under increasing ETL ingestion pressure",
                 fontsize=13, y=1.02)
    return _save(fig, outdir, "fig1_latency_by_etl.png")


# ---------------------------------------------------------------------
# Figure 2: per-query improvement (the headline result)
# ---------------------------------------------------------------------
def fig_per_query_improvement(outdir):
    _, rows = fetch("""
        SELECT s.query_name,
               avg(s.latency_ms) FILTER (WHERE r.mode = 'static')   AS static_ms,
               avg(s.latency_ms) FILTER (WHERE r.mode = 'adaptive') AS adaptive_ms
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY s.query_name
        HAVING count(*) FILTER (WHERE r.mode = 'adaptive') > 0
           AND count(*) FILTER (WHERE r.mode = 'static')   > 0
    """)
    if not rows:
        print("  (skipping fig2: needs both static and adaptive runs)")
        return None

    items = []
    for name, st, ad in rows:
        if not st or st == 0:
            continue
        items.append((name, 100.0 * (float(st) - float(ad)) / float(st)))
    if not items:
        return None
    items.sort(key=lambda t: t[1])

    names = [i[0] for i in items]
    vals = [i[1] for i in items]
    colors = [C_POS if v >= 0 else C_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(9, max(4, 0.38 * len(items))))
    ax.barh(names, vals, color=colors)
    ax.axvline(0, color="black", linewidth=0.9)
    ax.set_xlabel("latency improvement with adaptive indexing (%)")
    ax.set_title("Where adaptive optimization actually helps\n"
                 "(positive = adaptive faster; selective drill-downs vs broad rollups)",
                 fontsize=12)
    ax.grid(axis="x", alpha=0.3)
    for i, v in enumerate(vals):
        ax.text(v + (1.2 if v >= 0 else -1.2), i, f"{v:.1f}%",
                va="center", ha="left" if v >= 0 else "right", fontsize=9)
    return _save(fig, outdir, "fig2_per_query_improvement.png")


# ---------------------------------------------------------------------
# Figure 3: sequential-scan share and plan diversity
# ---------------------------------------------------------------------
def fig_seqscan_and_plans(outdir):
    _, rows = fetch("""
        SELECT r.workload, r.mode,
               100.0 * sum(CASE WHEN s.used_seqscan THEN 1 ELSE 0 END)
                     / NULLIF(count(*), 0)      AS seqscan_pct,
               count(DISTINCT s.plan_hash)      AS distinct_plans
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY r.workload, r.mode
    """)
    if not rows:
        print("  (skipping fig3: no data)")
        return None

    data = {(w, m): (float(p or 0), int(d)) for w, m, p, d in rows}
    present = [w for w in WORKLOADS if any(k[0] == w for k in data)]
    if not present:
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = range(len(present))
    width = 0.38

    ax1.bar([i - width / 2 for i in x],
            [data.get((w, "static"), (0, 0))[0] for w in present],
            width, label="static", color=C_STATIC)
    ax1.bar([i + width / 2 for i in x],
            [data.get((w, "adaptive"), (0, 0))[0] for w in present],
            width, label="adaptive", color=C_ADAPTIVE)
    ax1.set_xticks(list(x)); ax1.set_xticklabels(present)
    ax1.set_ylabel("queries using a sequential scan (%)")
    ax1.set_title("Sequential-scan share")
    ax1.grid(axis="y", alpha=0.3); ax1.legend()

    ax2.bar([i - width / 2 for i in x],
            [data.get((w, "static"), (0, 0))[1] for w in present],
            width, label="static", color=C_STATIC)
    ax2.bar([i + width / 2 for i in x],
            [data.get((w, "adaptive"), (0, 0))[1] for w in present],
            width, label="adaptive", color=C_ADAPTIVE)
    ax2.set_xticks(list(x)); ax2.set_xticklabels(present)
    ax2.set_ylabel("distinct execution plans observed")
    ax2.set_title("Plan diversity (proxy for plan changes)")
    ax2.grid(axis="y", alpha=0.3); ax2.legend()

    fig.suptitle("Did the physical design actually change?", fontsize=13, y=1.02)
    return _save(fig, outdir, "fig3_seqscan_and_plans.png")


# ---------------------------------------------------------------------
# Figure 4: adaptation overhead (the cost side)
# ---------------------------------------------------------------------
def fig_adaptation_overhead(outdir):
    _, rows = fetch("""
        SELECT r.workload, a.action_type, count(*) AS n,
               COALESCE(avg(a.duration_ms), 0) AS avg_ms
        FROM adaptive_action a JOIN experiment_run r USING (run_id)
        GROUP BY r.workload, a.action_type
    """)
    if not rows:
        print("  (skipping fig4: no adaptive actions recorded)")
        return None

    types = sorted({r[1] for r in rows})
    present = [w for w in WORKLOADS if any(r[0] == w for r in rows)]
    counts = {(w, t): 0 for w in present for t in types}
    durs = {(w, t): 0.0 for w in present for t in types}
    for w, t, n, ms in rows:
        counts[(w, t)] = int(n)
        durs[(w, t)] = float(ms or 0)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = range(len(present))
    width = 0.8 / max(1, len(types))
    palette = [C_ADAPTIVE, C_NEG, C_POS, C_STATIC]

    for i, t in enumerate(types):
        off = (i - (len(types) - 1) / 2) * width
        ax1.bar([j + off for j in x], [counts[(w, t)] for w in present],
                width, label=t, color=palette[i % len(palette)])
        ax2.bar([j + off for j in x], [durs[(w, t)] for w in present],
                width, label=t, color=palette[i % len(palette)])

    ax1.set_xticks(list(x)); ax1.set_xticklabels(present)
    ax1.set_ylabel("actions taken"); ax1.set_title("Adaptation actions by type")
    ax1.grid(axis="y", alpha=0.3); ax1.legend(fontsize=8)

    ax2.set_xticks(list(x)); ax2.set_xticklabels(present)
    ax2.set_ylabel("average duration (ms)")
    ax2.set_title("Cost per adaptation")
    ax2.grid(axis="y", alpha=0.3); ax2.legend(fontsize=8)

    fig.suptitle("Adaptation overhead: what the optimization cost",
                 fontsize=13, y=1.02)
    return _save(fig, outdir, "fig4_adaptation_overhead.png")


# ---------------------------------------------------------------------
# Figure 5: partition adaptation -- proactive vs reactive cost
# ---------------------------------------------------------------------
def fig_partition_adaptation(outdir):
    _, rows = fetch("""
        SELECT action_type, count(*) AS n,
               COALESCE(avg(duration_ms),0) AS avg_ms
        FROM adaptive_action
        WHERE action_type IN ('provision_partition','drain_default')
        GROUP BY action_type
    """)
    if not rows:
        print("  (skipping fig5: no partition actions recorded)")
        return None

    label_map = {
        "provision_partition": "Proactive\n(provision empty\npartition ahead of need)",
        "drain_default": "Reactive\n(drain rows already\nstranded in DEFAULT)",
    }
    order = ["provision_partition", "drain_default"]
    data = {a: (int(n), float(ms)) for a, n, ms in rows}
    present = [a for a in order if a in data]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.4))
    labels = [label_map[a] for a in present]
    colors = [C_POS if a == "provision_partition" else C_NEG for a in present]

    ax1.bar(labels, [data[a][1] for a in present], color=colors)
    ax1.set_ylabel("average duration (ms)")
    ax1.set_title("Cost per partition adaptation")
    ax1.grid(axis="y", alpha=0.3)
    for i, a in enumerate(present):
        ax1.text(i, data[a][1], f"{data[a][1]:.0f} ms",
                 ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax2.bar(labels, [data[a][0] for a in present], color=colors)
    ax2.set_ylabel("times invoked")
    ax2.set_title("How often each path was taken")
    ax2.grid(axis="y", alpha=0.3)

    fig.suptitle("Partition adaptation: anticipating vs reacting",
                 fontsize=13, y=1.02)
    return _save(fig, outdir, "fig5_partition_adaptation.png")


def main():
    ap = argparse.ArgumentParser(description="Generate report figures.")
    ap.add_argument("--outdir", default="analysis/figures")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    print(f"Generating figures into {args.outdir}/ ...")

    made = [
        fig_latency_by_etl(args.outdir),
        fig_per_query_improvement(args.outdir),
        fig_seqscan_and_plans(args.outdir),
        fig_adaptation_overhead(args.outdir),
        fig_partition_adaptation(args.outdir),
    ]
    n = len([m for m in made if m])
    print(f"\nDone: {n} figure(s) generated.")
    if n == 0:
        print("No data found. Run experiments first:")
        print("  python -m src.harness.run_matrix --seconds 30 --repeat 3")


if __name__ == "__main__":
    main()
