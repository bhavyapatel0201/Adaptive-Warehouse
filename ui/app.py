"""Live dashboard for the Adaptive Sales Warehouse.

A small Flask app that makes the system demonstrable: you can watch the
partition layout, launch an experiment, and see the adaptive engine build
indexes and provision partitions in real time -- which is far more convincing
in a demo than reading terminal output after the fact.

Run:
    python -m ui.app
    # then open http://localhost:5000

Nothing here changes experiment behaviour; it drives the same
`run_config()` the CLI uses, and reads the same log tables. The UI is a
window onto the system, not a second implementation of it.
"""
import io
import os
import threading
from contextlib import redirect_stdout

from flask import Flask, jsonify, render_template, request

from src.config import ADAPTIVE, EXPERIMENT_MATRIX, SCALE
from src.db import connect
from src.harness.run_experiment import run_config

app = Flask(__name__)

# When READ_ONLY is set (cloud deployment), the experiment-running and reset
# endpoints are disabled. A hosted free tier cannot sustain the background
# threads and CPU an experiment needs, so the deployed instance serves as a
# viewer of already-collected results rather than a place to launch new runs.
# Locally, leave it unset for the full interactive demo.
READ_ONLY = os.environ.get("READ_ONLY", "").lower() in ("1", "true", "yes")

# --- shared state for the currently running experiment -------------------
_run_state = {
    "running": False,
    "label": None,
    "log": [],
    "error": None,
}
_run_lock = threading.Lock()


class _LogCapture(io.StringIO):
    """Tee stdout from a running experiment into the shared log buffer."""

    def write(self, s):
        if s and s.strip():
            with _run_lock:
                _run_state["log"].append(s.rstrip())
                # keep the buffer bounded so a long run can't grow forever
                if len(_run_state["log"]) > 400:
                    del _run_state["log"][:100]
        return super().write(s)


def query(sql, params=None, one=False):
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return None
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            out = [dict(zip(cols, r)) for r in rows]
            return (out[0] if out else None) if one else out
    finally:
        conn.close()


# ---------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html",
                           configs=EXPERIMENT_MATRIX,
                           adaptive_cfg=ADAPTIVE,
                           read_only=READ_ONLY)


# ---------------------------------------------------------------------
# API: system status
# ---------------------------------------------------------------------
@app.route("/api/status")
def api_status():
    try:
        total = query("SELECT count(*) AS n FROM fact_sales", one=True)["n"]

        partitions = query("""
            SELECT c.relname AS name,
                   pg_get_expr(c.relpartbound, c.oid) ILIKE '%%DEFAULT%%' AS is_default,
                   COALESCE(s.n_live_tup, 0) AS rows,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS size
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE p.relname = 'fact_sales'
            ORDER BY c.relname
        """)
        # n_live_tup can lag; get exact counts for the (few) partitions
        for p in partitions:
            try:
                p["rows"] = query(
                    f'SELECT count(*) AS n FROM "{p["name"]}"', one=True)["n"]
            except Exception:
                pass

        # A partitioned index is only a catalog entry -- its storage and its
        # scan counters live on the per-partition leaf indexes. Reporting the
        # parent alone shows 0 bytes / 0 scans, which looks like the index
        # was never built. Roll the children up instead.
        adaptive_idx = query("""
            WITH parents AS (
                SELECT i.oid, i.relname, t.relname AS table_name, i.relkind
                FROM pg_class i
                JOIN pg_index x     ON x.indexrelid = i.oid
                JOIN pg_class t     ON t.oid = x.indrelid
                JOIN pg_namespace n ON n.oid = i.relnamespace
                WHERE n.nspname = 'public'
                  AND i.relname LIKE 'adx_%%'
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_inherits h WHERE h.inhrelid = i.oid
                  )
            )
            SELECT p.relname AS name,
                   p.table_name,
                   COALESCE(SUM(COALESCE(s.idx_scan, 0)), 0)::bigint AS scans,
                   pg_size_pretty(
                       COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
                       + pg_total_relation_size(p.oid)
                   ) AS size
            FROM parents p
            LEFT JOIN pg_inherits h ON h.inhparent = p.oid
            LEFT JOIN pg_class c    ON c.oid = h.inhrelid
            LEFT JOIN pg_stat_user_indexes s
                   ON s.indexrelid = COALESCE(c.oid, p.oid)
            GROUP BY p.relname, p.table_name, p.oid
            ORDER BY p.relname
        """)

        stranded = sum(p["rows"] for p in partitions if p["is_default"])

        return jsonify({
            "ok": True,
            "total_rows": total,
            "baseline_rows": SCALE["n_facts"],
            "partitions": partitions,
            "adaptive_indexes": adaptive_idx,
            "stranded_rows": stranded,
            "running": _run_state["running"],
            "run_label": _run_state["label"],
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# ---------------------------------------------------------------------
# API: results
# ---------------------------------------------------------------------
@app.route("/api/results")
def api_results():
    summary = query("""
        SELECT r.config_id, r.mode, r.workload, r.etl_rate,
               count(*) AS samples,
               round(avg(s.latency_ms)::numeric, 1) AS avg_ms,
               round((percentile_cont(0.5) WITHIN GROUP
                      (ORDER BY s.latency_ms))::numeric, 1) AS median_ms,
               round((percentile_cont(0.95) WITHIN GROUP
                      (ORDER BY s.latency_ms))::numeric, 1) AS p95_ms
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY r.config_id, r.mode, r.workload, r.etl_rate
        ORDER BY r.config_id, r.mode
    """)

    per_query = query("""
        SELECT s.query_name,
               round((avg(s.latency_ms) FILTER (WHERE r.mode='static'))::numeric,1)
                   AS static_ms,
               round((avg(s.latency_ms) FILTER (WHERE r.mode='adaptive'))::numeric,1)
                   AS adaptive_ms,
               round((100.0 * (avg(s.latency_ms) FILTER (WHERE r.mode='static')
                             - avg(s.latency_ms) FILTER (WHERE r.mode='adaptive'))
                      / NULLIF(avg(s.latency_ms) FILTER (WHERE r.mode='static'),0)
                     )::numeric, 1) AS improve_pct
        FROM query_sample s JOIN experiment_run r USING (run_id)
        GROUP BY s.query_name
        HAVING count(*) FILTER (WHERE r.mode='adaptive') > 0
           AND count(*) FILTER (WHERE r.mode='static')  > 0
        ORDER BY improve_pct DESC NULLS LAST
    """)

    actions = query("""
        SELECT r.config_id, a.action_type, a.target, a.reason,
               round(a.duration_ms::numeric,1) AS duration_ms,
               a.succeeded,
               to_char(a.ts, 'HH24:MI:SS') AS at
        FROM adaptive_action a JOIN experiment_run r USING (run_id)
        ORDER BY a.ts DESC
        LIMIT 40
    """)

    return jsonify({"summary": summary, "per_query": per_query,
                    "actions": actions})


# ---------------------------------------------------------------------
# API: run control
# ---------------------------------------------------------------------
def _background_run(cfg, mode, seconds):
    cap = _LogCapture()
    try:
        with redirect_stdout(cap):
            run_config(cfg, mode, seconds)
    except Exception as exc:
        with _run_lock:
            _run_state["error"] = str(exc)
            _run_state["log"].append(f"ERROR: {exc}")
    finally:
        with _run_lock:
            _run_state["running"] = False
            _run_state["log"].append("--- run finished ---")


@app.route("/api/run", methods=["POST"])
def api_run():
    if READ_ONLY:
        return jsonify({"ok": False,
                        "error": "This is a read-only deployment showing "
                                 "collected results. Run experiments locally "
                                 "with the full dashboard."}), 403
    with _run_lock:
        if _run_state["running"]:
            return jsonify({"ok": False,
                            "error": "an experiment is already running"}), 409

    data = request.get_json(force=True)
    config_id = data.get("config", "C5")
    mode = data.get("mode", "static")
    seconds = float(data.get("seconds", 30))

    cfg = next((c for c in EXPERIMENT_MATRIX
                if c["config_id"].upper() == config_id.upper()), None)
    if cfg is None:
        return jsonify({"ok": False, "error": f"unknown config {config_id}"}), 400

    with _run_lock:
        _run_state.update({
            "running": True,
            "label": f"{cfg['config_id']} [{mode}] "
                     f"{cfg['workload']} / etl={cfg['etl_rate']}",
            "log": [f"starting {cfg['config_id']} [{mode}] for {seconds:.0f}s ..."],
            "error": None,
        })

    threading.Thread(target=_background_run, args=(cfg, mode, seconds),
                     daemon=True).start()
    return jsonify({"ok": True, "label": _run_state["label"]})


@app.route("/api/log")
def api_log():
    with _run_lock:
        return jsonify({
            "running": _run_state["running"],
            "label": _run_state["label"],
            "log": list(_run_state["log"]),
            "error": _run_state["error"],
        })


# ---------------------------------------------------------------------
# API: maintenance actions
# ---------------------------------------------------------------------
def _reset_frontier():
    """Restore the warehouse to a clean, pre-demo state.

    Collapses the fiddly multi-command reset into one action:
      * drop every engine-created index
      * remove rows past the original data range
      * detach and drop any partitions the engine provisioned or drained into
      * trim ETL-injected rows and VACUUM

    This matters for demos specifically: once a partition beyond the baseline
    exists, ETL has somewhere valid to write, so nothing strands and the
    engine has no partition work to do. Both the proactive and the reactive
    path become undemonstrable until the frontier is rolled back.
    """
    from src.harness.reset_data import trim_etl_rows

    baseline_year = SCALE["year_end"]
    cutoff = (baseline_year + 1) * 10000 + 101

    from src.adaptive.engine import reset_physical_design
    dropped_idx = reset_physical_design(verbose=False)

    conn = connect(autocommit=True)
    dropped_parts = []
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fact_sales WHERE date_key >= %s", (cutoff,))
            removed = cur.rowcount

            # Any partition whose range starts at or past the cutoff was
            # created by the engine during a previous demo run.
            cur.execute("""
                SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bound
                FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = 'fact_sales'
            """)
            for name, bound in cur.fetchall():
                if not bound or "DEFAULT" in bound.upper():
                    continue
                digits, token = [], ""
                for ch in bound:
                    if ch.isdigit():
                        token += ch
                    elif token:
                        digits.append(int(token)); token = ""
                if token:
                    digits.append(int(token))
                if digits and digits[0] >= cutoff:
                    cur.execute(
                        f'ALTER TABLE fact_sales DETACH PARTITION "{name}"')
                    cur.execute(f'DROP TABLE "{name}"')
                    dropped_parts.append(name)
    finally:
        conn.close()

    # Called directly rather than as a subprocess: a subprocess with its
    # output captured hides any failure, which made an incomplete reset look
    # like a successful one.
    trim = trim_etl_rows()

    parts_msg = (", ".join(dropped_parts) if dropped_parts else "none")
    lines = [
        f"frontier reset — dropped {dropped_idx} adaptive index object(s), "
        f"removed {removed} post-baseline row(s), "
        f"trimmed {trim['removed']} ETL row(s), "
        f"dropped partition(s): {parts_msg}",
        f"fact_sales now holds {trim['after']} rows "
        f"(baseline is {SCALE['n_facts']}).",
    ]
    if trim["note"]:
        lines.append("NOTE: " + trim["note"])
    return lines


def _set_log(*lines):
    """Replace the shared log buffer.

    Reset messages have to go here rather than only into the HTTP response:
    the browser polls /api/log every second or so and rewrites the log panel
    from this buffer, so anything written directly to the panel gets wiped
    within a second and the reset looks like it did nothing.
    """
    with _run_lock:
        _run_state["log"] = list(lines)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    if READ_ONLY:
        return jsonify({"ok": False,
                        "error": "Reset is disabled on the read-only "
                                 "deployment."}), 403
    """Reset helpers so a demo can be re-run from a clean slate."""
    what = (request.get_json(force=True) or {}).get("what", "results")
    try:
        if what == "results":
            conn = connect(autocommit=True)
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM query_sample;")
                    cur.execute("DELETE FROM adaptive_action;")
                    cur.execute("DELETE FROM experiment_run;")
            finally:
                conn.close()
            _set_log("✓ experiment results cleared")
            return jsonify({"ok": True, "message": "experiment results cleared"})

        if what == "indexes":
            from src.adaptive.engine import reset_physical_design
            n = reset_physical_design(verbose=False)
            msg = f"dropped {n} adaptive index object(s)"
            _set_log("✓ " + msg)
            return jsonify({"ok": True, "message": msg})

        if what == "data":
            import subprocess, sys
            subprocess.run([sys.executable, "-m", "src.harness.reset_data",
                            "--yes"], capture_output=True, check=False)
            _set_log("✓ fact_sales trimmed to baseline")
            return jsonify({"ok": True,
                            "message": "fact_sales trimmed to baseline"})

        if what == "frontier":
            lines = _reset_frontier()
            _set_log("✓ " + lines[0], *lines[1:])
            return jsonify({"ok": True, "message": " | ".join(lines)})

        return jsonify({"ok": False, "error": f"unknown reset '{what}'"}), 400
    except Exception as exc:
        _set_log("✗ reset failed: " + str(exc))
        return jsonify({"ok": False, "error": str(exc)}), 500


def main():
    print("Adaptive Warehouse dashboard -> http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)


if __name__ == "__main__":
    main()
