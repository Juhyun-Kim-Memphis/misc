#!/usr/bin/env python3
"""Multi-client file-IO + metadata benchmark for the Hammerspace PoC.

Companion to ``fileio-benchmark.py``. Instead of one client doing the
work, N independent clients (Pods of a Kubernetes Indexed Job) run the
**same** fio + metadata workload on the **same** PVC, each in their own
sub-directory so they don't trample each other's files. The leader
collects the per-client results, aggregates them, and writes a single
row to the Google Sheet's ``fileio-multi`` worksheet.

The goal is to probe what the single-client benchmark cannot answer —
whether the backend (e.g. a 10-DSX Hammerspace cluster) actually scales
beyond the ceiling that a single NFS mount / fio process hits.

NOTE — canonical home is the **public** misc repo.
    The source of truth for this script lives at
        https://github.com/Juhyun-Kim-Memphis/misc/blob/main/hammerspace-poc/fileio-multiclient-benchmark.py
    (public, shared with Samwoo / Hammerspace). The bench Job in
    vessl-ai/k8s-manifest-test clones misc and runs the script from
    there. The copy in vessl-ai/k8s-manifest-test/hammerspace-poc/
    is a downstream backup; sync rules in CLAUDE.md §7 apply to this
    file just like to ``fileio-benchmark.py``.

Coordination
------------
Pure file-based, over the same NFS that is being benchmarked — no extra
infra (no Lease, no headless service). The leader is ``index 0`` of the
Indexed Job:

    <mount>/__mc_bench__/<run_id>/
        start                          # leader writes this; followers wait
        client-<i>/                    # each client's working dir
        result-<i>.json                # each client's per-metric dump
        done-<i>                       # each client's completion marker
                                       # (touched AFTER result-<i>.json
                                       #  so existence is atomic w.r.t.
                                       #  the leader's poll loop)

Phases (per pod):
    1. Leader publishes ``start``; followers wait on it.
    2. Every client measures inside its own ``client-<i>/`` sub-dir,
       running the *same* fio×4 + metadata workload as fileio-benchmark.py
       (delegated via importlib).
    3. Each client writes ``result-<i>.json`` then touches ``done-<i>``,
       then exits.
    4. Leader (after its own measurement) waits for all N done markers,
       loads all result files, aggregates, writes one Sheet row, cleans
       up ``__mc_bench__/<run_id>/``.

Aggregation policy
------------------
Throughput metrics (MiBps, IOPS, ops/s) are recorded as
    <metric>_agg        sum across N clients  (system-level throughput)
    <metric>_p_mean     mean per client
    <metric>_p_stddev   stddev across clients
    <metric>_p_min      min  across clients
    <metric>_p_max      max  across clients

Latency metrics (ms) are recorded only as p_mean / p_stddev / p_min /
p_max (summing latencies has no physical meaning).

Plus the following operational columns that only make sense for a
multi-client run:
    num_clients         total N (matches Job's parallelism = completions)
    mount_options       NFS mount options seen by the leader (e.g.
                        vers=4.2,nconnect=8). Other clients are assumed
                        to share the same mount; if that changes, dump
                        per-client mount options too.
    client_nodes        unique k8s node names the clients landed on
    client_pods         all pod hostnames participating
    pod_node_map        verbatim index → pod@node string for audit
    barrier_align_s     max(start_utc) − min(start_utc) across clients.
                        Lower is better; if this is large the aggregate
                        is suspect because not every client was active
                        for the same wall-clock window.
    wall_time_max_s     longest per-client measurement
    wall_time_p_mean_s  mean per-client measurement
    run_id              identical across all pods of one Job

Usage
-----
    fileio-multiclient-benchmark.py <MOUNT_PATH>

Required env vars
-----------------
    JOB_COMPLETION_INDEX            0..N-1, injected by Indexed Job.
    NUM_CLIENTS                     Total N, must match the Job's
                                    parallelism/completions.
    MC_RUN_ID                       Identical across pods of one run.
                                    Use a timestamp or UUID from the
                                    Job manifest (see template).
    SCENARIO                        Free-form label.
    STORAGECLASS                    SC backing the mounted PVC.
    BENCH_SHEET_URL  (or  BENCH_SHEET_ID)
    GOOGLE_APPLICATION_CREDENTIALS

Optional env vars
-----------------
    BENCH_WORKSHEET     Default: "fileio-multi".
    BENCH_BW_SIZE, BENCH_BW_RUNTIME, BENCH_IOPS_RUNTIME,
    BENCH_IOPS_NUMJOBS, BENCH_IOPS_IODEPTH,
    BENCH_META_FILES, BENCH_META_DIRS, BENCH_META_WORKERS
                        Same defaults as fileio-benchmark.py.
    MC_BARRIER_TIMEOUT_S Default: 120. Follower gives up if leader
                        hasn't published the barrier within this.
    MC_RESULT_TIMEOUT_S  Default: 600. Leader gives up if not all N
                        clients have written done markers by this.
    NODE_NAME           Optional but recommended. Pass via fieldRef
                        spec.nodeName in the Job template so we can
                        record the topology.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import socket
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ── Import single-client primitives from fileio-benchmark.py ──────────────
# The single-client script's filename uses a hyphen, which is not a valid
# Python identifier, so we have to load it explicitly via importlib.
_THIS = Path(__file__).resolve()
_SIBLING = _THIS.parent / "fileio-benchmark.py"
if not _SIBLING.exists():
    sys.exit(
        f"ERROR: sibling script not found: {_SIBLING}\n"
        "fileio-multiclient-benchmark.py expects fileio-benchmark.py in the\n"
        "same directory (both files live in misc/hammerspace-poc/)."
    )
_spec = importlib.util.spec_from_file_location("fileio_benchmark", str(_SIBLING))
fb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fb)


# ── Metric taxonomy ───────────────────────────────────────────────────────

THROUGHPUT_METRICS = [
    "seq_write_MiBps",
    "seq_read_MiBps",
    "randwrite_IOPS",
    "randread_IOPS",
    "meta_create_ops",
    "meta_stat_ops",
    "meta_list_ops",
    "meta_read_ops",
    "meta_delete_ops",
]

LATENCY_METRICS = [
    "seq_write_lat_ms",
    "seq_read_lat_ms",
    "randwrite_lat_ms",
    "randread_lat_ms",
]


# ── Local helpers ─────────────────────────────────────────────────────────


def wait_for(predicate, timeout_s: float, label: str, poll_s: float = 1.0) -> None:
    """Poll ``predicate()`` until it returns truthy or timeout elapses."""
    t0 = time.time()
    last_log = 0.0
    while True:
        if predicate():
            return
        elapsed = time.time() - t0
        if elapsed > timeout_s:
            raise TimeoutError(f"{label}: timed out after {timeout_s:.0f}s")
        if time.time() - last_log > 5:
            print(f"  [wait] {label} ... {elapsed:.0f}s", flush=True)
            last_log = time.time()
        time.sleep(poll_s)


def stats_block(values: list[float]) -> dict[str, float]:
    """Return p_mean / p_stddev / p_min / p_max for a list of numbers."""
    if not values:
        return {"p_mean": 0.0, "p_stddev": 0.0, "p_min": 0.0, "p_max": 0.0}
    return {
        "p_mean": round(statistics.fmean(values), 3),
        "p_stddev": round(statistics.pstdev(values), 3) if len(values) > 1 else 0.0,
        "p_min": round(min(values), 3),
        "p_max": round(max(values), 3),
    }


def read_mount_options(mount_path: Path) -> str:
    """Return the mount option string (column 4 of /proc/mounts) for the FS
    containing ``mount_path``. Empty string if not found."""
    target = str(mount_path.resolve())
    best_mp, best_opts = "", ""
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 4:
                    continue
                mp, opts = parts[1], parts[3]
                if target == mp or target.startswith(mp.rstrip("/") + "/"):
                    if len(mp) > len(best_mp):
                        best_mp, best_opts = mp, opts
    except OSError:
        pass
    return best_opts


def iso_to_epoch_s(iso_utc: str) -> float:
    """Parse the UTC ISO timestamps emitted by ``fb.utc_iso()``."""
    return (
        datetime.strptime(iso_utc, "%Y-%m-%dT%H:%M:%SZ")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )


def aggregate(per_client_metrics: list[dict]) -> dict[str, float]:
    """Reduce N per-client metric dicts to one flat aggregate dict.

    Throughput metrics get ``_agg`` (sum) plus p_mean/p_stddev/p_min/p_max.
    Latency metrics get only the per-client stats (no _agg).
    """
    out: dict[str, float] = {}
    for m in THROUGHPUT_METRICS:
        vs = [r[m] for r in per_client_metrics if m in r]
        out[f"{m}_agg"] = round(sum(vs), 3)
        for k, v in stats_block(vs).items():
            out[f"{m}_{k}"] = v
    for m in LATENCY_METRICS:
        vs = [r[m] for r in per_client_metrics if m in r]
        for k, v in stats_block(vs).items():
            out[f"{m}_{k}"] = v
    return out


# ── Single client's measurement run (delegates to fileio-benchmark.py) ───


def measure_one_client(
    workdir: Path,
    *,
    bw_size: str,
    bw_runtime: int,
    iops_runtime: int,
    iops_numjobs: int,
    iops_iodepth: int,
    meta_files: int,
    meta_dirs: int,
    meta_workers: int,
) -> dict[str, float]:
    """Run the same fio×4 + metadata workload as fileio-benchmark.py.

    Returns the same metric dict shape that fileio-benchmark.py emits.
    """
    workdir.mkdir(parents=True, exist_ok=True)

    print(f"\n[fio] sequential write — size={bw_size}, bs=1M, runtime≤{bw_runtime}s")
    j = fb.run_fio(
        workdir,
        "seq_write",
        [
            "--rw=write",
            "--bs=1M",
            f"--size={bw_size}",
            f"--runtime={bw_runtime}",
            "--time_based",
            "--direct=1",
            "--ioengine=psync",
            "--end_fsync=1",
            "--numjobs=1",
            "--group_reporting",
        ],
    )
    bw_w, lat_w = fb.fio_bandwidth(j, "write")
    print(f"       {bw_w:8.1f} MiB/s   lat_mean={lat_w:.2f} ms")

    print(f"[fio] sequential read  — size={bw_size}, bs=1M, runtime≤{bw_runtime}s")
    j = fb.run_fio(
        workdir,
        "seq_read",
        [
            "--rw=read",
            "--bs=1M",
            f"--size={bw_size}",
            f"--runtime={bw_runtime}",
            "--time_based",
            "--direct=1",
            "--ioengine=psync",
            "--numjobs=1",
            "--group_reporting",
        ],
    )
    bw_r, lat_r = fb.fio_bandwidth(j, "read")
    print(f"       {bw_r:8.1f} MiB/s   lat_mean={lat_r:.2f} ms")

    print(
        f"[fio] random write — bs=4k, numjobs={iops_numjobs}, "
        f"iodepth={iops_iodepth}, runtime≤{iops_runtime}s"
    )
    j = fb.run_fio(
        workdir,
        "rand_write",
        [
            "--rw=randwrite",
            "--bs=4k",
            "--size=512M",
            f"--runtime={iops_runtime}",
            "--time_based",
            "--direct=1",
            "--ioengine=libaio",
            f"--iodepth={iops_iodepth}",
            f"--numjobs={iops_numjobs}",
            "--group_reporting",
        ],
    )
    iops_w, lat_iw = fb.fio_iops(j, "write")
    print(f"       {iops_w:8.0f} IOPS    lat_mean={lat_iw:.2f} ms")

    print(
        f"[fio] random read  — bs=4k, numjobs={iops_numjobs}, "
        f"iodepth={iops_iodepth}, runtime≤{iops_runtime}s"
    )
    j = fb.run_fio(
        workdir,
        "rand_read",
        [
            "--rw=randread",
            "--bs=4k",
            "--size=512M",
            f"--runtime={iops_runtime}",
            "--time_based",
            "--direct=1",
            "--ioengine=libaio",
            f"--iodepth={iops_iodepth}",
            f"--numjobs={iops_numjobs}",
            "--group_reporting",
        ],
    )
    iops_r, lat_ir = fb.fio_iops(j, "read")
    print(f"       {iops_r:8.0f} IOPS    lat_mean={lat_ir:.2f} ms")

    # Drop fio artefacts so the metadata phase starts on a clean dir
    for p in workdir.iterdir():
        if p.is_file():
            p.unlink()

    print(f"\n[meta] {meta_files} files in {meta_dirs} dirs, {meta_workers} workers")
    meta_metrics = fb.run_metadata_test(
        workdir / "meta",
        n_files=meta_files,
        n_dirs=meta_dirs,
        workers=meta_workers,
    )

    return {
        "seq_write_MiBps": round(bw_w, 1),
        "seq_write_lat_ms": round(lat_w, 3),
        "seq_read_MiBps": round(bw_r, 1),
        "seq_read_lat_ms": round(lat_r, 3),
        "randwrite_IOPS": round(iops_w, 0),
        "randwrite_lat_ms": round(lat_iw, 3),
        "randread_IOPS": round(iops_r, 0),
        "randread_lat_ms": round(lat_ir, 3),
        **meta_metrics,
    }


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Multi-client fio + metadata benchmark; leader uploads one row to Sheets."
    )
    parser.add_argument("mount_path", help="Filesystem path to benchmark (e.g. /mnt/hs)")
    parser.add_argument(
        "--skip-sheet",
        action="store_true",
        help="Skip the Sheet upload (still runs the full measurement + aggregation).",
    )
    args = parser.parse_args()

    mount_path = Path(args.mount_path).resolve()
    if not mount_path.is_dir() or not os.access(mount_path, os.W_OK):
        print(f"ERROR: {mount_path} is not a writable directory", file=sys.stderr)
        return 2

    # Coordination params (Indexed Job env)
    try:
        index = int(os.environ["JOB_COMPLETION_INDEX"])
        num_clients = int(os.environ["NUM_CLIENTS"])
    except (KeyError, ValueError) as e:
        print(
            f"ERROR: JOB_COMPLETION_INDEX and NUM_CLIENTS env vars are required ({e})",
            file=sys.stderr,
        )
        return 2
    run_id = fb.env("MC_RUN_ID") or f"run-{int(time.time())}"
    if not 0 <= index < num_clients:
        print(
            f"ERROR: JOB_COMPLETION_INDEX={index} not in [0,{num_clients})",
            file=sys.stderr,
        )
        return 2
    is_leader = index == 0

    # Identifying labels
    scenario = fb.env("SCENARIO")
    storageclass = fb.env("STORAGECLASS")
    if not scenario or not storageclass:
        print("ERROR: SCENARIO and STORAGECLASS env vars are required", file=sys.stderr)
        return 2

    # Single-client knobs (same defaults as fileio-benchmark.py)
    bw_size = fb.env("BENCH_BW_SIZE", "1G")
    bw_runtime = fb.env_int("BENCH_BW_RUNTIME", 15)
    iops_runtime = fb.env_int("BENCH_IOPS_RUNTIME", 12)
    iops_numjobs = fb.env_int("BENCH_IOPS_NUMJOBS", 4)
    iops_iodepth = fb.env_int("BENCH_IOPS_IODEPTH", 16)
    meta_files = fb.env_int("BENCH_META_FILES", 5000)
    meta_dirs = fb.env_int("BENCH_META_DIRS", 50)
    meta_workers = fb.env_int("BENCH_META_WORKERS", 16)

    barrier_timeout = fb.env_int("MC_BARRIER_TIMEOUT_S", 120)
    result_timeout = fb.env_int("MC_RESULT_TIMEOUT_S", 600)

    # Sheets pre-flight — leader only. Followers never touch the sheet.
    ws = None
    if is_leader and not args.skip_sheet:
        sheet_url_or_id = fb.env("BENCH_SHEET_URL") or fb.env("BENCH_SHEET_ID")
        sa_json = fb.env("GOOGLE_APPLICATION_CREDENTIALS")
        worksheet_name = fb.env("BENCH_WORKSHEET", "fileio-multi")
        if not sheet_url_or_id:
            print(
                "ERROR: BENCH_SHEET_URL or BENCH_SHEET_ID required (or --skip-sheet)",
                file=sys.stderr,
            )
            return 2
        if not sa_json:
            print(
                "ERROR: GOOGLE_APPLICATION_CREDENTIALS required (or --skip-sheet)",
                file=sys.stderr,
            )
            return 2
        sheet_id = fb.parse_sheet_id(sheet_url_or_id)
        print(f"[init] leader opening sheet {sheet_id} / worksheet '{worksheet_name}'")
        ws = fb.open_sheet(sheet_id, worksheet_name, sa_json)
        print("[init] sheet ready")

    # Layout
    coord_root = mount_path / "__mc_bench__" / run_id
    barrier_file = coord_root / "start"
    client_dir = coord_root / f"client-{index:02d}"
    result_file = coord_root / f"result-{index:02d}.json"
    done_file = coord_root / f"done-{index:02d}"

    fstype = fb.get_fstype(mount_path)
    mount_options = read_mount_options(mount_path)
    hostname = socket.gethostname()
    node_name = os.environ.get("NODE_NAME", "?")

    role = "leader" if is_leader else "follower"
    print(f"[init] index={index}/{num_clients} role={role} run_id={run_id}")
    print(f"       pod={hostname} node={node_name} fstype={fstype}")
    print(f"       mount_options={mount_options}")

    # ── Phase 1: barrier ─────────────────────────────────────────────────
    if is_leader:
        coord_root.mkdir(parents=True, exist_ok=True)
        barrier_payload = {
            "run_id": run_id,
            "num_clients": num_clients,
            "leader_published_utc": fb.utc_iso(),
            "leader_pod": hostname,
            "leader_node": node_name,
        }
        # Atomic-ish publish: write to tmp then rename.
        tmp = barrier_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(barrier_payload))
        tmp.replace(barrier_file)
        print(f"[barrier] leader published {barrier_file}")
    else:
        print(f"[barrier] follower waiting for {barrier_file}")
        wait_for(barrier_file.exists, barrier_timeout, "barrier")
        print(f"[barrier] follower cleared barrier")

    start_utc = fb.utc_iso()
    start_perf = time.perf_counter()

    # ── Phase 2: per-client measurement ──────────────────────────────────
    metrics: dict[str, float] = {}
    try:
        metrics = measure_one_client(
            client_dir,
            bw_size=bw_size,
            bw_runtime=bw_runtime,
            iops_runtime=iops_runtime,
            iops_numjobs=iops_numjobs,
            iops_iodepth=iops_iodepth,
            meta_files=meta_files,
            meta_dirs=meta_dirs,
            meta_workers=meta_workers,
        )
    finally:
        # Each client cleans up its own working dir, no matter what.
        shutil.rmtree(client_dir, ignore_errors=True)

    elapsed = time.perf_counter() - start_perf
    end_utc = fb.utc_iso()

    # ── Phase 3: dump per-client result ──────────────────────────────────
    result_payload = {
        "index": index,
        "is_leader": is_leader,
        "pod": hostname,
        "node": node_name,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "elapsed_s": round(elapsed, 2),
        "mount_options": mount_options,
        "metrics": metrics,
    }
    # Write result THEN touch done — leader only watches done markers.
    tmp_result = result_file.with_suffix(".json.tmp")
    tmp_result.write_text(json.dumps(result_payload))
    tmp_result.replace(result_file)
    done_file.touch()
    print(
        f"\n[result] index={index} dumped {result_file.name} "
        f"(elapsed={elapsed:.1f}s)"
    )

    # Followers exit here.
    if not is_leader:
        print(f"[follower {index}] done — exiting")
        return 0

    # ── Phase 4: leader aggregates ──────────────────────────────────────
    print(f"[leader] waiting for all {num_clients} done markers ...")

    def _all_done() -> bool:
        return all((coord_root / f"done-{i:02d}").exists() for i in range(num_clients))

    wait_for(_all_done, result_timeout, "all-results")

    per_client: list[dict] = []
    for i in range(num_clients):
        rf = coord_root / f"result-{i:02d}.json"
        per_client.append(json.loads(rf.read_text()))

    # Sanity: warn if any client's metrics dict is missing or empty.
    bad = [r["index"] for r in per_client if not r.get("metrics")]
    if bad:
        print(
            f"WARNING: clients {bad} reported empty metrics — aggregation will be partial",
            file=sys.stderr,
        )

    metric_dicts = [r["metrics"] for r in per_client if r.get("metrics")]
    agg = aggregate(metric_dicts)

    # Operational columns
    start_secs = [iso_to_epoch_s(r["start_utc"]) for r in per_client]
    barrier_align_s = max(start_secs) - min(start_secs)
    elapsed_per_client = [r["elapsed_s"] for r in per_client]
    wall_time_max_s = max(elapsed_per_client)
    wall_time_mean_s = statistics.fmean(elapsed_per_client)

    nodes = sorted({r["node"] for r in per_client})
    pods = sorted(r["pod"] for r in per_client)
    pod_node_map = " | ".join(
        f"{r['index']}:{r['pod']}@{r['node']}" for r in per_client
    )

    config = {
        "timestamp_utc": fb.utc_iso(),
        "scenario": scenario,
        "storageclass": storageclass,
        "fstype": fstype,
        "num_clients": num_clients,
        "mount_options": mount_options,
        "client_nodes": ",".join(nodes),
        "client_pods": ",".join(pods),
        "pod_node_map": pod_node_map,
        "bw_size": bw_size,
        "bw_runtime_s": bw_runtime,
        "iops_runtime_s": iops_runtime,
        "iops_numjobs": iops_numjobs,
        "iops_iodepth": iops_iodepth,
        "meta_files": meta_files,
        "meta_dirs": meta_dirs,
        "meta_workers": meta_workers,
        "barrier_align_s": round(barrier_align_s, 2),
        "wall_time_max_s": round(wall_time_max_s, 2),
        "wall_time_p_mean_s": round(wall_time_mean_s, 2),
        "run_id": run_id,
    }

    # Pretty stdout summary
    print("\n" + "=" * 80)
    print(f"  Scenario     : {scenario}")
    print(f"  StorageClass : {storageclass}")
    print(f"  Mount        : {mount_path}  ({fstype})  opts={mount_options}")
    print(f"  Clients      : {num_clients}  on {len(nodes)} unique node(s)")
    print(f"  Barrier align: {barrier_align_s:.2f}s (max−min of per-client start_utc)")
    print(
        f"  Wall time    : max={wall_time_max_s:.1f}s  mean={wall_time_mean_s:.1f}s"
    )
    print("-" * 80)
    print("  Per-client matrix:")
    keys = THROUGHPUT_METRICS + LATENCY_METRICS
    header = ["idx"] + keys
    rows = [
        [str(r["index"])]
        + [str(r.get("metrics", {}).get(k, "")) for k in keys]
        for r in per_client
    ]
    col_w = [max(len(line[c]) for line in [header] + rows) for c in range(len(header))]
    fmt = "  ".join("{{:>{}}}".format(w) for w in col_w)
    print("  " + fmt.format(*header))
    for row in rows:
        print("  " + fmt.format(*row))
    print("-" * 80)
    print("  Aggregated row (will be appended to the sheet):")
    for k, v in agg.items():
        print(f"    {k:32s} {v}")
    print("=" * 80)

    if ws is not None:
        row = {**config, **agg}
        print(f"\n[sheet] appending row to '{ws.title}' (cols={len(row)})")
        fb.append_row(ws, row)
        print("[sheet] done")

    # Cleanup — leader removes the whole coord dir for this run.
    try:
        shutil.rmtree(coord_root, ignore_errors=True)
        print(f"[cleanup] removed {coord_root}")
    except Exception as e:  # pragma: no cover — best-effort
        print(f"[cleanup] warning: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
