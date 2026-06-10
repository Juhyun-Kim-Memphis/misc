#!/usr/bin/env python3
"""File IO + metadata benchmark for the Hammerspace PoC.

Runs a short fio workload (sequential bandwidth + random small-IO IOPS)
and a custom metadata-ops workload (create/stat/list/read/delete on many
small files) against the given mount path, then appends one row of
results to a Google Sheet for cross-run comparison.

Design constraints (from VES-1900 PoC):
  - Total wall-clock target: < 3 minutes.
  - Reports in standard storage units: MiB/s for bandwidth, IOPS for
    small random IO, ops/s for metadata ops, ms for mean latency.
  - Leaves no residue on the target FS (the delete phase doubles as
    the meta_delete_ops measurement).
  - Sheet layout: one row per run, columns = metrics + config.

Usage
-----
    fileio-benchmark.py <MOUNT_PATH>

Required env vars
-----------------
    SCENARIO                          Free-form label for this run.
    STORAGECLASS                      Kubernetes StorageClass that backs the
                                      mounted PVC. Key identifier when
                                      comparing storage backends.
    BENCH_SHEET_URL  (or  BENCH_SHEET_ID)
                                      Target Google Sheet.
    GOOGLE_APPLICATION_CREDENTIALS    Path to a service-account JSON key.
                                      The SA email must have Editor access
                                      on the target spreadsheet.

Optional env vars
-----------------
    BENCH_WORKSHEET     Worksheet/tab name (default: "fileio").
                        Auto-created if missing.
    BENCH_BW_SIZE       fio file size for bandwidth tests (default: 1G).
    BENCH_BW_RUNTIME    fio runtime cap for each bandwidth test (default: 15).
    BENCH_IOPS_RUNTIME  fio runtime cap for each random-IO test (default: 12).
    BENCH_IOPS_NUMJOBS  fio numjobs for random-IO tests (default: 4).
    BENCH_IOPS_IODEPTH  fio iodepth for random-IO tests (default: 16).
    BENCH_META_FILES    Total files for the metadata workload (default: 5000).
    BENCH_META_DIRS     Directories for the metadata workload (default: 50).
    BENCH_META_WORKERS  Thread count for metadata ops (default: 16).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials


# ── Defaults & helpers ────────────────────────────────────────────────────


def env(key: str, default: str | None = None) -> str | None:
    v = os.environ.get(key)
    return v if v not in (None, "") else default


def env_int(key: str, default: int) -> int:
    v = env(key)
    return int(v) if v is not None else default


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_sheet_id(url_or_id: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url_or_id)
    return m.group(1) if m else url_or_id


def get_fstype(path: Path) -> str:
    """Walk /proc/mounts to find the fstype of the mount that contains `path`."""
    target = str(path.resolve())
    best_mp, best_fs = "", ""
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 3:
                    continue
                mp, fstype = parts[1], parts[2]
                if target == mp or target.startswith(mp.rstrip("/") + "/"):
                    if len(mp) > len(best_mp):
                        best_mp, best_fs = mp, fstype
    except OSError:
        pass
    return best_fs


# ── fio runner ────────────────────────────────────────────────────────────


def run_fio(workdir: Path, name: str, args: list[str]) -> dict:
    """Run one fio job, return its first-job JSON result."""
    json_path = workdir / f"{name}.json"
    cmd = [
        "fio",
        f"--name={name}",
        f"--directory={workdir}",
        "--output-format=json",
        f"--output={json_path}",
        *args,
    ]
    print(f"  $ fio {' '.join(args)}", flush=True)
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    data = json.loads(json_path.read_text())
    return data["jobs"][0]


def fio_bandwidth(job: dict, rw: str) -> tuple[float, float]:
    """(MiB/s, mean clat ms) for the read or write side of an fio job."""
    side = job[rw]
    mib_s = side["bw_bytes"] / (1024 * 1024)
    lat_ms = side["clat_ns"]["mean"] / 1_000_000
    return mib_s, lat_ms


def fio_iops(job: dict, rw: str) -> tuple[float, float]:
    """(IOPS, mean clat ms) for the read or write side of an fio job."""
    side = job[rw]
    return side["iops"], side["clat_ns"]["mean"] / 1_000_000


# ── Metadata workload ─────────────────────────────────────────────────────


def metadata_phase(label: str, fn, items, workers: int) -> tuple[float, float]:
    """Run `fn(item)` for each item in a thread pool. Return (elapsed_s, ops_per_s)."""
    print(f"  [{label}] {len(items)} ops × {workers} workers ...", end="", flush=True)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        # consume the iterator to surface exceptions
        for _ in ex.map(fn, items):
            pass
    elapsed = time.perf_counter() - t0
    ops_s = len(items) / elapsed if elapsed > 0 else 0.0
    print(f" {elapsed:7.2f}s → {ops_s:10.1f} ops/s")
    return elapsed, ops_s


def run_metadata_test(base: Path, n_files: int, n_dirs: int, workers: int) -> dict:
    base.mkdir(parents=True, exist_ok=False)
    dirs = [base / f"d_{i:04d}" for i in range(n_dirs)]
    for d in dirs:
        d.mkdir()

    files_per_dir = max(1, n_files // n_dirs)
    files = [d / f"f_{i:04d}" for d in dirs for i in range(files_per_dir)]
    payload = b"x" * 4096

    def create(p: Path) -> None:
        with open(p, "wb") as fh:
            fh.write(payload)

    def stat(p: Path) -> None:
        os.stat(p)

    def listdir(d: Path) -> None:
        for _ in os.scandir(d):
            pass

    def read(p: Path) -> None:
        with open(p, "rb") as fh:
            fh.read()

    def delete(p: Path) -> None:
        os.unlink(p)

    _, create_ops = metadata_phase("create", create, files, workers)
    _, stat_ops = metadata_phase("stat  ", stat, files, workers)
    _, list_ops = metadata_phase("list  ", listdir, dirs, workers)
    _, read_ops = metadata_phase("read  ", read, files, workers)
    _, delete_ops = metadata_phase("delete", delete, files, workers)

    # rmdir is small — not in the headline numbers, but do it for cleanup
    for d in dirs:
        d.rmdir()

    return {
        "meta_create_ops": round(create_ops, 1),
        "meta_stat_ops": round(stat_ops, 1),
        "meta_list_ops": round(list_ops, 1),  # ops here = directory readdirs/sec
        "meta_read_ops": round(read_ops, 1),
        "meta_delete_ops": round(delete_ops, 1),
    }


# ── Sheets I/O ────────────────────────────────────────────────────────────


SHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def open_sheet(sheet_id: str, worksheet_name: str, sa_json: str):
    """Open spreadsheet + worksheet, creating the worksheet if absent.

    Fail-fast: any auth or permission error raises before the benchmark runs.
    """
    creds = Credentials.from_service_account_file(sa_json, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=64)
    return ws


def append_row(ws, row: dict) -> None:
    """Append `row` to `ws`, extending the header with any new keys."""
    header = ws.row_values(1)
    if not header:
        header = list(row.keys())
        ws.update(values=[header], range_name="A1")
    else:
        new_keys = [k for k in row.keys() if k not in header]
        if new_keys:
            header = header + new_keys
            ws.update(values=[header], range_name="A1")

    values = [row.get(k, "") for k in header]
    # USER_ENTERED so numbers stay numeric in the sheet
    ws.append_row(values, value_input_option="USER_ENTERED")


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="fio + metadata-ops benchmark; uploads one row to Google Sheets."
    )
    parser.add_argument("mount_path", help="Filesystem path to benchmark (e.g. /mnt/hs)")
    parser.add_argument(
        "--skip-sheet",
        action="store_true",
        help="Run benchmark and print to stdout only (no Sheets upload).",
    )
    args = parser.parse_args()

    mount_path = Path(args.mount_path).resolve()
    if not mount_path.is_dir():
        print(f"ERROR: {mount_path} is not a directory", file=sys.stderr)
        return 2
    if not os.access(mount_path, os.W_OK):
        print(f"ERROR: {mount_path} is not writable", file=sys.stderr)
        return 2

    scenario = env("SCENARIO")
    if not scenario:
        print("ERROR: SCENARIO env var is required", file=sys.stderr)
        return 2
    storageclass = env("STORAGECLASS")
    if not storageclass:
        print("ERROR: STORAGECLASS env var is required", file=sys.stderr)
        return 2

    # Config knobs
    bw_size = env("BENCH_BW_SIZE", "1G")
    bw_runtime = env_int("BENCH_BW_RUNTIME", 15)
    iops_runtime = env_int("BENCH_IOPS_RUNTIME", 12)
    iops_numjobs = env_int("BENCH_IOPS_NUMJOBS", 4)
    iops_iodepth = env_int("BENCH_IOPS_IODEPTH", 16)
    meta_files = env_int("BENCH_META_FILES", 5000)
    meta_dirs = env_int("BENCH_META_DIRS", 50)
    meta_workers = env_int("BENCH_META_WORKERS", 16)

    # Sheets pre-flight (fail-fast)
    ws = None
    if not args.skip_sheet:
        sheet_url_or_id = env("BENCH_SHEET_URL") or env("BENCH_SHEET_ID")
        sa_json = env("GOOGLE_APPLICATION_CREDENTIALS")
        worksheet_name = env("BENCH_WORKSHEET", "fileio")
        if not sheet_url_or_id:
            print(
                "ERROR: BENCH_SHEET_URL or BENCH_SHEET_ID env var is required "
                "(or pass --skip-sheet)",
                file=sys.stderr,
            )
            return 2
        if not sa_json:
            print(
                "ERROR: GOOGLE_APPLICATION_CREDENTIALS env var is required "
                "(or pass --skip-sheet)",
                file=sys.stderr,
            )
            return 2
        sheet_id = parse_sheet_id(sheet_url_or_id)
        print(f"[init] opening sheet {sheet_id} / worksheet '{worksheet_name}'")
        ws = open_sheet(sheet_id, worksheet_name, sa_json)
        print("[init] sheet ready")

    # Collect identifying config now (so it's recorded even if the run aborts late)
    fstype = get_fstype(mount_path)
    config = {
        "timestamp_utc": utc_iso(),
        "scenario": scenario,
        "storageclass": storageclass,
        "fstype": fstype,
        "bw_size": bw_size,
        "bw_runtime_s": bw_runtime,
        "iops_runtime_s": iops_runtime,
        "iops_numjobs": iops_numjobs,
        "iops_iodepth": iops_iodepth,
        "meta_files": meta_files,
        "meta_dirs": meta_dirs,
        "meta_workers": meta_workers,
    }

    # Run benchmarks under a unique subdir; remove it whatever happens.
    run_tag = f"bench-{os.getpid()}-{int(time.time())}"
    workdir = mount_path / run_tag
    workdir.mkdir(parents=True, exist_ok=False)

    metrics: dict[str, float] = {}
    overall_t0 = time.perf_counter()
    try:
        print(f"\n[fio] sequential write — size={bw_size}, bs=1M, runtime≤{bw_runtime}s")
        j = run_fio(
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
        bw_w, lat_w = fio_bandwidth(j, "write")
        print(f"       {bw_w:8.1f} MiB/s   lat_mean={lat_w:.2f} ms")

        print(f"[fio] sequential read  — size={bw_size}, bs=1M, runtime≤{bw_runtime}s")
        j = run_fio(
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
        bw_r, lat_r = fio_bandwidth(j, "read")
        print(f"       {bw_r:8.1f} MiB/s   lat_mean={lat_r:.2f} ms")

        print(
            f"[fio] random write — bs=4k, numjobs={iops_numjobs}, "
            f"iodepth={iops_iodepth}, runtime≤{iops_runtime}s"
        )
        j = run_fio(
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
        iops_w, lat_iw = fio_iops(j, "write")
        print(f"       {iops_w:8.0f} IOPS    lat_mean={lat_iw:.2f} ms")

        print(
            f"[fio] random read  — bs=4k, numjobs={iops_numjobs}, "
            f"iodepth={iops_iodepth}, runtime≤{iops_runtime}s"
        )
        j = run_fio(
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
        iops_r, lat_ir = fio_iops(j, "read")
        print(f"       {iops_r:8.0f} IOPS    lat_mean={lat_ir:.2f} ms")

        # Drop fio artefacts before metadata phase so its dir count is clean
        for p in workdir.iterdir():
            if p.is_file():
                p.unlink()

        print(f"\n[meta] {meta_files} files in {meta_dirs} dirs, {meta_workers} workers")
        meta_metrics = run_metadata_test(
            workdir / "meta",
            n_files=meta_files,
            n_dirs=meta_dirs,
            workers=meta_workers,
        )

        metrics = {
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
    finally:
        # Always clean up — even on failure
        shutil.rmtree(workdir, ignore_errors=True)

    overall_elapsed = time.perf_counter() - overall_t0

    # Pretty stdout summary
    print("\n" + "=" * 64)
    print(f"  Scenario     : {scenario}")
    print(f"  StorageClass : {storageclass}")
    print(f"  Mount        : {mount_path}  ({fstype})")
    print(f"  Wall time    : {overall_elapsed:.1f}s")
    print("-" * 64)
    for k, v in metrics.items():
        print(f"  {k:24s} {v}")
    print("=" * 64)

    if ws is not None:
        row = {**config, **metrics}
        print(f"\n[sheet] appending row to '{ws.title}' (cols={len(row)})")
        append_row(ws, row)
        print("[sheet] done")

    return 0


if __name__ == "__main__":
    sys.exit(main())
