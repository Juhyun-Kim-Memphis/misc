#!/usr/bin/env python3
"""HSTK (Hammerspace Toolkit) metadata-op benchmark for the Hammerspace PoC.

Times a single filesystem operation against a mount point, comparing the
*vanilla* client-side command (e.g. `rm -rf`) with the HSTK *offloaded*
equivalent (e.g. `hs rm -rf`) which executes the op on the Hammerspace
Anvil instead of on the client. The result is one row of measurements
appended to a Google Sheet.

Scope (VES-2106 phase 1):
  - op = `rm` only. The op dispatcher is generalised so `cp` / `rsync`
    can be added later (HSTK exposes `hs cp` and `hs rsync` with the
    same offload semantics; see VES-2106 Future work).
  - One run = one (scenario × set_label × op_variant) triple, one row.
  - Setup time is recorded but not the headline metric — the headline
    is `delete_elapsed_s` / `delete_ops_per_s` over the target tree.

NOTE — canonical home is the **public** misc repo.
    The source of truth for this script lives at
        https://github.com/Juhyun-Kim-Memphis/misc/blob/main/hammerspace-poc/hstk-benchmark.py
    (public, shared with Samwoo / Hammerspace). The bench Job in
    vessl-ai/k8s-manifest-test clones misc and runs the script from
    there. The copy in vessl-ai/k8s-manifest-test/hammerspace-poc/
    is a downstream backup; whenever it is modified, push the
    canonical update to misc/hammerspace-poc/ in the same commit
    cycle so the Job actually picks it up.
    See hammerspace-poc/CLAUDE.md §7 for the sync recipe.

Usage
-----
    hstk-benchmark.py <MOUNT_PATH>

Required env vars
-----------------
    SCENARIO                          Free-form label for this run
                                      (e.g. `vanilla_rm_on_hs`,
                                      `hs_rm_on_hs`, `vanilla_rm_on_nfs_subdir`).
    STORAGECLASS                      Kubernetes StorageClass that backs the
                                      mounted PVC. Key identifier when
                                      comparing storage backends.
    OP_VARIANT                        `vanilla` | `hs`
                                        vanilla → `rm -rf <target>`
                                        hs      → `hs -j rm -rf <target>`
                                                  (JSON output parsed for
                                                   the `rate` self-report)
    SET_LABEL                         `small` | `large`
                                        small → 5000 files / 50 dirs / 4 KiB
                                                (matches VES-2083 meta set)
                                        large → 100000 files / 500 dirs / 4 KiB
                                      Any of BENCH_N_FILES / BENCH_N_DIRS /
                                      BENCH_FILE_SIZE override individual
                                      values below.
    BENCH_SHEET_URL  (or  BENCH_SHEET_ID)
                                      Target Google Sheet.
    GOOGLE_APPLICATION_CREDENTIALS    Path to a service-account JSON key.
                                      The SA email must have Editor access
                                      on the target spreadsheet.

Optional env vars
-----------------
    OP                  Operation to benchmark; default `rm`. Future:
                        `cp`, `rsync`. Unknown values fail fast.
    EXPECTED_NFS_SERVER NFS server (left of `:` in /proc/mounts) that the
                        mount must come from. When set, the script aborts
                        before setup if the actual mount source disagrees.
                        Added after VES-2106 06-12 run measured a phantom
                        share on the decommissioned 2-DSX Anvil because a
                        stale csi-node pod was caching the old endpoint.
    BENCH_WORKSHEET     Worksheet/tab name (default: "hstk").
                        Auto-created if missing.
    BENCH_N_FILES       Override total file count (default: from SET_LABEL).
    BENCH_N_DIRS        Override directory count (default: from SET_LABEL).
    BENCH_FILE_SIZE     Override per-file size in bytes (default: 4096).
    BENCH_SETUP_WORKERS Thread count for the setup phase (default: 16 for
                        `small`, 32 for `large`).
    HS_BIN              Path to the `hs` binary (default: `hs` from PATH).
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


# ── Set-label presets ─────────────────────────────────────────────────────

SET_PRESETS = {
    "small": {
        "n_files": 5_000,
        "n_dirs": 50,
        "file_size": 4096,
        "setup_workers": 16,
    },
    "large": {
        "n_files": 100_000,
        "n_dirs": 500,
        "file_size": 4096,
        "setup_workers": 32,
    },
}

SUPPORTED_OPS = {"rm"}  # extend with "cp", "rsync" when those variants land
SUPPORTED_OP_VARIANTS = {"vanilla", "hs"}


# ── Helpers ───────────────────────────────────────────────────────────────


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


def get_mount_info(path: Path) -> tuple[str, str]:
    """Return (source, fstype) for the longest-prefix mount containing `path`.

    `source` is the device column of /proc/mounts (e.g. `10.197.20.100:/csi-pvc-...`
    for NFS). Empty strings if no entry matches.
    """
    target = str(path.resolve())
    best_mp, best_src, best_fs = "", "", ""
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 3:
                    continue
                src, mp, fstype = parts[0], parts[1], parts[2]
                if target == mp or target.startswith(mp.rstrip("/") + "/"):
                    if len(mp) > len(best_mp):
                        best_mp, best_src, best_fs = mp, src, fstype
    except OSError:
        pass
    return best_src, best_fs


def get_fstype(path: Path) -> str:
    return get_mount_info(path)[1]


def assert_nfs_server(path: Path, expected_server: str) -> None:
    """Fail fast if `path` is not mounted from `expected_server`.

    Guards against the VES-2106 06-12 bench-day pitfall: the Hammerspace CSI
    plugin pods capture HS_ENDPOINT from a Secret via `env: secretKeyRef` at
    pod start; secret rotations don't propagate, so a stale csi-node pod can
    silently mount a PVC from the previous (decommissioned) Anvil instead of
    the intended one. This check pins the actual NFS server (left of `:` in
    /proc/mounts) before any measurement runs.
    """
    src, _ = get_mount_info(path)
    if not src:
        raise RuntimeError(f"{path}: no mount entry in /proc/mounts")
    actual_server = src.split(":", 1)[0] if ":" in src else src
    if actual_server != expected_server:
        raise RuntimeError(
            f"{path}: mounted from {actual_server!r} (full src={src!r}), "
            f"expected {expected_server!r}. Likely stale CSI endpoint cache "
            f"or PVC bound to a phantom share — restart csi-provisioner and "
            f"csi-node DaemonSet, then recreate the PVC."
        )


# ── Setup: create n_files spread across n_dirs ────────────────────────────


def setup_tree(target: Path, n_files: int, n_dirs: int, file_size: int, workers: int) -> float:
    """Create `n_dirs` subdirs under `target`, distribute `n_files` 4 KiB files
    across them. Returns elapsed seconds for the setup phase (not the metric
    of interest, but recorded for context). Pattern matches VES-2083's
    metadata-create phase so the `small` set is directly comparable.
    """
    target.mkdir(parents=True, exist_ok=False)
    dirs = [target / f"d_{i:04d}" for i in range(n_dirs)]
    for d in dirs:
        d.mkdir()

    files_per_dir = max(1, n_files // n_dirs)
    files = [d / f"f_{i:05d}" for d in dirs for i in range(files_per_dir)]
    payload = b"x" * file_size

    def create(p: Path) -> None:
        with open(p, "wb") as fh:
            fh.write(payload)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        # consume to surface exceptions
        for _ in ex.map(create, files):
            pass
    elapsed = time.perf_counter() - t0
    print(
        f"[setup] {len(files)} files in {len(dirs)} dirs "
        f"({workers} workers) → {elapsed:.2f}s"
    )
    return elapsed


# ── Op runners ────────────────────────────────────────────────────────────


def run_vanilla_rm(target: Path) -> tuple[float, float | None]:
    """`rm -rf <target>` wall-clock. Returns (elapsed_s, hs_rate=None)."""
    cmd = ["rm", "-rf", str(target)]
    print(f"[delete] $ {' '.join(cmd)}", flush=True)
    t0 = time.perf_counter()
    subprocess.run(cmd, check=True)
    elapsed = time.perf_counter() - t0
    return elapsed, None


def run_hs_rm(target: Path) -> tuple[float, float | None]:
    """`hs -j rm -rf <target>` wall-clock, parse JSON for the `rate` self-report.

    `hs -j` emits a JSON object with (per the VES-2106 spec & HSTK docs):
        {status, dirents_found, started, finished, rate}
    `rate` is the Anvil-reported ops/s; we record it alongside the wall-clock
    `ops_per_s` so the two views can be compared in the sheet.

    If JSON parsing fails for any reason, the wall-clock measurement is
    still returned and the raw output is echoed to stderr for debugging
    — wall-clock is the headline metric, the self-report is informational.
    """
    hs_bin = env("HS_BIN", "hs")
    cmd = [hs_bin, "-j", "rm", "-rf", str(target)]
    print(f"[delete] $ {' '.join(cmd)}", flush=True)
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0

    stdout = proc.stdout.strip()
    hs_rate: float | None = None
    if stdout:
        try:
            payload = json.loads(stdout)
            # `hs -j` is documented to emit one JSON object per command;
            # if a future version wraps it in a list, take the last entry.
            if isinstance(payload, list) and payload:
                payload = payload[-1]
            if isinstance(payload, dict) and "rate" in payload:
                hs_rate = float(payload["rate"])
            print(f"[delete] hs JSON: {payload}", flush=True)
        except (ValueError, TypeError) as exc:
            print(f"[delete] WARN: could not parse hs -j output: {exc}", file=sys.stderr)
            print(f"[delete] raw stdout: {stdout!r}", file=sys.stderr)
    return elapsed, hs_rate


OP_DISPATCH = {
    # op_name → { op_variant → callable(target) -> (elapsed_s, hs_rate_or_None) }
    "rm": {
        "vanilla": run_vanilla_rm,
        "hs": run_hs_rm,
    },
    # Future: "cp": { "vanilla": run_vanilla_cp, "hs": run_hs_cp }, ...
}


# ── Sheets I/O ────────────────────────────────────────────────────────────


SHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def open_sheet(sheet_id: str, worksheet_name: str, sa_json: str):
    creds = Credentials.from_service_account_file(sa_json, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=32)
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
    ws.append_row(values, value_input_option="USER_ENTERED")


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="HSTK metadata-op benchmark; uploads one row to Google Sheets."
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
    storageclass = env("STORAGECLASS")
    op_variant = env("OP_VARIANT")
    set_label = env("SET_LABEL")
    op = env("OP", "rm")

    for name, value in [
        ("SCENARIO", scenario),
        ("STORAGECLASS", storageclass),
        ("OP_VARIANT", op_variant),
        ("SET_LABEL", set_label),
    ]:
        if not value:
            print(f"ERROR: {name} env var is required", file=sys.stderr)
            return 2

    if op not in SUPPORTED_OPS:
        print(
            f"ERROR: OP={op!r} not supported yet "
            f"(supported: {sorted(SUPPORTED_OPS)})",
            file=sys.stderr,
        )
        return 2
    if op_variant not in SUPPORTED_OP_VARIANTS:
        print(
            f"ERROR: OP_VARIANT={op_variant!r} invalid "
            f"(supported: {sorted(SUPPORTED_OP_VARIANTS)})",
            file=sys.stderr,
        )
        return 2
    if set_label not in SET_PRESETS:
        print(
            f"ERROR: SET_LABEL={set_label!r} unknown "
            f"(supported: {sorted(SET_PRESETS)})",
            file=sys.stderr,
        )
        return 2

    preset = SET_PRESETS[set_label]
    n_files = env_int("BENCH_N_FILES", preset["n_files"])
    n_dirs = env_int("BENCH_N_DIRS", preset["n_dirs"])
    file_size = env_int("BENCH_FILE_SIZE", preset["file_size"])
    setup_workers = env_int("BENCH_SETUP_WORKERS", preset["setup_workers"])

    # `hs` variant needs the binary present; fail fast before setup.
    if op_variant == "hs":
        hs_bin = env("HS_BIN", "hs")
        if shutil.which(hs_bin) is None:
            print(
                f"ERROR: OP_VARIANT=hs but `{hs_bin}` not found in PATH "
                f"(is the image `benchmark-env-ubuntu-22.04-hstk`?)",
                file=sys.stderr,
            )
            return 2

    # Sheets pre-flight (fail-fast — same as fileio-benchmark.py)
    ws = None
    if not args.skip_sheet:
        sheet_url_or_id = env("BENCH_SHEET_URL") or env("BENCH_SHEET_ID")
        sa_json = env("GOOGLE_APPLICATION_CREDENTIALS")
        worksheet_name = env("BENCH_WORKSHEET", "hstk")
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

    expected_server = env("EXPECTED_NFS_SERVER")
    if expected_server:
        assert_nfs_server(mount_path, expected_server)

    mount_src, fstype = get_mount_info(mount_path)
    config = {
        "timestamp_utc": utc_iso(),
        "scenario": scenario,
        "storageclass": storageclass,
        "fstype": fstype,
        "mount_src": mount_src,
        "op": op,
        "op_variant": op_variant,
        "set_label": set_label,
        "n_files": n_files,
        "n_dirs": n_dirs,
        "file_size_bytes": file_size,
    }

    # Unique run dir at the top of the mount so concurrent runs don't collide
    run_tag = f"hstk-bench-{os.getpid()}-{int(time.time())}"
    workdir = mount_path / run_tag
    target = workdir / "target"

    print()
    print("=" * 72)
    print(f"  scenario     : {scenario}")
    print(f"  storageclass : {storageclass}  (fstype={fstype})")
    print(f"  mount_src    : {mount_src or '(not found in /proc/mounts)'}")
    print(f"  op / variant : {op} / {op_variant}")
    print(f"  set_label    : {set_label}  "
          f"({n_files} files in {n_dirs} dirs, {file_size}B each)")
    print(f"  workdir      : {workdir}")
    print("=" * 72)

    metrics: dict = {}
    try:
        setup_elapsed = setup_tree(
            target,
            n_files=n_files,
            n_dirs=n_dirs,
            file_size=file_size,
            workers=setup_workers,
        )

        runner = OP_DISPATCH[op][op_variant]
        delete_elapsed, hs_rate = runner(target)
        delete_ops = n_files / delete_elapsed if delete_elapsed > 0 else 0.0

        metrics = {
            "setup_elapsed_s": round(setup_elapsed, 3),
            "delete_elapsed_s": round(delete_elapsed, 3),
            "delete_ops_per_s": round(delete_ops, 1),
            "hs_rate": round(hs_rate, 1) if hs_rate is not None else "",
        }

        print(
            f"[delete] {delete_elapsed:.2f}s → "
            f"{delete_ops:.1f} ops/s "
            f"(wall-clock; n_files={n_files})"
        )
        if hs_rate is not None:
            print(f"[delete] hs self-report rate = {hs_rate:.1f} ops/s")
    finally:
        # Always clean up — even on failure (target may be partially deleted)
        shutil.rmtree(workdir, ignore_errors=True)

    print("\n" + "-" * 72)
    for k, v in {**config, **metrics}.items():
        print(f"  {k:20s} {v}")
    print("-" * 72)

    if ws is not None:
        row = {**config, **metrics}
        print(f"\n[sheet] appending row to '{ws.title}' (cols={len(row)})")
        append_row(ws, row)
        print("[sheet] done")

    return 0


if __name__ == "__main__":
    sys.exit(main())
