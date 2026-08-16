"""Detached launcher for the full NeuroMorpho production ingestion.

Spawns ``trace_tools/ingest_neuromorpho_full.py`` as a fully detached child
process (survives the parent shell) and returns immediately. Progress is
written to ``trace_tools/neuromorpho_full_run.log``; the DB itself is the
source of truth for progress polling.

Usage (from Neuro-Agents/):::

    python -m trace_tools.launch_neuromorpho_full
"""

from __future__ import annotations

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(HERE, "neuromorpho_full_run.log")


def main() -> int:
    python = sys.executable
    cmd = [python, "-m", "trace_tools.ingest_neuromorpho_full"]
    flags = 0
    if os.name == "nt":
        flags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    with open(LOG_PATH, "w", encoding="utf-8") as log_fh:
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(HERE),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=flags,
            close_fds=True,
        )
    print(f"launched pid={proc.pid} log={LOG_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
