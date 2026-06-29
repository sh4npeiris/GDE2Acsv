"""PLAT-0b CI verifier — confirm a `flet pack` artifact embeds the client OFFLINE.

THROWAWAY spike helper (deleted with the prototype after PLAT-1). Runs on headless
Windows/Linux/macOS CI runners, so it does NOT depend on a visible window.

Signal (display-independent): move ~/.flet aside AND set FLET_CLIENT_URL to an
unreachable host, then launch the packed artifact. flet_desktop.ensure_client_cached()
resolves cache -> bundled archive -> download; with the cache gone and download
impossible, the ONLY way ~/.flet/client can reappear is extraction of the EMBEDDED
bundle. So "client cache repopulated" == "client is bundled offline".

Usage:  python ci_verify_pack.py <dist_dir> <base_name>
Exit 0 = offline embedding confirmed; 1 = not confirmed (see captured output).
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

DIST = Path(sys.argv[1])
NAME = sys.argv[2]
OSN = platform.system()  # Windows / Linux / Darwin
HOME = Path.home()
FLET_HOME = HOME / ".flet"
FLET_BAK = HOME / ".flet_ci_bak"


def resolve_artifact() -> str | None:
    cands = [
        DIST / f"{NAME}.exe",
        DIST / NAME,
        DIST / f"{NAME}.app" / "Contents" / "MacOS" / NAME,
    ]
    for c in cands:
        if c.exists():
            return str(c)
    return None


def dump_tree(p: Path, label: str):
    print(f"--- {label}: {p} ---")
    if not p.exists():
        print("   (absent)")
        return
    for root, _dirs, files in os.walk(p):
        depth = Path(root).relative_to(p).parts
        if len(depth) > 2:
            continue
        for f in sorted(files)[:12]:
            print(f"   {Path(root).relative_to(p)}/{f}")


def main() -> int:
    print(f"== PLAT-0b verify on {OSN} ==")
    print(f"dist dir contents:")
    dump_tree(DIST, "dist")

    art = resolve_artifact()
    if not art:
        print(f"FAIL: no artifact found under {DIST} for base name '{NAME}'")
        return 1
    print(f"artifact: {art}")
    print(f"size: {os.path.getsize(art) / 1e6:.1f} MB")

    # Windows-only: no-console is a real PE-subsystem property (2 = GUI, 3 = console).
    if OSN == "Windows":
        try:
            import pefile

            pe = pefile.PE(art, fast_load=True)
            sub = pe.OPTIONAL_HEADER.Subsystem
            pe.close()
            print(f"PE subsystem = {sub} ({'WINDOWS_GUI (no console)' if sub == 2 else 'CONSOLE'})")
            if sub != 2:
                print("FAIL: expected GUI subsystem (no console)")
                return 1
        except Exception as e:
            print(f"(pefile check skipped: {e})")

    # Offline embedding check.
    moved = False
    if FLET_HOME.exists():
        if FLET_BAK.exists():
            shutil.rmtree(FLET_BAK, ignore_errors=True)
        shutil.move(str(FLET_HOME), str(FLET_BAK))
        moved = True
        print(f"moved {FLET_HOME} aside (forcing the bundle path)")

    env = {**os.environ, "FLET_CLIENT_URL": "http://127.0.0.1:9"}  # unreachable
    proc = None
    ok = False
    try:
        proc = subprocess.Popen([art], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + 45
        while time.time() < deadline:
            if (FLET_HOME / "client").exists():
                ok = True
                break
            if proc.poll() is not None:
                if (FLET_HOME / "client").exists():
                    ok = True
                break
            time.sleep(0.5)
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        # surface the artifact's own output — diagnostic if macOS embedding differs
        try:
            out = proc.communicate(timeout=5)[0] if proc else ""
        except Exception:
            out = ""
        if out:
            print("--- artifact output (first 40 lines) ---")
            for line in out.splitlines()[:40]:
                print(f"   {line}")
        repem = (FLET_HOME / "client").exists()
        if moved:
            if FLET_HOME.exists():
                shutil.rmtree(FLET_HOME, ignore_errors=True)
            shutil.move(str(FLET_BAK), str(FLET_HOME))
            print(f"restored {FLET_HOME}")

    print(f"\noffline_embedding (cache repopulated from bundle, download impossible): {'PASS' if ok else 'FAIL'}")
    if not ok:
        print("NOTE: on macOS a FAIL may mean `flet pack` did not embed the client → use `flet build macos`.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
