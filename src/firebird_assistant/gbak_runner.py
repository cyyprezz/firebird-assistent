from __future__ import annotations
import subprocess, shutil, os, sys
from typing import Optional
from .fb_utils import detect_server_version, server_major

class GbakError(RuntimeError):
    pass

def _is_windows_local_path(p: str) -> bool:
    return bool(p) and len(p) >= 3 and p[1:3] == ":\\"

def _normalize_dsn_for_gbak(dsn: str) -> str:
    """
    Ensure gbak uses TCP when a bare local Windows path was provided.
    """
    # If it's a classic DSN with host:... return as is
    if ":" in dsn and not _is_windows_local_path(dsn):
        return dsn
    # If it's a plain Windows path like C:\path\db.fdb, prefix localhost:
    if _is_windows_local_path(dsn):
        return f"localhost:{dsn}"
    return dsn

def _candidate_paths_for_major(major: int) -> list[str]:
    paths: list[str] = []
    # Windows standard installs
    if os.name == "nt":
        base = r"C:\Program Files\Firebird"
        mapping = {
            5: os.path.join(base, "Firebird_5_0", "bin", "gbak.exe"),
            4: os.path.join(base, "Firebird_4_0", "bin", "gbak.exe"),
            3: os.path.join(base, "Firebird_3_0", "bin", "gbak.exe"),
            2: os.path.join(base, "Firebird_2_5", "bin", "gbak.exe"),
        }
        # prefer exact major, then newer, then older as fallback
        ordered = [major, 5, 4, 3, 2]
        seen = set()
        for m in ordered:
            p = mapping.get(m)
            if p and p not in seen:
                paths.append(p); seen.add(p)
    else:
        # Linux/macOS candidates
        paths = [
            "/opt/firebird/bin/gbak",
            "/usr/bin/gbak",
            "/usr/local/firebird/bin/gbak",
            "/usr/local/bin/gbak",
        ]
    return paths

def find_gbak(user_path: Optional[str] = None, *, auto_major: Optional[int] = None) -> str:
    """
    1) explicit user path wins
    2) if auto_major is provided, try version-specific install folders
    3) else: PATH (shutil.which)
    """
    if user_path:
        p = os.path.expandvars(os.path.expanduser(user_path))
        if os.path.isfile(p):
            return p
        raise GbakError(f"Provided --gbak-path not found: {p}")

    if auto_major:
        for p in _candidate_paths_for_major(auto_major):
            if os.path.isfile(p):
                return p

    gbak = shutil.which("gbak")
    if gbak:
        return gbak

    raise GbakError("gbak executable not found. Provide --gbak-path or install Firebird tools.")

def run_backup(
    dsn: str,
    output: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    gbak_path: Optional[str] = None,
    auto_select: bool = True,
    compress: bool = False,
    verbose: bool = True,
    timeout: Optional[int] = None,
) -> None:
    """
    Run gbak backup. If auto_select=True, detect server major version and choose matching gbak.
    """
    major: Optional[int] = None
    if auto_select:
        ver = detect_server_version(dsn, user=user, password=password)
        major = server_major(ver) or None

    gbak = find_gbak(gbak_path, auto_major=major)

    # Build source: prefer TCP DSN if a bare local Windows path was given
    source = _normalize_dsn_for_gbak(dsn)
    # If dsn contains "database=" key, convert to classic form if host present
    if "database=" in dsn.lower() and "host=" in dsn.lower():
        # For simplicity we still allow classic "host:path" in CLI. Prefer that format.
        pass

    cmd = [gbak, "-b"]  # -backup
    if verbose:
        cmd.append("-v")
    if compress:
        # only supported by newer gbak; if it fails, user can disable
        cmd.append("-zip")
    if user:
        cmd += ["-user", user]
    if password:
        cmd += ["-password", password]
    cmd += [source, output]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as e:
        raise GbakError(f"gbak not found: {e}")
    except subprocess.TimeoutExpired:
        raise GbakError("gbak timed out.")

    if proc.returncode != 0:
        raise GbakError(
            f"gbak failed (exit {proc.returncode}).\n"
            f"CMD: {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

def run_restore(
    backup_file: str,
    dsn: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    gbak_path: Optional[str] = None,
    auto_select: bool = True,
    replace: bool = False,
    verbose: bool = True,
    timeout: Optional[int] = None,
) -> None:
    """
    Restore a Firebird database from a backup (.fbk) file using gbak.

    If auto_select is True, detect the server major version for the target DSN and
    choose a matching gbak binary. If replace is True, use gbak -rep to replace an
    existing database; otherwise use gbak -c to create a new database.
    """
    major: Optional[int] = None
    if auto_select:
        try:
            ver = detect_server_version(dsn, user=user, password=password)
            major = server_major(ver) or None
        except Exception:
            major = None

    gbak = find_gbak(gbak_path, auto_major=major)

    mode_flag = "-rep" if replace else "-c"
    cmd = [gbak, mode_flag]
    if verbose:
        cmd.append("-v")
    if user:
        cmd += ["-user", user]
    if password:
        cmd += ["-password", password]
    dest = _normalize_dsn_for_gbak(dsn)
    cmd += [backup_file, dest]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as e:
        raise GbakError(f"gbak not found: {e}")
    except subprocess.TimeoutExpired:
        raise GbakError("gbak timed out.")

    if proc.returncode != 0:
        raise GbakError(
            f"gbak failed (exit {proc.returncode}).\n"
            f"CMD: {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
