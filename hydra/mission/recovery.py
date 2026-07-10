from __future__ import annotations

from hydra.mission.mission_state import MissionPaths, connect_state, get_kv, set_kv


def increment_crash_count(paths: MissionPaths) -> int:
    conn = connect_state(paths)
    try:
        count = int(get_kv(conn, "crash_count", 0)) + 1
        set_kv(conn, "crash_count", count)
        return count
    finally:
        conn.close()


def mark_clean_shutdown(paths: MissionPaths) -> None:
    conn = connect_state(paths)
    try:
        set_kv(conn, "last_shutdown", "clean")
    finally:
        conn.close()

