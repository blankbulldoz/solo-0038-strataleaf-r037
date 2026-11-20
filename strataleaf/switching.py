"""Resumable in-place environment switch for verified database archives.

The switch workflow keeps the receiving environment on its old database until a
freshly restored copy has been fully built and read-verified inside a sibling
staging directory.  The actual cutover is a sequence of same-directory renames,
which makes every phase recoverable if the process exits halfway through.

Staging layout next to the target database (``live.db`` -> ``live.db.switch/``):

* ``lock``       -- an ``flock`` gate serializing switch attempts; the kernel
  releases it automatically when the process exits, so no stale lock survives.
* ``state.json`` -- the persisted switch state machine (atomic replace).
* ``staged.db``  -- the verified new database, checkpointed to a single file.
* ``retired/``   -- the old database files moved aside during cutover.

Phases: ``preparing`` -> ``prepared`` -> ``committing`` -> ``committed``.
The target database is never touched before ``committing``; a failed prepare
therefore cannot damage the running environment.
"""
import contextlib
import fcntl
import json
import os
import secrets
import shutil
import sqlite3
import tempfile
import uuid
from pathlib import Path

from .archive import inspect_archive
from .codec import digest, loads
from .errors import Conflict, DatabaseError
from .storage import Storage

STAGING_SUFFIX = ".switch"
STATE_NAME = "state.json"
LOCK_NAME = "lock"
STAGED_NAME = "staged.db"
RETIRED_NAME = "retired"
SIDECAR_SUFFIXES = ("", "-wal", "-shm")
REQUIRED_META = {"head", "floor", "format", "database_id",
                 "cursor_secret", "mode", "upstream"}


def _staging_paths(target):
    target = Path(target).resolve()
    staging = target.parent / (target.name + STAGING_SUFFIX)
    return target, staging


def _fsync_directory(directory):
    descriptor = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_state(staging):
    state_path = staging / STATE_NAME
    if not state_path.exists():
        return None
    try:
        with open(state_path, "r", encoding="utf-8") as stream:
            state = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise DatabaseError("switch state is unreadable; abort the switch before retrying") from exc
    if not isinstance(state, dict) or state.get("format") != 1 or not state.get("target"):
        raise DatabaseError("switch state is malformed; abort the switch before retrying")
    return state


def _write_state(staging, state):
    state_path = staging / STATE_NAME
    fd, temporary = tempfile.mkstemp(prefix=".state-", dir=str(staging))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(state, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, state_path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    _fsync_directory(staging)


@contextlib.contextmanager
def _switch_lock(staging):
    staging.mkdir(parents=True, exist_ok=True)
    with open(staging / LOCK_NAME, "a+") as lock_stream:
        fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)


def _same_parameters(state, source, expected_sha256, follower):
    return (state.get("source") == str(source)
            and state.get("expected_sha256") == expected_sha256
            and bool(state.get("follower")) == bool(follower))


def _fingerprint(connection, revision):
    content = []
    collections = [row[0] for row in connection.execute(
        "SELECT name FROM collections ORDER BY name")]
    for collection in collections:
        rows = connection.execute("""SELECT v.collection,v.key,v.rev,v.body,v.expires
          FROM versions v JOIN
          (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? GROUP BY key) latest
          ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
          WHERE v.body IS NOT NULL ORDER BY v.key""",
          (collection, revision, collection)).fetchall()
        content.extend({"collection": row[0], "key": row[1], "revision": row[2],
                        "document": loads(row[3]), "expires_at": row[4]} for row in rows)
    return {"revision": revision, "documents": len(content), "sha256": digest(content)}


def _fingerprint_file(path, revision):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?immutable=1", uri=True)
    try:
        return _fingerprint(connection, revision)
    finally:
        connection.close()


def _build_staged(source, staged, receipt, follower):
    """Restore the archive into the staging file and apply identity rules."""
    for suffix in SIDECAR_SUFFIXES:
        Path(str(staged) + suffix).unlink(missing_ok=True)
    original = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    target = None
    try:
        target = sqlite3.connect(str(staged))
        original.backup(target)
        target.execute("UPDATE meta SET value=? WHERE key='mode'",
                       ("replica" if follower else "primary",))
        target.execute("UPDATE meta SET value=? WHERE key='upstream'",
                       (receipt["database_id"] if follower else "",))
        if not follower:
            target.execute("UPDATE meta SET value=? WHERE key='database_id'", (str(uuid.uuid4()),))
            target.execute("UPDATE meta SET value=? WHERE key='cursor_secret'", (secrets.token_hex(32),))
        target.execute("DELETE FROM pins")
        target.execute("DELETE FROM consumers")
        target.commit()
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        if target is not None:
            target.close()
        original.close()
    for suffix in ("-wal", "-shm"):
        Path(str(staged) + suffix).unlink(missing_ok=True)


def _read_verify(staged, receipt, follower, source_fingerprint):
    """Open the staged database read-only and prove it is intact and readable."""
    connection = sqlite3.connect(staged.resolve().as_uri() + "?immutable=1", uri=True)
    try:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise DatabaseError("staged database failed SQLite integrity check")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise DatabaseError("staged database has broken references")
        meta = dict(connection.execute("SELECT key,value FROM meta"))
        if not REQUIRED_META.issubset(meta) or meta["format"] != "1":
            raise DatabaseError("staged database metadata is unsupported")
        head, floor = int(meta["head"]), int(meta["floor"])
        if head != receipt["revision"] or floor != receipt["floor"]:
            raise DatabaseError("staged database revision watermarks do not match the archive")
        revisions = [row[0] for row in connection.execute(
            "SELECT rev FROM commits WHERE rev>? ORDER BY rev", (floor,))]
        if revisions != list(range(floor + 1, head + 1)):
            raise DatabaseError("staged database retained commit range is not contiguous")
        expected_mode = "replica" if follower else "primary"
        if meta["mode"] != expected_mode:
            raise DatabaseError("staged database mode was not installed correctly")
        if follower and meta["upstream"] != receipt["database_id"]:
            raise DatabaseError("staged replica lost its upstream binding")
        if not follower and meta["database_id"] == receipt["database_id"]:
            raise DatabaseError("staged primary kept the archived database identity")
        staged_fingerprint = _fingerprint(connection, head)
    finally:
        connection.close()
    if staged_fingerprint != source_fingerprint:
        raise DatabaseError("staged database logical fingerprint differs from the archive")
    return staged_fingerprint


def _open_owners(target):
    """Return descriptions of every holder that still has the target open."""
    watched = {}
    for suffix in SIDECAR_SUFFIXES:
        candidate = Path(str(target) + suffix)
        if candidate.exists():
            watched[str(candidate)] = suffix or "main"
    owners = []
    if Storage.is_path_open(str(target)):
        owners.append("strataleaf Storage handle open in this process")
    proc = Path("/proc")
    if not proc.exists():
        raise DatabaseError(
            "switch safety guard requires /proc to prove the SQLite files are not open; "
            "refusing to overwrite an environment that may still be in use")
    inodes = {}
    for path in watched:
        try:
            inodes[os.stat(path).st_ino] = path
        except FileNotFoundError:
            continue
    for pid_directory in proc.glob("[0-9]*"):
        try:
            descriptors = list((pid_directory / "fd").iterdir())
        except (PermissionError, FileNotFoundError):
            continue
        for descriptor in descriptors:
            try:
                link = os.readlink(descriptor)
            except OSError:
                continue
            candidate = link[: -len(" (deleted)")] if link.endswith(" (deleted)") else link
            if candidate in watched:
                owners.append("pid {} holds open fd for {} ({})".format(
                    pid_directory.name, watched[candidate], link))
    try:
        lock_lines = (proc / "locks").read_text().splitlines()[1:]
    except OSError:
        lock_lines = []
    for line in lock_lines:
        parts = line.split()
        if len(parts) >= 7 and parts[1] in ("POSIX", "FLOCK"):
            inode_text = parts[5].rsplit(":", 1)[-1]
            if inode_text.isdigit() and int(inode_text) in inodes:
                owners.append("pid {} holds an SQLite {} lock on {}".format(
                    parts[4], parts[1], inodes[int(inode_text)]))
    return list(dict.fromkeys(owners))


def _guard_target_unused(target):
    owners = _open_owners(target)
    if owners:
        raise Conflict("refusing to switch while the target SQLite files are still open: "
                       + "; ".join(owners))


def _sweep_temp_files(staging):
    for leftover in staging.glob(".state-*"):
        leftover.unlink(missing_ok=True)


def prepare_switch(archive, target, expected_sha256=None, follower=False, _state=None):
    """Build and verify the new database without touching the live target.

    ``_state`` is an existing in-progress state object supplied by the crash
    recovery path so the rebuild reuses the same staging directory; in that
    case the caller already holds the switch lock.
    """
    source = Path(archive).resolve()
    target, staging = _staging_paths(target)
    staging.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def _gate():
        if _state is None:
            with _switch_lock(staging):
                yield
        else:
            yield

    with _gate():
        _sweep_temp_files(staging)
        state = _state if _state is not None else _load_state(staging)
        if state is None:
            state = {"format": 1, "target": str(target), "source": str(source),
                     "expected_sha256": expected_sha256, "follower": bool(follower),
                     "phase": "preparing", "attempts": 0}
        else:
            if state.get("phase") == "committed":
                if _same_parameters(state, source, expected_sha256, follower):
                    return state
                raise Conflict("a finalized switch is still retained; "
                               "call Archives.switch_cleanup before starting another")
            if not _same_parameters(state, source, expected_sha256, follower):
                raise Conflict("a switch for a different archive is already staged; "
                               "abort it before preparing another")
        state["phase"] = "preparing"
        state["attempts"] = int(state.get("attempts", 0)) + 1
        _write_state(staging, state)
        receipt = inspect_archive(source)
        if expected_sha256 is not None and expected_sha256 != receipt["sha256"]:
            raise DatabaseError("archive checksum mismatch")
        staged = staging / STAGED_NAME
        _build_staged(source, staged, receipt, follower)
        source_fingerprint = _fingerprint_file(source, receipt["revision"])
        staged_fingerprint = _read_verify(staged, receipt, follower, source_fingerprint)
        state["phase"] = "prepared"
        state["receipt"] = {**receipt, "mode": "replica" if follower else "primary"}
        state["fingerprint"] = staged_fingerprint
        _write_state(staging, state)
        return state


def _move_database_aside(target, retired_dir):
    retired_dir.mkdir(parents=True, exist_ok=True)
    moved = []
    for suffix in SIDECAR_SUFFIXES:
        candidate = Path(str(target) + suffix)
        if candidate.exists():
            destination = retired_dir / candidate.name
            os.rename(candidate, destination)
            moved.append(str(destination))
    return moved


def _restore_retired(target, retired_dir):
    for candidate in sorted(retired_dir.iterdir()):
        os.rename(candidate, target.parent / candidate.name)
    shutil.rmtree(retired_dir, ignore_errors=True)


def _finalize_committed(staging, state, keep_retired):
    retired_dir = staging / RETIRED_NAME
    retained = sorted(str(path) for path in retired_dir.glob("*")) if retired_dir.exists() else []
    if keep_retired:
        state["retired"] = retained
        _write_state(staging, state)
        return {**state["receipt"], "path": state["target"], "phase": "committed",
                "retired": retained, "staging": str(staging)}
    shutil.rmtree(retired_dir, ignore_errors=True)
    shutil.rmtree(staging, ignore_errors=True)
    return {**state["receipt"], "path": state["target"], "phase": "committed",
            "retired": "removed", "staging": None}


def commit_switch(target, keep_retired=False):
    """Cut over a prepared switch; recoverable and idempotent after crashes."""
    target, staging = _staging_paths(target)
    with _switch_lock(staging):
        _sweep_temp_files(staging)
        state = _load_state(staging)
        if state is None:
            raise DatabaseError("no staged switch exists for this target; call switch_prepare first")
        if state["phase"] == "committed":
            return _finalize_committed(staging, state, keep_retired)
        staged = staging / STAGED_NAME
        retired_dir = staging / RETIRED_NAME
        if state["phase"] == "committing":
            # Recovery from a process exit during the rename sequence. No guard
            # is re-run for a cutover that had already moved the live database.
            if target.exists() and retired_dir.exists() and any(retired_dir.iterdir()):
                # Rename sequence completed; only the state update was lost.
                staged_fingerprint = _fingerprint_file(target, int(state["receipt"]["revision"]))
                if staged_fingerprint != state["fingerprint"]:
                    raise DatabaseError(
                        "interrupted switch left unexpected content at the target; "
                        "manual operator recovery is required")
                state["phase"] = "committed"
                _write_state(staging, state)
                return _finalize_committed(staging, state, keep_retired)
            if not target.exists() and retired_dir.exists() and not staged.exists():
                # Both moves happened but neither persisted state recorded it;
                # the retired set is the only complete database, so roll back.
                _restore_retired(target, retired_dir)
                state["phase"] = "preparing"
                _write_state(staging, state)
                state = prepare_switch(state["source"], state["target"],
                                       state.get("expected_sha256"),
                                       state.get("follower", False), _state=state)
                staged = staging / STAGED_NAME
            elif not target.exists() and staged.exists():
                # Old database moved aside; the staged copy still has to be
                # installed. Fall through to the final rename below.
                pass
            elif target.exists() and not retired_dir.exists():
                # No rename had happened before the exit: restart the cutover
                # from the authorization step, including the open-file guard.
                if not staged.exists():
                    state["phase"] = "preparing"
                    _write_state(staging, state)
                    state = prepare_switch(state["source"], state["target"],
                                           state.get("expected_sha256"),
                                           state.get("follower", False), _state=state)
                    staged = staging / STAGED_NAME
                _guard_target_unused(target)
            else:
                raise DatabaseError("interrupted switch state is not recoverable automatically")
        elif state["phase"] != "prepared":
            raise DatabaseError("staged switch is not ready to commit; rerun the switch entry")
        else:
            _guard_target_unused(target)
        if not staged.exists():
            raise DatabaseError("verified staged database is missing; rerun the switch entry to rebuild it")
        state["phase"] = "committing"
        _write_state(staging, state)
        retired = _move_database_aside(target, retired_dir)
        if target.exists():
            raise DatabaseError("target reappeared while the old database was being retired")
        os.rename(staged, target)
        _fsync_directory(target.parent)
        state["phase"] = "committed"
        state["retired"] = retired
        _write_state(staging, state)
        return _finalize_committed(staging, state, keep_retired)


def abort_switch(target):
    """Cancel a switch that has not committed; the live target is never touched."""
    target, staging = _staging_paths(target)
    if not staging.exists():
        return {"target": str(target), "phase": "absent"}
    with _switch_lock(staging):
        state = _load_state(staging)
        if state is not None and state["phase"] == "committed":
            raise Conflict("a committed switch cannot be aborted; use switch_cleanup instead")
        if target.exists() and (staging / RETIRED_NAME).exists():
            _restore_retired(target, staging / RETIRED_NAME)
        shutil.rmtree(staging, ignore_errors=True)
        return {"target": str(target), "phase": "aborted"}


def cleanup_switch(target):
    """Remove retained retired files and staging state from a finalized switch."""
    target, staging = _staging_paths(target)
    if not staging.exists():
        return {"target": str(target), "phase": "absent"}
    with _switch_lock(staging):
        state = _load_state(staging)
        if state is not None and state["phase"] != "committed":
            raise Conflict("only a committed switch can be cleaned up; abort or commit it first")
        shutil.rmtree(staging, ignore_errors=True)
        return {"target": str(target), "phase": "cleaned"}


def switch_status(target):
    """Observe the persisted switch state without modifying anything."""
    target, staging = _staging_paths(target)
    if not staging.exists():
        return {"target": str(target), "phase": "absent", "staging": None}
    with _switch_lock(staging):
        state = _load_state(staging)
        if state is None:
            return {"target": str(target), "phase": "absent", "staging": str(staging)}
        retired_dir = staging / RETIRED_NAME
        state = dict(state)
        state["staging"] = str(staging)
        state["staged_exists"] = (staging / STAGED_NAME).exists()
        state["retired_files"] = sorted(path.name for path in retired_dir.glob("*")) \
            if retired_dir.exists() else []
        return state


def run_switch(archive, target, expected_sha256=None, follower=False, keep_retired=False):
    """One-call, re-openable entry: prepare/verify, then cut over."""
    prepare_switch(archive, target, expected_sha256, follower)
    return commit_switch(target, keep_retired)
