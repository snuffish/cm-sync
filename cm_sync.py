#!/usr/bin/env python3
"""cm-sync: sync claude-mem memory between devices through a git repo.

Each device publishes only the rows it created to <repo>/claude-mem/devices/<id>/,
plus tombstones for rows it deleted. Other devices import them through the
claude-mem worker (POST /api/import, DELETE routes), never into SQLite directly.

The one exception is work state (claude-mem's "My Work"): no worker route keeps
its timestamps, so it is inserted the way claude-mem's own cloud sync does,
with origin_device_id / origin_local_id.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

FORMAT = 1
BATCH_BYTES = 4 * 1024 * 1024
DEVICES_DIR = Path("claude-mem") / "devices"
LEGACY_EXPORT_DIR = Path("claude-mem") / "macbook-pro"
DEFAULT_REMOTE = "git@github.com:your-username/claude-memory.git"
KINDS = ("sessions", "observations", "summaries", "prompts", "work_state")
# Kinds that go through POST /api/import
IMPORT_KINDS = ("sessions", "observations", "summaries", "prompts")

# Columns /api/import reads per table
COLUMNS = {
    "sessions": (
        "content_session_id", "memory_session_id", "project", "platform_source", "user_prompt",
        "custom_title", "started_at", "started_at_epoch", "completed_at", "completed_at_epoch", "status",
    ),
    "observations": (
        "memory_session_id", "project", "text", "type", "title", "subtitle", "facts", "narrative",
        "concepts", "files_read", "files_modified", "prompt_number", "discovery_tokens",
        "agent_type", "agent_id", "created_at", "created_at_epoch",
    ),
    "summaries": (
        "memory_session_id", "project", "request", "investigated", "learned", "completed",
        "next_steps", "files_read", "files_edited", "notes", "prompt_number", "discovery_tokens",
        "created_at", "created_at_epoch",
    ),
    "prompts": (
        "content_session_id", "platform_source", "prompt_number", "prompt_text", "created_at", "created_at_epoch",
    ),
    "work_state": ("local_id", "project", "list_name", "fields", "created_at", "created_at_epoch"),
}

# Same columns the worker dedups on in importSdkSession / importObservation / importSessionSummary / importUserPrompt
KEY_COLUMNS = {
    "sessions": ("platform_source", "content_session_id"),
    "observations": (
        "memory_session_id", "text", "type", "title", "subtitle", "facts", "narrative", "concepts",
        "files_read", "files_modified", "prompt_number", "agent_type", "agent_id", "created_at_epoch",
    ),
    "summaries": (
        "memory_session_id", "request", "investigated", "learned", "completed", "next_steps",
        "files_read", "files_edited", "notes", "prompt_number", "created_at_epoch",
    ),
    "prompts": ("content_session_id", "prompt_number"),
    "work_state": ("local_id", "project", "list_name", "fields", "created_at_epoch"),
}

SCAN_COLUMNS = {
    "sessions": ("user_prompt", "custom_title"),
    "observations": ("text", "title", "subtitle", "facts", "narrative", "concepts", "files_read", "files_modified"),
    "summaries": ("request", "investigated", "learned", "completed", "next_steps", "notes"),
    "prompts": ("prompt_text",),
    "work_state": ("list_name", "fields"),
}

SECRET_PATTERNS = {
    "private-key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "aws-access-key": re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    "github-token": re.compile(r"\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b|\bgithub_pat_[A-Za-z0-9_]{40,}\b"),
    "slack-token": re.compile(r"\bxox[abpr]-[A-Za-z0-9-]{10,}\b"),
    "anthropic-key": re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b"),
    "api-key": re.compile(r"\bsk-[A-Za-z0-9_-]{32,}\b"),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
    "azure-key": re.compile(r"(AccountKey|SharedAccessKey)=[A-Za-z0-9+/=]{20,}"),
    "connection-string-password": re.compile(r"(?i)\b(password|pwd)=[^;\s\"']{6,}"),
    "assigned-secret": re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key)\b\s*[:=]\s*[\"']?[^\s\"'<>]{8,}"),
}

# Rows derived from Markdown memory files; those files sync on their own
DERIVED_SOURCES = ('"source":"memory-file"', '"source":"memory-import"')

# work_state_entries columns the direct insert writes; claude-mem before v15 lacks the origin ones
WORK_STATE_COLUMNS = {
    "project", "list_name", "fields", "created_at", "created_at_epoch",
    "synced_at", "origin_device_id", "origin_local_id", "sync_rev",
}

# claude-mem's My Rules: rules are work-state tasks in this list; MY-RULES.md holds the "on" ones these set
RULES_LIST = "my-rules"
RULES_ACTIVE_BY = ("owner", "approved")

QUIET = False
WINDOWS = os.name == "nt"


class CmSyncError(Exception):
    pass


STYLES = {"bold": 1, "dim": 2, "red": 31, "green": 32, "yellow": 33, "blue": 34, "magenta": 35, "cyan": 36}
STEPS = {
    "Pulled": ("↓", "cyan"),
    "Imported": ("↓", "blue"),
    "Deleted": ("✗", "yellow"),
    "Exported": ("↑", "magenta"),
    "Published": ("↑", "green"),
    "Rules": ("↻", "green"),
}


def colored(stream):
    """ANSI colours only on a terminal and never with NO_COLOR; on Windows only where the terminal announces itself."""
    if "NO_COLOR" in os.environ or os.environ.get("TERM") == "dumb" or not stream.isatty():
        return False
    return not WINDOWS or bool(os.environ.get("WT_SESSION") or os.environ.get("TERM"))


def paint(text, *styles, stream=None):
    if not styles or not text or not colored(stream or sys.stdout):
        return str(text)
    return f"\033[{';'.join(str(STYLES[s]) for s in styles)}m{text}\033[0m"


def info(msg):
    if not QUIET:
        print(msg)


def warn(msg, style="yellow"):
    print(paint(msg, style, stream=sys.stderr), file=sys.stderr)


def step(label, text):
    """One line of sync progress: a coloured arrow and label, padded to one column."""
    glyph, color = STEPS[label]
    info(paint(f"{glyph} {label + ':':<12}", color, "bold") + text)


def counted(*parts):
    """`(n, noun)` pairs as `n noun · n noun`, leaving out the zero counts."""
    shown = [f"{paint(n, 'bold')} {noun}" for n, noun in parts if n]
    return paint(" · ", "dim").join(shown) or paint("nothing", "dim")


# ------------------------------------------------------------------ config


class Config:
    def __init__(self, repo=None, state_dir=None):
        home = Path.home()
        default_data = home / ".claude-mem"
        data_dir = os.environ.get("CLAUDE_MEM_DATA_DIR")
        if not data_dir:
            data_dir = read_json(default_data / "settings.json", {}).get("CLAUDE_MEM_DATA_DIR") or str(default_data)
        self.data_dir = Path(data_dir).expanduser()
        self.settings_path = self.data_dir / "settings.json"
        self.settings = read_json(self.settings_path, {})
        self.db_path = self.data_dir / "claude-mem.db"
        self.rules_path = self.data_dir / "MY-RULES.md"
        self.claude_md = Path(os.environ.get("CLAUDE_CONFIG_DIR") or home / ".claude") / "CLAUDE.md"

        host = self.setting("CLAUDE_MEM_WORKER_HOST") or "127.0.0.1"
        if host == "localhost":
            host = "127.0.0.1"
        # claude-mem uses uid 77 where there is none (Windows)
        getuid = getattr(os, "getuid", None)
        uid = getuid() if getuid else 77
        port = self.setting("CLAUDE_MEM_WORKER_PORT") or str(37700 + uid % 100)
        self.worker_url = f"http://{host}:{port}"

        self.repo = Path(repo or os.environ.get("CM_SYNC_REPO") or home / ".claude-memory").expanduser()
        self.remote = os.environ.get("CM_SYNC_REMOTE") or DEFAULT_REMOTE
        xdg_state = os.environ.get("XDG_STATE_HOME") or str(home / ".local" / "state")
        self.state_dir = Path(state_dir or os.environ.get("CM_SYNC_STATE_DIR") or Path(xdg_state) / "cm-sync").expanduser()

    def setting(self, name):
        return os.environ.get(name) or self.settings.get(name)

    @property
    def redaction_enabled(self):
        return str(self.setting("CLAUDE_MEM_REDACT_ENABLED")).lower() == "true"

    @property
    def devices_root(self):
        return self.repo / DEVICES_DIR


def read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def tilde(path):
    """`path` with the home directory shown as `~`, in forward slashes so it reads the same on Windows."""
    path, home = Path(path), Path.home()
    try:
        return "~/" + path.relative_to(home).as_posix()
    except ValueError:
        return path.as_posix()


def write_text_atomic(path, text, mode=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp{os.getpid()}")
    # LF on every platform, so files in the memory repo are byte-identical across devices
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    if mode is not None:
        os.chmod(tmp, mode)
    os.replace(tmp, path)


# ------------------------------------------------------------------ local state


class State:
    def __init__(self, cfg):
        self.dir = cfg.state_dir
        self.device = read_json(self.dir / "device.json", None)
        self.imported = set(self._lines("imported.keys"))
        self.tombstoned = set(self._lines("tombstoned.keys"))
        # key -> kind for every row this device has published, so deletions become tombstones
        self.exported = dict(line.split(" ", 1) for line in self._lines("exported.keys") if " " in line)

    def _lines(self, name):
        try:
            return (self.dir / name).read_text(encoding="utf-8").splitlines()
        except OSError:
            return []

    def save(self):
        write_text_atomic(self.dir / "imported.keys", "".join(f"{k}\n" for k in sorted(self.imported)))
        write_text_atomic(self.dir / "tombstoned.keys", "".join(f"{k}\n" for k in sorted(self.tombstoned)))
        write_text_atomic(self.dir / "exported.keys", "".join(f"{k} {v}\n" for k, v in sorted(self.exported.items())))

    def create_device(self, name):
        self.device = {"id": str(uuid.uuid4()), "name": name}
        write_text_atomic(self.dir / "device.json", json.dumps(self.device, indent=2) + "\n")

    def require_device(self):
        if not self.device:
            raise CmSyncError("no device id yet: run `cm-sync setup` first")
        return self.device


# ------------------------------------------------------------------ worker


class Worker:
    def __init__(self, url, timeout=60):
        self.url = url
        self.timeout = timeout

    def _request(self, method, route, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.url + route, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, OSError) as e:
            raise CmSyncError(f"claude-mem worker not reachable at {self.url} ({e})") from e

    def health(self):
        try:
            status, body = self._request("GET", "/api/health")
        except CmSyncError:
            return None
        return body if status == 200 and isinstance(body, dict) else None

    def require_health(self):
        health = self.health()
        if not health:
            raise CmSyncError(f"claude-mem worker not running at {self.url}: start a Claude Code session first")
        return health

    def sync_status(self):
        try:
            status, body = self._request("GET", "/api/sync/status")
        except CmSyncError:
            return None
        return body if status == 200 else None

    def import_batch(self, payload):
        status, body = self._request("POST", "/api/import", payload)
        if status != 200:
            raise CmSyncError(f"POST /api/import failed ({status}): {body}")
        return body

    def delete(self, kind, row):
        if kind == "sessions":
            route = f"/api/sessions/{quote(row['platform_source'])}/{quote(row['content_session_id'])}"
        else:
            route = f"/api/{DELETE_ROUTES[kind]}/{row['id']}"
        status, body = self._request("DELETE", route)
        if status in (200, 204, 404):
            return status != 404
        raise CmSyncError(f"DELETE {route} failed ({status}): {body}")

    def restart(self):
        self._request("POST", "/api/admin/restart", {})

    def load_rules(self):
        """Have the worker rewrite MY-RULES.md and load it in CLAUDE.md; None when claude-mem predates 15.1."""
        route = "/api/work-view/rules/load-in-claude-code"
        status, body = self._request("POST", route, {})
        if status == 404:
            return None
        if status != 200:
            raise CmSyncError(f"POST {route} failed ({status}): {body}")
        return True


DELETE_ROUTES = {"observations": "observation", "summaries": "summary", "prompts": "prompt"}


def quote(value):
    return urllib.request.quote(str(value), safe="")


# ------------------------------------------------------------------ rows and keys


def row_key(kind, row):
    values = [kind] + [row.get(c) for c in KEY_COLUMNS[kind]]
    blob = json.dumps(values, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def connect_ro(db_path):
    if not db_path.exists():
        raise CmSyncError(f"claude-mem database not found: {db_path}")
    con = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def read_local(db_path):
    """All local rows per kind, with `id` and filter fields kept for internal use."""
    con = connect_ro(db_path)
    try:
        def select(sql):
            return [dict(r) for r in con.execute(sql)]

        sessions = select(
            "SELECT id, " + ", ".join(COLUMNS["sessions"]) + " FROM sdk_sessions"
        )
        observations = select(
            "SELECT id, metadata, "
            + ", ".join("COALESCE(NULLIF(merged_into_project, ''), project) AS project" if c == "project" else c for c in COLUMNS["observations"])
            + " FROM observations"
        )
        summaries = select(
            "SELECT id, "
            + ", ".join("COALESCE(NULLIF(merged_into_project, ''), project) AS project" if c == "project" else c for c in COLUMNS["summaries"])
            + " FROM session_summaries"
        )
        prompts = select(
            "SELECT p.id, p.content_session_id, COALESCE(NULLIF(s.platform_source, ''), 'claude') AS platform_source, "
            "p.prompt_number, p.prompt_text, p.created_at, p.created_at_epoch "
            "FROM user_prompts p LEFT JOIN sdk_sessions s ON s.id = p.session_db_id"
        )
        # local_id is the row's id on the device that created it, so its key matches that device's tombstones
        work_state = select(
            "SELECT id, COALESCE(CAST(origin_local_id AS INTEGER), id) AS local_id, origin_device_id, origin_local_id, "
            "project, list_name, fields, created_at, created_at_epoch FROM work_state_entries"
        ) if has_work_state(con) else []
    finally:
        con.close()
    return {"sessions": sessions, "observations": observations, "summaries": summaries, "prompts": prompts,
            "work_state": work_state}


def has_work_state(con):
    """True when work_state_entries exists with every column the direct insert writes."""
    columns = {r[1] for r in con.execute("PRAGMA table_info(work_state_entries)")}
    return WORK_STATE_COLUMNS <= columns


def is_derived(row):
    metadata = (row.get("metadata") or "").replace(" ", "")
    return any(s in metadata for s in DERIVED_SOURCES)


ROW_LABELS = {"sessions": "session", "observations": "observation", "summaries": "summary", "prompts": "prompt",
              "work_state": "work-state entry"}


def public(kind, row):
    return {c: row.get(c) for c in COLUMNS[kind]}


def scan_secrets(kind, row):
    hits = []
    for column in SCAN_COLUMNS[kind]:
        value = row.get(column)
        if not isinstance(value, str):
            continue
        for name, pattern in SECRET_PATTERNS.items():
            if pattern.search(value):
                hits.append((column, name))
    return hits


# ------------------------------------------------------------------ export


def device_dir(cfg, device_id):
    return cfg.devices_root / device_id


def read_jsonl(path):
    try:
        with open(path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except OSError:
        return []


def jsonl(rows):
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n" for r in rows)


def export(cfg, state, worker=None, with_prompts=False, allow_unredacted=False, exclude_flagged=False):
    device = state.require_device()
    if not cfg.redaction_enabled and not allow_unredacted:
        raise CmSyncError(
            "claude-mem redaction is off (CLAUDE_MEM_REDACT_ENABLED), so secrets may be in your memory.\n"
            "Turn it on with `cm-sync setup`, or pass --allow-unredacted to export anyway (the secret scan still runs)."
        )

    local = read_local(cfg.db_path)
    present = {kind: {row_key(kind, r) for r in rows} for kind, rows in local.items()}
    all_present = set().union(*present.values())
    blocked = state.imported | state.tombstoned

    own = {kind: [] for kind in KINDS}
    for kind in ("observations", "summaries", "prompts"):
        if kind == "prompts" and not with_prompts:
            continue
        for row in local[kind]:
            if kind == "observations" and (row.get("type") == "sensitive" or is_derived(row)):
                continue
            if row_key(kind, row) not in blocked:
                own[kind].append(row)
    # Rows with an origin came from another device (cm-sync or claude-mem cloud sync)
    own["work_state"] = [r for r in local["work_state"]
                         if not r.get("origin_device_id") and row_key("work_state", r) not in blocked]

    by_memory_id = {s["memory_session_id"]: s for s in local["sessions"] if s.get("memory_session_id")}
    by_content_id = {(s["platform_source"], s["content_session_id"]): s for s in local["sessions"]}
    needed = {}
    for kind in ("observations", "summaries"):
        for row in own[kind]:
            session = by_memory_id.get(row["memory_session_id"])
            if session:
                needed[session["id"]] = session
    for row in own["prompts"]:
        session = by_content_id.get((row["platform_source"], row["content_session_id"]))
        if session:
            needed[session["id"]] = session
    own["sessions"] = [dict(s, user_prompt=s["user_prompt"] if with_prompts else None) for s in needed.values()]

    flagged = {}
    for kind in KINDS:
        for row in own[kind]:
            for column, pattern in scan_secrets(kind, row):
                flagged.setdefault((kind, row["id"]), (row, []))[1].append(f"{column} matches {pattern}")
    if flagged:
        lines = [f"  {ROW_LABELS[kind]} #{row_id} ({(row.get('title') or row.get('custom_title') or row.get('request') or row.get('list_name') or '')[:60]}): {', '.join(hits)}"
                 for (kind, row_id), (row, hits) in flagged.items()]
        listing = "possible secrets found:\n" + "\n".join(lines)
        # No worker route deletes sessions or work state
        deletable = {k: v for k, v in flagged.items() if k[0] not in ("sessions", "work_state")}
        if not exclude_flagged and worker and deletable and ask_tty(
                listing + f"\nDelete these {len(deletable)} rows from claude-mem now?", default=True):
            for (kind, _), (row, _) in deletable.items():
                worker.delete(kind, row)
                all_present.discard(row_key(kind, row))
            info(f"Deleted {len(deletable)} rows")
            kept = [k for k in flagged if k not in deletable]
            if kept:
                warn("Kept local only (sessions and work state are never deleted here): "
                     + ", ".join(f"{ROW_LABELS[kind]} #{i}" for kind, i in kept))
        elif not exclude_flagged:
            raise CmSyncError(
                listing.replace("found:", "found, nothing was written:", 1) +
                f"\nDelete those rows in the claude-mem viewer ({cfg.worker_url}), or pass --exclude-flagged to keep them local only."
            )
        else:
            warn("Kept local only " + listing)
        own = {kind: [r for r in rows if (kind, r["id"]) not in flagged] for kind, rows in own.items()}

    published = {kind: sorted(((row_key(kind, r), public(kind, r)) for r in own[kind]),
                              key=lambda kr: (kr[1].get("created_at_epoch") or kr[1].get("started_at_epoch") or 0, kr[0]))
                 for kind in KINDS}

    out = device_dir(cfg, device["id"])
    old_tombstones = read_jsonl(out / "tombstones.jsonl")
    known = {t["key"] for t in old_tombstones}
    now = int(time.time())
    new_tombstones = [{"kind": kind, "key": key, "at": now}
                      for key, kind in sorted(state.exported.items())
                      if key not in all_present and key not in known]

    for kind in KINDS:
        path = out / f"{kind}.jsonl"
        if kind == "prompts" and not with_prompts:
            if path.exists():
                path.unlink()
            continue
        write_if_changed(path, jsonl(row for _, row in published[kind]))
    if new_tombstones:
        write_text_atomic(out / "tombstones.jsonl", jsonl(old_tombstones + new_tombstones))

    version = (worker.health() or {}).get("version") if worker else None
    manifest = {
        "format": FORMAT,
        "device_id": device["id"],
        "device_name": device["name"],
        "claude_mem_version": version or read_json(out / "manifest.json", {}).get("claude_mem_version"),
        "counts": {kind: len(published[kind]) for kind in KINDS},
    }
    write_if_changed(out / "manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    for t in new_tombstones:
        state.exported.pop(t["key"], None)
    for kind in KINDS:
        for key, _ in published[kind]:
            state.exported[key] = kind
    state.save()

    step("Exported", counted(*((len(published[k]), k) for k in KINDS if k != "prompts" or with_prompts), (len(new_tombstones), "new tombstones")))
    info(" " * 14 + paint(f"→ {tilde(out.parent)}/{device['id'][:8]}…", "dim"))
    return {"published": {k: len(v) for k, v in published.items()}, "tombstones": len(new_tombstones)}


def write_if_changed(path, text):
    try:
        if path.read_text(encoding="utf-8") == text:
            return False
    except OSError:
        pass
    write_text_atomic(path, text)
    return True


# ------------------------------------------------------------------ import


def other_devices(cfg, own_id):
    if not cfg.devices_root.is_dir():
        return []
    return sorted(p for p in cfg.devices_root.iterdir() if p.is_dir() and p.name != own_id and not p.name.startswith("."))


def batches(rows_by_kind, sessions, limit=BATCH_BYTES):
    """Yield (payload, keys_by_kind) batches under `limit` bytes, each carrying the sessions its rows need."""
    by_memory_id = {s["memory_session_id"]: s for _, s in sessions if s.get("memory_session_id")}
    by_content_id = {(s["platform_source"], s["content_session_id"]): s for _, s in sessions}
    session_keys = {id(s): k for k, s in sessions}

    def empty():
        return {kind: [] for kind in IMPORT_KINDS}, {kind: [] for kind in IMPORT_KINDS}, set(), 2

    payload, keys, included, size = empty()
    for kind in ("observations", "summaries", "prompts"):
        for key, row in rows_by_kind.get(kind, []):
            if kind == "prompts":
                session = by_content_id.get((row.get("platform_source"), row["content_session_id"]))
            else:
                session = by_memory_id.get(row["memory_session_id"])
            def cost():
                extra = [session] if session is not None and id(session) not in included else []
                return extra, len(json.dumps(row)) + sum(len(json.dumps(s)) for s in extra) + 2

            extra, row_size = cost()
            if size + row_size > limit and any(payload[k] for k in IMPORT_KINDS):
                yield payload, keys
                payload, keys, included, size = empty()
                extra, row_size = cost()
            for s in extra:
                payload["sessions"].append(s)
                keys["sessions"].append(session_keys[id(s)])
                included.add(id(s))
            payload[kind].append(row)
            keys[kind].append(key)
            size += row_size
    if any(payload[k] for k in IMPORT_KINDS):
        yield payload, keys


def import_all(cfg, state, worker, full=False):
    device = state.require_device()
    worker.require_health()
    devices = other_devices(cfg, device["id"])
    if not devices:
        step("Imported", paint("nothing, no other devices in " + tilde(cfg.devices_root), "dim"))
        return {"imported": 0, "skipped": 0, "rejected": 0, "deleted": 0}

    tombstones = {}
    for d in devices:
        for t in read_jsonl(d / "tombstones.jsonl"):
            tombstones[t["key"]] = t.get("kind", "observations")
    blocked = set(tombstones) | state.tombstoned
    known = set() if full else state.imported

    totals = {"imported": 0, "skipped": 0, "rejected": 0, "deleted": 0}
    for d in devices:
        manifest = read_json(d / "manifest.json", {})
        name = manifest.get("device_name") or d.name
        sessions = [(row_key("sessions", s), s) for s in read_jsonl(d / "sessions.jsonl")]
        rows = {}
        for kind in ("observations", "summaries", "prompts", "work_state"):
            rows[kind] = []
            for row in read_jsonl(d / f"{kind}.jsonl"):
                key = row_key(kind, row)
                if key not in blocked and key not in known:
                    rows[kind].append((key, row))
        if not any(rows.values()):
            step("Imported", f"{name}: " + paint("up to date", "dim"))
            continue

        imported = skipped = rejected = 0
        for payload, keys in batches(rows, sessions):
            result = worker.import_batch(payload)
            stats = result.get("stats", {})
            imported += sum(stats.get(f"{k}Imported", 0) for k in ("observations", "summaries", "prompts"))
            skipped += sum(stats.get(f"{k}Skipped", 0) for k in ("observations", "summaries", "prompts"))
            bad = result.get("rejected") or {}
            for kind in IMPORT_KINDS:
                bad_idx = {r.get("index") for r in bad.get(kind, [])}
                rejected += len(bad_idx) if kind != "sessions" else 0
                for i, key in enumerate(keys[kind]):
                    if i not in bad_idx:
                        state.imported.add(key)
                for r in bad.get(kind, [])[:5]:
                    warn(f"  {name}: {kind}[{r.get('index')}] rejected: {r.get('reason')}")
        if rows["work_state"]:
            added = insert_work_state(cfg, d.name, [row for _, row in rows["work_state"]])
            if added is not None:
                imported += added
                skipped += len(rows["work_state"]) - added
                state.imported.update(key for key, _ in rows["work_state"])
        step("Imported", f"{name}: " + counted((imported, "new"), (skipped, "already present"), (rejected, "rejected")))
        totals["imported"] += imported
        totals["skipped"] += skipped
        totals["rejected"] += rejected

    pending = {k: v for k, v in tombstones.items() if k not in state.tombstoned}
    if pending:
        local = read_local(cfg.db_path)
        order = ("observations", "summaries", "prompts", "sessions", "work_state")
        index = {kind: {row_key(kind, r): r for r in local[kind]} for kind in order}
        for kind in order:
            for key, t_kind in pending.items():
                if t_kind != kind:
                    continue
                row = index[kind].get(key)
                if row is not None and (delete_work_state(cfg, row) if kind == "work_state" else worker.delete(kind, row)):
                    totals["deleted"] += 1
                state.tombstoned.add(key)
        if totals["deleted"]:
            step("Deleted", f"{totals['deleted']} rows deleted on other devices")

    state.save()
    # Every import, not only when rule rows arrive: rows imported before cm-sync rebuilt rules never arrive again
    rebuild_rules(cfg, worker)
    return totals


def rules_loaded(cfg):
    """True when CLAUDE.md imports MY-RULES.md, written the way claude-mem writes the line."""
    try:
        lines = cfg.claude_md.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    return bool({line.strip() for line in lines} & {"@" + tilde(cfg.rules_path), "@" + str(cfg.rules_path)})


def rule_counts(db_path):
    """(active, total) My Rules, folded like claude-mem: each rule's fields from its entries, oldest first."""
    con = connect_ro(db_path)
    try:
        rows = con.execute(
            "SELECT fields FROM work_state_entries WHERE list_name = ? ORDER BY created_at_epoch, "
            "COALESCE(origin_device_id, ''), COALESCE(CAST(origin_local_id AS INTEGER), id)", (RULES_LIST,)).fetchall()
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()
    rules = {}
    for row in rows:
        fields = json.loads(row["fields"])
        task = str(fields.get("task") or "")
        if not task.startswith("Rule: "):
            continue
        rule = rules.setdefault(task, {})
        for key, value in fields.items():
            if value is None:
                rule.pop(key, None)
            else:
                rule[key] = value
    active = sum(r.get("status") == "on" and r.get("set_by") in RULES_ACTIVE_BY for r in rules.values())
    return active, len(rules)


def rebuild_rules(cfg, worker):
    """Rewrite MY-RULES.md through the worker: it rebuilds the file only for rules its own cloud sync applies."""
    if not rules_loaded(cfg):
        counts = rule_counts(cfg.db_path) if cfg.db_path.exists() else None
        if counts and counts[0]:
            info(paint(f"  My Rules aren't loaded in {tilde(cfg.claude_md)}, run `cm-sync setup`", "dim"))
        return
    try:
        rebuilt = worker.load_rules()
    except CmSyncError as e:
        warn(f"cm-sync: My Rules not rebuilt ({e})")
        return
    if rebuilt is None:
        warn("cm-sync: My Rules not rebuilt, the worker needs claude-mem 15.1+")
        return
    step("Rules", "rebuilt " + tilde(cfg.rules_path))


def connect_rw(db_path):
    # timeout waits out the worker's write locks; the database is in WAL mode, so its reads go on
    return sqlite3.connect(db_path.resolve().as_uri() + "?mode=rw", uri=True, timeout=5)


def insert_work_state(cfg, origin_device, rows):
    """Insert another device's work-state rows the way claude-mem's cloud sync applies them.

    Keeps created_at_epoch, which decides the latest value of every field. The unique
    (origin_device_id, origin_local_id) index makes re-imports no-ops. Returns how many rows
    were new, or None when this claude-mem has no origin columns.
    """
    con = connect_rw(cfg.db_path)
    try:
        if not has_work_state(con):
            warn("cm-sync: claude-mem has no work-state origin columns (needs v15+), work state not imported")
            return None
        now = int(time.time() * 1000)
        with con:
            before = con.total_changes
            con.executemany(
                "INSERT OR IGNORE INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch, "
                "synced_at, origin_device_id, origin_local_id, sync_rev) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '1')",
                [(r["project"], r["list_name"], r["fields"], r["created_at"], r["created_at_epoch"],
                  now, origin_device, str(r["local_id"])) for r in rows],
            )
            return con.total_changes - before
    except sqlite3.Error as e:
        raise CmSyncError(f"work-state import failed, nothing was written ({e})") from e
    finally:
        con.close()


def delete_work_state(cfg, row):
    """Delete a work-state row that came from another device; this device's own rows are never deleted."""
    if not row.get("origin_device_id"):
        return False
    con = connect_rw(cfg.db_path)
    try:
        with con:
            cur = con.execute("DELETE FROM work_state_entries WHERE origin_device_id = ? AND origin_local_id = ?",
                              (row["origin_device_id"], row["origin_local_id"]))
            return cur.rowcount > 0
    finally:
        con.close()


# ------------------------------------------------------------------ refile


def refile(cfg, worker, session_id, project):
    """Move a session's observations and summaries to another project: import copies, then delete the originals."""
    worker.require_health()
    con = connect_ro(cfg.db_path)
    try:
        con_rows = con.execute("SELECT id, " + ", ".join(COLUMNS["sessions"]) + " FROM sdk_sessions WHERE content_session_id = ?", (session_id,))
        sessions = [dict(r) for r in con_rows]
    finally:
        con.close()
    if not sessions:
        raise CmSyncError(f"no claude-mem session with id {session_id}")
    source = sessions[0]
    if source["project"] == project:
        raise CmSyncError(f"session {session_id} already belongs to {project}")

    local = read_local(cfg.db_path)
    originals = {kind: [r for r in local[kind] if r.get("memory_session_id") == source["memory_session_id"]]
                 for kind in ("observations", "summaries")}
    total = sum(len(v) for v in originals.values())
    if not total:
        info(f"Nothing to move: session {session_id} has no observations or summaries left")
        return

    target = dict(public("sessions", source),
                  content_session_id=f"{session_id}@{project}",
                  memory_session_id=f"{source['memory_session_id']}@{project}",
                  project=project)
    rows = {kind: [] for kind in originals}
    for kind, items in originals.items():
        for row in items:
            copy = dict(public(kind, row), memory_session_id=target["memory_session_id"], project=project)
            rows[kind].append((row_key(kind, copy), copy))

    accepted = 0
    for payload, _ in batches(rows, [(row_key("sessions", target), target)]):
        result = worker.import_batch(payload)
        if any(result.get("rejected", {}).get(k) for k in ("sessions", "observations", "summaries")):
            raise CmSyncError(f"import rejected rows, originals kept: {result['rejected']}")
        stats = result.get("stats", {})
        accepted += sum(stats.get(f"{k}{s}", 0) for k in ("observations", "summaries") for s in ("Imported", "Skipped"))
    if accepted != total:
        raise CmSyncError(f"only {accepted} of {total} rows were accepted, originals kept")

    for kind, items in originals.items():
        for row in items:
            worker.delete(kind, row)
    info(f"Moved {len(originals['observations'])} observations and {len(originals['summaries'])} summaries to {project}")


# ------------------------------------------------------------------ git


def git(cfg, *args, check=True):
    result = subprocess.run(["git", "-C", str(cfg.repo), *args], capture_output=True, encoding="utf-8", errors="replace")
    if check and result.returncode != 0:
        raise CmSyncError(f"git {' '.join(args)} failed:\n{result.stderr.strip() or result.stdout.strip()}")
    return result


def require_repo(cfg):
    if git(cfg, "rev-parse", "--is-inside-work-tree", check=False).returncode != 0:
        raise CmSyncError(f"{cfg.repo} is not a git repo: run `cm-sync setup`")


def has_upstream(cfg):
    return git(cfg, "rev-parse", "--abbrev-ref", "@{upstream}", check=False).returncode == 0


def require_no_conflicts(cfg):
    unmerged = git(cfg, "diff", "--name-only", "--diff-filter=U").stdout.split()
    if not unmerged:
        return
    stashed = "; your local edits are also kept in `git stash list`" if git(cfg, "stash", "list").stdout.strip() else ""
    raise CmSyncError(
        f"unmerged files in {cfg.repo}: {', '.join(unmerged)}\n"
        f"Resolve them, `git add` them{stashed}, then run cm-sync again"
    )


def pull(cfg):
    require_repo(cfg)
    if not has_upstream(cfg):
        return
    require_no_conflicts(cfg)
    branch = git(cfg, "symbolic-ref", "--short", "HEAD").stdout.strip()
    remote = git(cfg, "config", f"branch.{branch}.remote").stdout.strip()
    merge = git(cfg, "config", f"branch.{branch}.merge").stdout.strip().removeprefix("refs/heads/")
    # Naming the branch keeps FETCH_HEAD to one entry even when an editor fetches at the same time
    args = ("pull", "--rebase", "--autostash", "--quiet", remote, merge)
    if git(cfg, *args, check=False).returncode != 0:
        time.sleep(1)
        git(cfg, *args)
    # A conflicting autostash pop still exits 0, leaving the conflict for the next pull to trip over
    require_no_conflicts(cfg)
    sha, _, subject = git(cfg, "log", "-1", "--format=%h %s").stdout.strip().partition(" ")
    step("Pulled", f"{paint(sha, 'yellow')} {subject}")


def run_gitleaks(path):
    if not shutil.which("gitleaks"):
        return
    result = subprocess.run(["gitleaks", "dir", str(path), "--no-banner", "--redact"], capture_output=True, encoding="utf-8", errors="replace")
    if result.returncode == 1:
        raise CmSyncError(f"gitleaks found possible secrets in {path}, nothing was committed:\n{result.stdout.strip()}")


def sync(cfg, state, worker, **export_args):
    device = state.require_device()
    pull(cfg)
    import_all(cfg, state, worker)
    result = export(cfg, state, worker, **export_args)

    rel = (DEVICES_DIR / device["id"]).as_posix()
    run_gitleaks(cfg.repo / rel)
    git(cfg, "add", "--", rel)
    if not git(cfg, "status", "--porcelain", "--", rel).stdout.strip():
        step("Published", paint("nothing new", "dim"))
        return
    rows = sum(result["published"].values())
    tombstones = f", {result['tombstones']} tombstones" if result["tombstones"] else ""
    git(cfg, "commit", "--quiet", "--no-verify", "-m", f"cm-sync: {device['name']} {rows} rows{tombstones}", "--", rel)
    if not has_upstream(cfg):
        step("Published", "committed locally (no upstream branch to push to)")
        return
    if git(cfg, "push", "--quiet", check=False).returncode != 0:
        pull(cfg)
        git(cfg, "push", "--quiet")
    step("Published", "pushed")


# ------------------------------------------------------------------ setup / status / hooks


def ask(question, default=False, assume_yes=False):
    if assume_yes:
        return True
    return ask_tty(question, default)


def ask_tty(question, default=False):
    """Prompt only on an interactive terminal; otherwise answer no."""
    if not sys.stdin.isatty():
        return False
    suffix = " [Y/n] " if default else " [y/N] "
    answer = input(question + suffix).strip().lower()
    return default if not answer else answer in ("y", "yes")


def setup(cfg, state, worker, assume_yes=False):
    ok, problems = "✅", 0

    def report(passed, label, fix=None, level="❌"):
        nonlocal problems
        print(f"{ok if passed else level} {label}")
        if not passed:
            problems += level == "❌"
            if fix:
                print(f"   → {fix}")

    pythons = ("py", "python") if WINDOWS else ("python3",)
    report(any(shutil.which(p) for p in pythons), f"{' or '.join(pythons)} on PATH (runs cm-sync)", "install Python 3.9+")
    for tool, why in (("git", "syncs the memory repo"), ("bun", "claude-mem's worker needs it"), ("node", "claude-mem's hooks need it")):
        report(bool(shutil.which(tool)), f"{tool} on PATH ({why})", f"install {tool}")

    claude_settings = read_json(Path.home() / ".claude" / "settings.json", {})
    enabled = (claude_settings.get("enabledPlugins") or {}).get("claude-mem@thedotmack") is True
    report(enabled, "claude-mem plugin enabled in ~/.claude/settings.json", "run `npx claude-mem install`")

    health = worker.health()
    report(bool(health), f"claude-mem worker running at {cfg.worker_url}" + (f" (v{health.get('version')})" if health else ""),
           "start a Claude Code session; the plugin's SessionStart hook starts the worker")

    is_repo = git(cfg, "rev-parse", "--is-inside-work-tree", check=False).returncode == 0 if cfg.repo.exists() else False
    remote = None if cfg.remote == DEFAULT_REMOTE else cfg.remote
    if not is_repo and not cfg.repo.exists():
        if sys.stdin.isatty() and not assume_yes:
            hint = f" [{remote}]" if remote else " (blank to skip)"
            remote = input(f"Git URL of your memory repo to clone into {cfg.repo}{hint}: ").strip() or remote
        if remote:
            subprocess.run(["git", "clone", remote, str(cfg.repo)], check=False)
            is_repo = git(cfg, "rev-parse", "--is-inside-work-tree", check=False).returncode == 0
    clone_fix = (f"git clone {remote} {cfg.repo}" if remote
                 else "rerun `cm-sync setup` and enter your memory repo URL, or set CM_SYNC_REMOTE")
    report(is_repo, f"memory repo at {cfg.repo}", clone_fix)

    auto_memory = claude_settings.get("autoMemoryDirectory")
    expected = tilde(cfg.repo)
    matches = bool(auto_memory) and Path(auto_memory).expanduser() == cfg.repo
    actual = "not set" if not auto_memory else f"is {auto_memory}"
    report(matches, f"Claude Code autoMemoryDirectory is {expected}" if matches
           else f"Claude Code autoMemoryDirectory {actual}, expected {expected}",
           f'set "autoMemoryDirectory": "{expected}" in ~/.claude/settings.json', level="⚠️")

    if not cfg.redaction_enabled and cfg.settings_path.exists() and ask(
            "Turn on claude-mem secret redaction (CLAUDE_MEM_REDACT_ENABLED) and restart the worker?", default=True, assume_yes=assume_yes):
        settings = read_json(cfg.settings_path, None)
        if settings is not None:
            settings["CLAUDE_MEM_REDACT_ENABLED"] = "true"
            write_text_atomic(cfg.settings_path, json.dumps(settings, indent=2, ensure_ascii=False) + "\n", mode=0o600)
            cfg.settings = settings
            if health:
                worker.restart()
                print("   restarted the worker")
    report(cfg.redaction_enabled, "claude-mem secret redaction on", "rerun `cm-sync setup` and accept, or export with --allow-unredacted")

    if health and not rules_loaded(cfg) and ask(
            f"Load My Rules ({tilde(cfg.rules_path)}) in {tilde(cfg.claude_md)}?", default=True, assume_yes=assume_yes):
        if worker.load_rules() is None:
            print("   the worker needs claude-mem 15.1+")
    report(rules_loaded(cfg), f"My Rules loaded in {tilde(cfg.claude_md)}",
           "rerun `cm-sync setup` and accept (needs claude-mem 15.1+)", level="⚠️")

    cloud = worker.sync_status() if health else None
    report(not (cloud or {}).get("configured"), "cmem.ai cloud sync not configured (don't combine it with cm-sync)",
           "disconnect cloud sync, or don't use cm-sync on this machine", level="⚠️")

    legacy = cfg.repo / LEGACY_EXPORT_DIR
    report(not legacy.exists(), "no legacy Markdown export in the memory repo", f"delete {legacy} once you no longer need it", level="⚠️")
    roots = cfg.setting("CLAUDE_MEM_MEMORY_WATCH_ROOTS") or ""
    report((cfg.repo / "claude-mem").as_posix() not in roots.replace("\\", "/").replace("~", Path.home().as_posix()),
           "CLAUDE_MEM_MEMORY_WATCH_ROOTS doesn't watch the claude-mem export", "remove those roots from ~/.claude-mem/settings.json", level="⚠️")

    if not state.device:
        default_name = socket.gethostname().split(".")[0]
        name = default_name
        if sys.stdin.isatty() and not assume_yes:
            name = input(f"Name for this device [{default_name}]: ").strip() or default_name
        state.create_device(name)
    report(True, f"device {state.device['name']} ({state.device['id']})")

    print("\nReady: run `cm-sync sync`" if problems == 0 else f"\n{problems} problem(s) to fix first")
    return problems


def status(cfg, state, worker):
    def row(label, value):
        print(f"  {label + ':':<12}{value}")

    health = worker.health()
    print(paint("claude-mem", "bold"))
    if health:
        row("Worker", paint("●", "green") + f" v{health.get('version')} at {cfg.worker_url}")
    else:
        row("Worker", paint("● not running", "red") + f" at {cfg.worker_url}")
    row("Database", tilde(cfg.db_path))
    row("Redaction", paint("on", "green") if cfg.redaction_enabled else paint("OFF", "red", "bold"))
    cloud = worker.sync_status() if health else None
    row("Cloud sync", "configured" if (cloud or {}).get("configured") else paint("not configured", "dim"))
    if cfg.db_path.exists():
        local = read_local(cfg.db_path)
        row("Local", counted(*((len(local[k]), k) for k in KINDS)))
        rules = rule_counts(cfg.db_path)
        if rules is not None:
            loaded = "" if rules_loaded(cfg) else "  " + paint("not loaded in Claude Code", "yellow")
            row("Rules", f"{paint(rules[0], 'bold')} active ({rules[1]} total) → {tilde(cfg.rules_path)}{loaded}")

    device = state.device
    print()
    if device:
        print(paint(f"This device: {device['name']}", "bold") + " " + paint(device["id"][:8] + "…", "dim"))
    else:
        print(paint("This device: ", "bold") + paint("none, run `cm-sync setup`", "yellow"))
    row("State", counted((len(state.exported), "published"), (len(state.imported), "imported"),
                         (len(state.tombstoned), "tombstones applied")))
    in_git = cfg.devices_root.is_dir() and git(cfg, "rev-parse", "--is-inside-work-tree", check=False).returncode == 0
    if device and in_git:
        last = git(cfg, "log", "-1", "--format=%h %cr", "--", str(DEVICES_DIR / device["id"]), check=False).stdout.strip()
        sha, _, when = last.partition(" ")
        row("Last push", f"{paint(sha, 'yellow')} {when}" if last else paint("never", "dim"))

    print()
    print(paint("Repo: ", "bold") + tilde(cfg.repo))
    if cfg.devices_root.is_dir():
        devices = []
        for d in sorted(p for p in cfg.devices_root.iterdir() if p.is_dir()):
            manifest = read_json(d / "manifest.json", {})
            mine = " (this device)" if device and d.name == device["id"] else ""
            devices.append((manifest.get("device_name", d.name), mine, f"v{manifest.get('claude_mem_version') or '?'}", manifest, d))
        name_width = max((len(name + mine) for name, mine, *_ in devices), default=0)
        version_width = max((len(version) for _, _, version, *_ in devices), default=0)
        for name, mine, version, manifest, d in devices:
            counts = manifest.get("counts", {})
            tomb = len(read_jsonl(d / "tombstones.jsonl"))
            print("  " + paint(name, "green" if mine else "bold") + paint(mine, "dim")
                  + " " * (name_width - len(name + mine) + 2)
                  + paint(f"{version:<{version_width}}", "dim") + "  "
                  + counted(*((counts.get(k, 0), k) for k in KINDS), (tomb, "tombstones")))
        if not devices:
            print("  " + paint("no devices yet", "dim"))
    if (cfg.repo / LEGACY_EXPORT_DIR).exists():
        warn(f"⚠️  legacy Markdown export still in {tilde(cfg.repo / LEGACY_EXPORT_DIR)}")


def hooks():
    path = Path(__file__).resolve()
    if WINDOWS:
        # Claude Code runs hooks through Git Bash on Windows, which has no python3: name this interpreter, quoted
        script = f'"{Path(sys.executable).as_posix()}" -I "{path.as_posix()}"'
    else:
        script = f"python3 -I {tilde(path)}"
    snippet = {
        "hooks": {
            "SessionStart": [{"hooks": [{"type": "command", "command": f"{script} import --quiet || true", "timeout": 60}]}],
            "SessionEnd": [{"hooks": [{"type": "command", "command": f"{script} sync --quiet || true", "timeout": 120}]}],
        }
    }
    print("Add to ~/.claude/settings.json (or ask Claude: /update-config):\n")
    print(json.dumps(snippet, indent=2))


# ------------------------------------------------------------------ cli

GLOBAL_OPTIONS_WITH_VALUE = ("--repo", "--state-dir")


def with_default_command(argv, commands, default="sync"):
    """Insert `default` after the global options when no command is given: `cm-sync` means `cm-sync sync`."""
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in commands or arg in ("-h", "--help"):
            return argv
        if arg in GLOBAL_OPTIONS_WITH_VALUE:
            i += 2
        elif arg.split("=", 1)[0] in GLOBAL_OPTIONS_WITH_VALUE:
            i += 1
        else:
            break
    return argv[:i] + [default] + argv[i:]


def main(argv=None):
    global QUIET
    if WINDOWS:
        # Piped output (hooks) would otherwise use the ANSI code page and fail on ✅ and non-ASCII titles
        for stream in (sys.stdout, sys.stderr):
            stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(prog="cm-sync", description="Sync claude-mem memory between devices through a git repo.")
    parser.add_argument("--repo", help="memory repo (default: $CM_SYNC_REPO or ~/.claude-memory)")
    parser.add_argument("--state-dir", help="local state (default: $CM_SYNC_STATE_DIR or ~/.local/state/cm-sync)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_setup = sub.add_parser("setup", help="check and fix this machine's setup")
    p_setup.add_argument("-y", "--yes", action="store_true", help="accept every fix without asking")

    export_flags = argparse.ArgumentParser(add_help=False)
    export_flags.add_argument("--with-prompts", action="store_true", help="also publish your raw prompts")
    export_flags.add_argument("--allow-unredacted", action="store_true", help="export even though claude-mem redaction is off")
    export_flags.add_argument("--exclude-flagged", action="store_true", help="keep rows that look like secrets local instead of aborting")
    quiet_flag = argparse.ArgumentParser(add_help=False)
    quiet_flag.add_argument("-q", "--quiet", action="store_true", help="print only warnings and errors")

    sub.add_parser("export", parents=[export_flags, quiet_flag], help="publish this device's rows to the repo")
    p_import = sub.add_parser("import", parents=[quiet_flag], help="import other devices' rows")
    p_import.add_argument("--full", action="store_true", help="re-send rows already imported (after a database reset)")
    p_import.add_argument("--no-pull", action="store_true", help="import what is checked out without pulling the repo first")
    sub.add_parser("sync", parents=[export_flags, quiet_flag], help="pull, import, export, commit and push")
    sub.add_parser("status", help="show sync state")
    sub.add_parser("hooks", help="print the Claude Code hooks for automatic sync")
    sub.add_parser("url", help="print the claude-mem worker URL")
    p_refile = sub.add_parser("refile", parents=[quiet_flag], help="move a session's observations and summaries to another project")
    p_refile.add_argument("session", help="Claude Code session id (the transcript file name)")
    p_refile.add_argument("project", help="target claude-mem project, e.g. my-project")

    args = parser.parse_args(with_default_command(sys.argv[1:] if argv is None else list(argv), set(sub.choices)))
    QUIET = getattr(args, "quiet", False)
    cfg = Config(args.repo, args.state_dir)
    state = State(cfg)
    worker = Worker(cfg.worker_url)
    export_args = {k: getattr(args, k, False) for k in ("with_prompts", "allow_unredacted", "exclude_flagged")}

    try:
        if args.command == "setup":
            return 1 if setup(cfg, state, worker, args.yes) else 0
        if args.command == "export":
            export(cfg, state, worker, **export_args)
        elif args.command == "import":
            if not args.no_pull:
                pull(cfg)
            import_all(cfg, state, worker, full=args.full)
        elif args.command == "sync":
            sync(cfg, state, worker, **export_args)
        elif args.command == "status":
            status(cfg, state, worker)
        elif args.command == "hooks":
            hooks()
        elif args.command == "url":
            print(cfg.worker_url)
        elif args.command == "refile":
            refile(cfg, worker, args.session, args.project)
    except CmSyncError as e:
        warn(f"cm-sync: {e}", "red")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
