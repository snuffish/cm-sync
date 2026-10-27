import contextlib
import io
import json
import os
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import cm_sync as cm

SCHEMA = """
CREATE TABLE sdk_sessions (id INTEGER PRIMARY KEY, content_session_id TEXT NOT NULL, memory_session_id TEXT UNIQUE,
  project TEXT NOT NULL, platform_source TEXT NOT NULL DEFAULT 'claude', user_prompt TEXT, custom_title TEXT,
  started_at TEXT NOT NULL, started_at_epoch INTEGER NOT NULL, completed_at TEXT, completed_at_epoch INTEGER,
  status TEXT NOT NULL DEFAULT 'active');
CREATE TABLE observations (id INTEGER PRIMARY KEY, memory_session_id TEXT NOT NULL, project TEXT NOT NULL, text TEXT,
  type TEXT NOT NULL, title TEXT, subtitle TEXT, facts TEXT, narrative TEXT, concepts TEXT, files_read TEXT,
  files_modified TEXT, prompt_number INTEGER, created_at TEXT NOT NULL, created_at_epoch INTEGER NOT NULL,
  discovery_tokens INTEGER DEFAULT 0, merged_into_project TEXT, agent_type TEXT, agent_id TEXT, metadata TEXT);
CREATE TABLE session_summaries (id INTEGER PRIMARY KEY, memory_session_id TEXT NOT NULL, project TEXT NOT NULL,
  request TEXT, investigated TEXT, learned TEXT, completed TEXT, next_steps TEXT, files_read TEXT, files_edited TEXT,
  notes TEXT, prompt_number INTEGER, created_at TEXT NOT NULL, created_at_epoch INTEGER NOT NULL,
  discovery_tokens INTEGER DEFAULT 0, merged_into_project TEXT);
CREATE TABLE user_prompts (id INTEGER PRIMARY KEY, session_db_id INTEGER, content_session_id TEXT NOT NULL,
  prompt_number INTEGER NOT NULL, prompt_text TEXT NOT NULL, created_at TEXT NOT NULL, created_at_epoch INTEGER NOT NULL);
CREATE TABLE work_state_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT NOT NULL, list_name TEXT NOT NULL,
  fields TEXT NOT NULL, created_at TEXT NOT NULL, created_at_epoch INTEGER NOT NULL, synced_at INTEGER,
  origin_device_id TEXT, origin_local_id TEXT, sync_rev TEXT NOT NULL DEFAULT '1');
CREATE UNIQUE INDEX ux_work_state_entries_origin ON work_state_entries(origin_device_id, origin_local_id)
  WHERE origin_device_id IS NOT NULL;
"""


RULES_ROUTE = "/api/work-view/rules/load-in-claude-code"


def rule(rule_id, status="on", set_by="approved"):
    return {"task": f"Rule: {rule_id}", "rule_id": rule_id, "scope_tier": "everywhere", "scope_name": "Everywhere",
            "when": "", "then": f"rule {rule_id}", "set_by": set_by, "status": status}


class FakeWorker(BaseHTTPRequestHandler):
    requests = []
    rules_route = True

    def log_message(self, *args):
        pass

    def _reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.requests.append(("GET", self.path, None))
        self._reply(200, {"status": "ok", "version": "test"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append(("POST", self.path, body))
        if self.path == RULES_ROUTE:
            self._reply(200 if self.rules_route else 404, {})
            return
        stats = {f"{k}Imported": len(body.get(k, [])) for k in ("sessions", "summaries", "observations", "prompts")}
        self._reply(200, {"stats": stats, "rejected": {}})

    def do_DELETE(self):
        self.requests.append(("DELETE", self.path, None))
        self._reply(200, {"ok": True})


class Env:
    """A device: its own database, state dir and settings, sharing one repo."""

    def __init__(self, root, name, repo, worker_url):
        self.dir = root / name
        self.dir.mkdir(parents=True)
        self.db = self.dir / "claude-mem.db"
        con = sqlite3.connect(self.db)
        con.executescript(SCHEMA)
        con.commit()
        con.close()
        (self.dir / "settings.json").write_text(json.dumps({"CLAUDE_MEM_REDACT_ENABLED": "true"}))
        self.repo = repo
        self.worker_url = worker_url
        self.state_dir = self.dir / "state"
        self.cfg = None
        self.reload()
        self.state.create_device(name)

    def reload(self):
        cfg = cm.Config(self.repo, self.state_dir)
        cfg.data_dir, cfg.db_path, cfg.settings_path = self.dir, self.db, self.dir / "settings.json"
        cfg.settings = json.loads(cfg.settings_path.read_text())
        cfg.worker_url = self.worker_url
        cfg.rules_path, cfg.claude_md = self.dir / "MY-RULES.md", self.dir / "CLAUDE.md"
        cfg.work_view_dir = self.dir / "work-view"
        self.cfg = cfg
        self.state = cm.State(cfg)
        self.worker = cm.Worker(self.worker_url)

    def sql(self, statement, *params):
        con = sqlite3.connect(self.db)
        cur = con.execute(statement, params)
        con.commit()
        con.close()
        return cur.lastrowid

    def add_session(self, sid="s1", prompt="first prompt"):
        return self.sql(
            "INSERT INTO sdk_sessions (content_session_id, memory_session_id, project, platform_source, user_prompt, started_at, started_at_epoch) "
            "VALUES (?, ?, 'GR', 'claude', ?, '2026-10-20', 1)", sid, "mem-" + sid, prompt)

    def add_observation(self, title, sid="s1", epoch=100, type_="discovery", narrative="n", metadata=None):
        return self.sql(
            "INSERT INTO observations (memory_session_id, project, type, title, narrative, facts, created_at, created_at_epoch, metadata) "
            "VALUES (?, 'GR', ?, ?, ?, '[]', '2026-10-20', ?, ?)", "mem-" + sid, type_, title, narrative, epoch, metadata)

    def add_work_state(self, fields, epoch, list_name="suite"):
        return self.sql(
            "INSERT INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch) VALUES ('GR', ?, ?, '2026-10-20', ?)",
            list_name, json.dumps(fields), epoch)

    def work_state(self):
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        rows = [dict(r) for r in con.execute("SELECT * FROM work_state_entries ORDER BY created_at_epoch, id")]
        con.close()
        return rows

    def export(self, **kw):
        return cm.export(self.cfg, self.state, None, **kw)

    def device_dir(self):
        return cm.device_dir(self.cfg, self.state.device["id"])


class CmSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        FakeWorker.requests = []
        FakeWorker.rules_route = True
        self.server = HTTPServer(("127.0.0.1", 0), FakeWorker)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{self.server.server_port}"
        cm.QUIET = True
        self.a = Env(self.root, "a", self.repo, url)
        self.b = Env(self.root, "b", self.repo, url)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def rows(self, env, kind):
        return cm.read_jsonl(env.device_dir() / f"{kind}.jsonl")

    def test_key_ignores_non_dedup_columns(self):
        row = {"memory_session_id": "m", "type": "discovery", "title": "t", "created_at_epoch": 1, "discovery_tokens": 5}
        self.assertEqual(cm.row_key("observations", row), cm.row_key("observations", dict(row, discovery_tokens=99, project="x")))
        self.assertNotEqual(cm.row_key("observations", row), cm.row_key("observations", dict(row, title="other")))

    def test_export_is_deterministic(self):
        self.a.add_session()
        self.a.add_observation("one", epoch=2)
        self.a.add_observation("two", epoch=1)
        self.a.export()
        first = {p.name: p.read_bytes() for p in self.a.device_dir().iterdir()}
        self.a.export()
        self.assertEqual(first, {p.name: p.read_bytes() for p in self.a.device_dir().iterdir()})
        self.assertEqual([r["title"] for r in self.rows(self.a, "observations")], ["two", "one"])

    def test_imported_rows_are_not_exported_again(self):
        self.a.add_session()
        self.a.add_observation("from a")
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        # simulate the worker having inserted A's rows into B's database
        self.b.add_session()
        self.b.add_observation("from a")
        self.b.add_observation("from b", epoch=200)
        self.b.export()
        self.assertEqual([r["title"] for r in self.rows(self.b, "observations")], ["from b"])

    def test_deleted_row_becomes_tombstone(self):
        self.a.add_session()
        obs = self.a.add_observation("doomed")
        self.a.export()
        self.a.sql("DELETE FROM observations WHERE id = ?", obs)
        result = self.a.export()
        self.assertEqual(result["tombstones"], 1)
        tombstones = cm.read_jsonl(self.a.device_dir() / "tombstones.jsonl")
        self.assertEqual([t["kind"] for t in tombstones], ["observations"])
        self.assertEqual(self.rows(self.a, "observations"), [])
        self.a.export()
        self.assertEqual(len(cm.read_jsonl(self.a.device_dir() / "tombstones.jsonl")), 1)

    def test_import_skips_tombstoned_rows_and_deletes_local_copy(self):
        self.a.add_session()
        obs = self.a.add_observation("doomed")
        self.a.export()
        # B already has A's row from an earlier import
        self.b.add_session()
        b_obs = self.b.add_observation("doomed")
        self.a.sql("DELETE FROM observations WHERE id = ?", obs)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        posts = [r for r in FakeWorker.requests if r[0] == "POST"]
        self.assertEqual(posts, [])
        self.assertIn(("DELETE", f"/api/observation/{b_obs}", None), FakeWorker.requests)
        FakeWorker.requests.clear()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertNotIn("DELETE", [r[0] for r in FakeWorker.requests])

    def test_batches_respect_limit_and_carry_sessions(self):
        sessions = [(cm.row_key("sessions", s), s) for s in (
            {"content_session_id": "c1", "memory_session_id": "m1", "platform_source": "claude"},
            {"content_session_id": "c2", "memory_session_id": "m2", "platform_source": "claude"},
        )]
        rows = {"observations": [(f"k{i}", {"memory_session_id": "m1" if i < 5 else "m2", "narrative": "x" * 400}) for i in range(10)]}
        result = list(cm.batches(rows, sessions, limit=1500))
        self.assertGreater(len(result), 1)
        for payload, keys in result:
            self.assertLessEqual(len(json.dumps(payload)), 1500 + 600)
            ids = {s["memory_session_id"] for s in payload["sessions"]}
            self.assertTrue(all(o["memory_session_id"] in ids for o in payload["observations"]))
            self.assertEqual(len(keys["observations"]), len(payload["observations"]))
        self.assertEqual(sum(len(p["observations"]) for p, _ in result), 10)

    def test_sensitive_and_prompts_excluded_by_default(self):
        sid = self.a.add_session(prompt="secret first prompt")
        self.a.add_observation("public")
        self.a.add_observation("hidden", type_="sensitive", epoch=101)
        self.a.add_observation("from md", epoch=102, metadata='{"source":"memory-file"}')
        self.a.sql("INSERT INTO user_prompts (session_db_id, content_session_id, prompt_number, prompt_text, created_at, created_at_epoch) "
                   "VALUES (?, 's1', 1, 'hello', '2026-10-20', 1)", sid)
        self.a.export()
        self.assertEqual([r["title"] for r in self.rows(self.a, "observations")], ["public"])
        self.assertIsNone(self.rows(self.a, "sessions")[0]["user_prompt"])
        self.assertFalse((self.a.device_dir() / "prompts.jsonl").exists())
        self.a.export(with_prompts=True)
        self.assertEqual([r["prompt_text"] for r in self.rows(self.a, "prompts")], ["hello"])
        self.assertEqual(self.rows(self.a, "sessions")[0]["user_prompt"], "secret first prompt")

    def test_secret_scan_aborts_without_writing_or_leaking(self):
        token = "ghp_" + "A1b2C3d4" * 5
        self.a.add_session()
        self.a.add_observation("leak", narrative=f"the token is {token}")
        with self.assertRaises(cm.CmSyncError) as ctx:
            self.a.export()
        self.assertIn("github-token", str(ctx.exception))
        self.assertNotIn(token, str(ctx.exception))
        self.assertFalse(self.a.device_dir().exists())
        self.a.export(exclude_flagged=True)
        self.assertEqual(self.rows(self.a, "observations"), [])

    def test_secret_prompt_deletes_flagged_rows_then_exports(self):
        token = "ghp_" + "A1b2C3d4" * 5
        self.a.add_session()
        self.a.add_observation("clean")
        leak = self.a.add_observation("leak", narrative=f"the token is {token}", epoch=101)
        worker = cm.Worker(self.a.worker_url)
        with mock.patch.object(cm, "ask_tty", return_value=True) as prompt:
            cm.export(self.a.cfg, self.a.state, worker)
        self.assertIn("leak", prompt.call_args[0][0])
        self.assertNotIn(token, prompt.call_args[0][0])
        self.assertIn(("DELETE", f"/api/observation/{leak}", None), FakeWorker.requests)
        self.assertEqual([r["title"] for r in self.rows(self.a, "observations")], ["clean"])
        with mock.patch.object(cm, "ask_tty", return_value=False):
            self.a.add_observation("leak again", narrative=f"the token is {token}", epoch=102)
            with self.assertRaises(cm.CmSyncError):
                cm.export(self.a.cfg, self.a.state, worker)

    def test_export_refuses_without_redaction(self):
        self.a.cfg.settings["CLAUDE_MEM_REDACT_ENABLED"] = "false"
        with self.assertRaises(cm.CmSyncError):
            self.a.export()
        self.a.add_session()
        self.a.add_observation("ok")
        self.a.export(allow_unredacted=True)
        self.assertEqual(len(self.rows(self.a, "observations")), 1)

    def test_refile_imports_copies_then_deletes_originals(self):
        self.a.add_session()
        first = self.a.add_observation("one")
        second = self.a.add_observation("two", epoch=101)
        cm.refile(self.a.cfg, self.a.worker, "s1", "my-project")
        posts = [r[2] for r in FakeWorker.requests if r[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertEqual([s["project"] for s in posts[0]["sessions"]], ["my-project"])
        self.assertEqual({o["project"] for o in posts[0]["observations"]}, {"my-project"})
        self.assertEqual({o["memory_session_id"] for o in posts[0]["observations"]}, {"mem-s1@my-project"})
        deletes = [r[1] for r in FakeWorker.requests if r[0] == "DELETE"]
        self.assertEqual(deletes, [f"/api/observation/{first}", f"/api/observation/{second}"])
        with self.assertRaises(cm.CmSyncError):
            cm.refile(self.a.cfg, self.a.worker, "missing", "my-project")

    def test_sync_is_the_default_command(self):
        commands = {"sync", "import", "export", "status"}
        self.assertEqual(cm.with_default_command([], commands), ["sync"])
        self.assertEqual(cm.with_default_command(["--exclude-flagged"], commands), ["sync", "--exclude-flagged"])
        self.assertEqual(cm.with_default_command(["--repo", "/r", "-q"], commands), ["--repo", "/r", "sync", "-q"])
        self.assertEqual(cm.with_default_command(["--state-dir=/s"], commands), ["--state-dir=/s", "sync"])
        self.assertEqual(cm.with_default_command(["--repo", "/r", "import"], commands), ["--repo", "/r", "import"])
        self.assertEqual(cm.with_default_command(["--help"], commands), ["--help"])

    def test_import_posts_rows_with_their_sessions_once(self):
        self.a.add_session()
        self.a.add_observation("one")
        self.a.add_observation("two", epoch=101)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        posts = [r[2] for r in FakeWorker.requests if r[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertEqual(len(posts[0]["sessions"]), 1)
        self.assertEqual(len(posts[0]["observations"]), 2)
        FakeWorker.requests.clear()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual([r for r in FakeWorker.requests if r[0] == "POST"], [])

    def test_work_state_round_trip_keeps_timestamps(self):
        first = self.a.add_work_state({"task": "scout", "status": "doing"}, epoch=1000)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        [row] = self.b.work_state()
        self.assertEqual(row["created_at_epoch"], 1000)
        self.assertEqual(json.loads(row["fields"]), {"task": "scout", "status": "doing"})
        self.assertEqual(row["origin_device_id"], self.a.state.device["id"])
        self.assertEqual(row["origin_local_id"], str(first))
        self.assertIsNotNone(row["synced_at"])

    def test_imported_work_state_is_not_exported_again(self):
        self.a.add_work_state({"task": "from a"}, epoch=1000)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.b.add_work_state({"task": "from b"}, epoch=2000)
        self.b.export()
        self.assertEqual([json.loads(r["fields"]) for r in self.rows(self.b, "work_state")], [{"task": "from b"}])

    def test_work_state_import_is_idempotent(self):
        self.a.add_work_state({"task": "scout"}, epoch=1000)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        totals = cm.import_all(self.b.cfg, self.b.state, self.b.worker, full=True)
        self.assertEqual(len(self.b.work_state()), 1)
        self.assertEqual((totals["imported"], totals["skipped"]), (0, 1))

    def test_newer_local_entry_still_wins(self):
        self.a.add_work_state({"task": "scout", "status": "doing"}, epoch=1000)
        self.a.export()
        self.b.add_work_state({"task": "scout", "status": "done"}, epoch=2000)
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual([json.loads(r["fields"])["status"] for r in self.b.work_state()], ["doing", "done"])

    def test_work_state_never_sent_to_api_import(self):
        self.a.add_session()
        self.a.add_observation("one")
        self.a.add_work_state({"task": "scout"}, epoch=1000)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        posts = [r[2] for r in FakeWorker.requests if r[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertNotIn("work_state", posts[0])
        self.assertEqual(len(self.b.work_state()), 1)

    def test_missing_work_state_table_is_skipped(self):
        self.a.sql("DROP TABLE work_state_entries")
        self.a.add_session()
        self.a.add_observation("one")
        self.a.export()
        self.assertEqual(self.rows(self.a, "work_state"), [])
        self.b.add_work_state({"task": "from b"}, epoch=1000)
        self.b.export()
        with mock.patch.object(cm, "warn") as warned:
            cm.import_all(self.a.cfg, self.a.state, self.a.worker)
        self.assertIn("work state not imported", warned.call_args[0][0])
        self.assertEqual(self.a.state.imported, set())

    def test_work_state_tombstone_deletes_only_synced_copy(self):
        doomed = self.a.add_work_state({"task": "doomed"}, epoch=1000)
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.b.add_work_state({"task": "doomed"}, epoch=1000)  # B's own row with the same content
        self.a.sql("DELETE FROM work_state_entries WHERE id = ?", doomed)
        self.a.export()
        totals = cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual(totals["deleted"], 1)
        self.assertEqual([r["origin_device_id"] for r in self.b.work_state()], [None])

    def test_secret_in_work_state_fields_kept_local(self):
        token = "ghp_" + "A1b2C3d4" * 5
        self.a.add_work_state({"task": "clean"}, epoch=1000)
        self.a.add_work_state({"task": "leak", "note": f"token {token}"}, epoch=1001)
        worker = cm.Worker(self.a.worker_url)
        with mock.patch.object(cm, "ask_tty", return_value=True) as prompt:
            with self.assertRaises(cm.CmSyncError) as ctx:
                cm.export(self.a.cfg, self.a.state, worker)
        prompt.assert_not_called()
        self.assertIn("work-state entry", str(ctx.exception))
        self.assertNotIn(token, str(ctx.exception))
        cm.export(self.a.cfg, self.a.state, worker, exclude_flagged=True)
        self.assertEqual([json.loads(r["fields"])["task"] for r in self.rows(self.a, "work_state")], ["clean"])
        self.assertNotIn("DELETE", [r[0] for r in FakeWorker.requests])

    def load_rules(self, env):
        env.cfg.claude_md.write_text("# mine\n\n@" + cm.tilde(env.cfg.rules_path) + "\n")

    def rule_rebuilds(self):
        return [r for r in FakeWorker.requests if r[1] == RULES_ROUTE]

    def test_every_import_rebuilds_rules_when_loaded(self):
        self.a.add_work_state(rule("r1"), epoch=1000, list_name="my-rules")
        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual(self.rule_rebuilds(), [])
        self.load_rules(self.b)
        # The rule was imported before the file was loaded: the next import still rebuilds
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual(self.rule_rebuilds(), [("POST", RULES_ROUTE, {})])

    def test_old_worker_warns_and_import_succeeds(self):
        FakeWorker.rules_route = False
        self.load_rules(self.b)
        self.a.add_work_state(rule("r1"), epoch=1000, list_name="my-rules")
        self.a.export()
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            totals = cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        self.assertEqual(totals["imported"], 1)
        self.assertIn("claude-mem 15.1+", stderr.getvalue())

    def test_rule_counts_fold_latest_entry_by_timestamp(self):
        self.a.add_work_state(rule("r1"), epoch=1000, list_name="my-rules")
        self.a.add_work_state(rule("r1", status="off"), epoch=2000, list_name="my-rules")
        self.a.add_work_state(rule("r2", set_by="owner"), epoch=1000, list_name="my-rules")
        # Inserted later but older: an imported entry must not win by id
        self.a.add_work_state(rule("r2", status="off", set_by="owner"), epoch=500, list_name="my-rules")
        self.a.add_work_state(rule("r3", set_by="learned"), epoch=1000, list_name="my-rules")
        self.a.add_work_state({"task": "scout", "status": "on"}, epoch=1000, list_name="my-rules")
        self.a.add_work_state(rule("r4"), epoch=1000)
        self.assertEqual(cm.rule_counts(self.a.db), (1, 3))

    def test_rules_loaded_needs_the_whole_line(self):
        line = "@" + cm.tilde(self.a.cfg.rules_path)
        self.a.cfg.claude_md.write_text(f"see {line}\n{line}.bak\n")
        self.assertFalse(cm.rules_loaded(self.a.cfg))
        self.a.cfg.claude_md.write_text(f"intro\n  {line}  \n")
        self.assertTrue(cm.rules_loaded(self.a.cfg))

    def test_worker_port_without_uid_matches_claude_mem(self):
        env = {"CLAUDE_MEM_DATA_DIR": str(self.root / "none"), "CLAUDE_MEM_WORKER_HOST": "", "CLAUDE_MEM_WORKER_PORT": ""}
        with mock.patch.dict("os.environ", env), mock.patch.object(cm.os, "getuid", None, create=True):
            self.assertEqual(cm.Config(self.repo).worker_url, "http://127.0.0.1:37777")

    def test_written_files_use_lf(self):
        self.a.add_session()
        self.a.add_observation("one")
        self.a.export()
        self.assertNotIn(b"\r\n", (self.a.device_dir() / "manifest.json").read_bytes())

    def test_pull_reports_conflicting_autostash(self):
        ident = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}

        def run(cwd, *args):
            subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, env={**os.environ, **ident})

        run(self.root, "init", "--quiet", "--bare", "-b", "main", "remote.git")
        for name in ("one", "two"):
            run(self.root, "clone", "--quiet", "remote.git", name)
        one, two = self.root / "one", self.root / "two"
        (one / "MEMORY.md").write_text("a\n")
        run(one, "add", "MEMORY.md")
        run(one, "commit", "--quiet", "-m", "init")
        run(one, "push", "--quiet", "-u", "origin", "main")
        run(two, "pull", "--quiet", "origin", "main")
        run(two, "branch", "--quiet", "-u", "origin/main")
        (one / "MEMORY.md").write_text("a\nupstream\n")
        run(one, "commit", "--quiet", "-am", "upstream")
        run(one, "push", "--quiet")
        (two / "MEMORY.md").write_text("a\nlocal\n")

        cfg = SimpleNamespace(repo=two)
        with self.assertRaisesRegex(cm.CmSyncError, r"unmerged files .*MEMORY\.md[\s\S]*git stash list"):
            cm.pull(cfg)
        with self.assertRaisesRegex(cm.CmSyncError, "unmerged files"):
            cm.pull(cfg)

    def test_tilde_uses_forward_slashes(self):
        self.assertEqual(cm.tilde(Path.home() / "a" / "b"), "~/a/b")
        self.assertEqual(cm.tilde(self.root / "x"), (self.root / "x").as_posix())

    def test_paint_only_on_terminals(self):
        tty = mock.Mock(**{"isatty.return_value": True})
        pipe = mock.Mock(**{"isatty.return_value": False})
        with mock.patch.dict(os.environ, {"TERM": "xterm-256color"}, clear=True):
            self.assertEqual(cm.paint("x", "bold", stream=tty), "\033[1mx\033[0m")
            self.assertEqual(cm.paint("x", "bold", stream=pipe), "x")
        with mock.patch.dict(os.environ, {"TERM": "xterm-256color", "NO_COLOR": "1"}, clear=True):
            self.assertEqual(cm.paint("x", "bold", stream=tty), "x")

    def test_work_view_export_copies_ledger_to_device_dir(self):
        wv = self.a.cfg.work_view_dir
        wv.mkdir(parents=True)
        (wv / "story.json").write_text(json.dumps({"narrative": "Hello", "preferences": [{"preference": "Rule 1", "timesSeen": 1}], "writtenAt": "2026-10-25T10:00:00Z"}))
        (wv / "request-groups.json").write_text(json.dumps({"groups": {"g_1": {"title": "Group 1"}}, "conversations": {}, "updatedAt": "2026-10-25T10:00:00Z"}))
        (wv / "alignment.json").write_text(json.dumps({"alignedAt": "2026-10-25T10:00:00Z", "goals": []}))
        (wv / "progress").mkdir()
        (wv / "progress" / "g_1.json").write_text(json.dumps({"groupId": "g_1", "percent": 50, "checkedAt": "2026-10-25T10:00:00Z"}))

        self.a.export()

        out_wv = self.a.device_dir() / "work-view"
        self.assertTrue((out_wv / "story.json").is_file())
        self.assertTrue((out_wv / "request-groups.json").is_file())
        self.assertTrue((out_wv / "alignment.json").is_file())
        self.assertTrue((out_wv / "progress" / "g_1.json").is_file())

        story = cm.read_json(out_wv / "story.json", {})
        self.assertEqual(story["narrative"], "Hello")
        self.assertEqual(story["preferences"][0]["preference"], "Rule 1")

        prog = cm.read_json(out_wv / "progress" / "g_1.json", {})
        self.assertEqual(prog["percent"], 50)

    def test_work_view_import_merges_progress_and_groups(self):
        wv_a = self.a.cfg.work_view_dir
        wv_a.mkdir(parents=True)
        (wv_a / "progress").mkdir()
        (wv_a / "progress" / "g_1.json").write_text(json.dumps({"groupId": "g_1", "percent": 50, "checkedAt": "2026-10-25T10:00:00Z"}))
        (wv_a / "progress" / "g_2.json").write_text(json.dumps({"groupId": "g_2", "percent": 20, "checkedAt": "2026-10-25T11:00:00Z"}))
        (wv_a / "request-groups.json").write_text(json.dumps({
            "groups": {"g_1": {"title": "Group 1", "updatedAt": "2026-10-25T10:00:00Z"}},
            "conversations": {"c_1": {"title": "Conv 1"}},
            "updatedAt": "2026-10-25T10:00:00Z",
        }))
        (wv_a / "alignment.json").write_text(json.dumps({"alignedAt": "2026-10-25T10:00:00Z", "version": 1}))

        wv_b = self.b.cfg.work_view_dir
        wv_b.mkdir(parents=True)
        (wv_b / "progress").mkdir()
        (wv_b / "progress" / "g_1.json").write_text(json.dumps({"groupId": "g_1", "percent": 80, "checkedAt": "2026-10-25T12:00:00Z"}))
        (wv_b / "request-groups.json").write_text(json.dumps({
            "groups": {"g_3": {"title": "Group 3", "updatedAt": "2026-10-25T12:00:00Z"}},
            "conversations": {"c_2": {"title": "Conv 2"}},
            "updatedAt": "2026-10-25T12:00:00Z",
        }))
        (wv_b / "alignment.json").write_text(json.dumps({"alignedAt": "2026-10-25T12:00:00Z", "version": 2}))

        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)

        # On Device B:
        # g_1 was newer on B (80% vs 50%)
        g_1 = cm.read_json(wv_b / "progress" / "g_1.json", {})
        self.assertEqual(g_1["percent"], 80)

        # g_2 was imported from A
        g_2 = cm.read_json(wv_b / "progress" / "g_2.json", {})
        self.assertEqual(g_2["percent"], 20)

        # request-groups unioned groups (g_1 and g_3) and conversations (c_1 and c_2)
        rg = cm.read_json(wv_b / "request-groups.json", {})
        self.assertIn("g_1", rg["groups"])
        self.assertIn("g_3", rg["groups"])
        self.assertIn("c_1", rg["conversations"])
        self.assertIn("c_2", rg["conversations"])

        # alignment took the newer one (alignedAt 12:00:00Z)
        al = cm.read_json(wv_b / "alignment.json", {})
        self.assertEqual(al["version"], 2)

    def test_work_view_import_unions_preferences_and_prunes_decided_rules(self):
        wv_a = self.a.cfg.work_view_dir
        wv_a.mkdir(parents=True)
        (wv_a / "story.json").write_text(json.dumps({
            "narrative": "Story A",
            "writtenAt": "2026-10-25T10:00:00Z",
            "preferences": [
                {"preference": "P1", "timesSeen": 2},
                {"preference": "P2", "timesSeen": 1},
            ],
        }))

        self.a.export()
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)

        wv_b = self.b.cfg.work_view_dir
        story_b = cm.read_json(wv_b / "story.json", {})
        pref_texts = [p["preference"] for p in story_b.get("preferences", [])]
        self.assertEqual(pref_texts, ["P1", "P2"])

        # Device B approves P1
        p1_id = cm.pref_rule_id("P1")
        self.b.add_work_state({"task": f"Rule: {p1_id}", "rule_id": p1_id, "status": "on"}, epoch=200, list_name="my-rules")

        # Re-import; verify P1 is pruned
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        story_b = cm.read_json(wv_b / "story.json", {})
        pref_texts = [p["preference"] for p in story_b.get("preferences", [])]
        self.assertEqual(pref_texts, ["P2"])

        # Device B dismisses P2
        p2_id = cm.pref_rule_id("P2")
        self.b.add_work_state({"task": f"Rule: {p2_id}", "rule_id": p2_id, "status": "dismissed"}, epoch=300, list_name="my-rules")

        # Re-import; verify P2 is also pruned
        cm.import_all(self.b.cfg, self.b.state, self.b.worker)
        story_b = cm.read_json(wv_b / "story.json", {})
        pref_texts = [p["preference"] for p in story_b.get("preferences", [])]
        self.assertEqual(pref_texts, [])


if __name__ == "__main__":
    unittest.main()
