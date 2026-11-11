#!/usr/bin/env python3
"""One-off: replay a memory repo's history as append-only commits on a new branch.

Hand-made commits keep their message, dates and files. cm-sync commits collapse into one per device per day, each
holding only the lines that day added, in the current append-only format. Only the new branch ref is written: the
source branch and the working tree are never touched.

    python3 tools/rebuild_history.py ~/.claude-memory --source main --dest rebuilt
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cm_sync as cm  # noqa: E402

DEVICES = "claude-mem/devices"


def run(repo, *args, input=None, env=None, check=True):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, input=input, env=env)
    if check and result.returncode:
        raise SystemExit(f"git {' '.join(args)} failed: {result.stderr.decode(errors='replace').strip()}")
    return result.stdout


def history(repo, ref):
    if run(repo, "rev-list", "--merges", ref).strip():
        raise SystemExit(f"{ref} has merge commits; this tool replays a linear history")
    return run(repo, "rev-list", "--first-parent", "--reverse", ref).decode().split()


def commit_meta(repo, sha):
    fields = run(repo, "log", "-1", "--format=%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%at%x00%B", sha).decode().split("\0", 7)
    keys = ("an", "ae", "ai", "cn", "ce", "ci", "at", "message")
    return dict(zip(keys, fields))


def changed_paths(repo, sha):
    return [p for p in run(repo, "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", "-z", sha).decode().split("\0") if p]


def classify(paths):
    """('cm', device-id) when a commit touches only one device folder, else ('hand', None)."""
    devices = {p.split("/")[2] for p in paths if p.startswith(DEVICES + "/")}
    others = [p for p in paths if not p.startswith(DEVICES + "/")]
    if devices and others:
        raise SystemExit(f"a commit mixes device files and other files ({others[0]}); inspect it by hand")
    if len(devices) > 1:
        raise SystemExit(f"a commit touches several device folders: {sorted(devices)}")
    return ("cm", next(iter(devices))) if devices else ("hand", None)


def tree_files(repo, sha, prefix=None):
    args = ["ls-tree", "-r", "-z", sha] + (["--", prefix] if prefix else [])
    files = {}
    for entry in run(repo, *args).decode().split("\0"):
        if entry:
            head, _, path = entry.partition("\t")
            mode, _, blob = head.split(" ")
            files[path] = (mode, blob)
    return files


def rows_of(data):
    return [json.loads(line) for line in (data or b"").decode("utf-8").splitlines() if line.strip()]


def fold(data, kind):
    return {cm.row_key(kind, r): r for r in rows_of(data)}


def fold_entity_rows(data):
    ents = {}
    for row in rows_of(data):
        key = (row["kind"], row["id"])
        if row.get("data") is None:
            ents.pop(key, None)
        else:
            ents[key] = row["data"]
    return ents


def device_files(repo, sha, device):
    prefix = f"{DEVICES}/{device}/"
    return {path[len(prefix):]: run(repo, "cat-file", "blob", blob)
            for path, (_, blob) in tree_files(repo, sha, prefix).items()}


def epoch_of(row):
    return row.get("created_at_epoch") or row.get("started_at_epoch") or 0


def append_day(repo, out, last, meta, device, published):
    """Append what the device's folder at `last` adds to the lines already emitted; returns (rows, tombstones, name).

    `published` names the files the source tip still has. A kind missing there was withdrawn on purpose (raw prompts
    are unlinked unless exported with --with-prompts), so its lines are never replayed."""
    end = device_files(repo, last, device)
    manifest = json.loads(end.get("manifest.json", b"{}") or b"{}")
    lines = 0

    seen = {t["key"] for t in rows_of(out.get("tombstones.jsonl"))}
    fresh_tombstones = [t for t in rows_of(end.get("tombstones.jsonl")) if t["key"] not in seen]
    if fresh_tombstones:
        out["tombstones.jsonl"] = out.get("tombstones.jsonl", b"") + cm.jsonl(fresh_tombstones).encode("utf-8")
    dead = seen | {t["key"] for t in fresh_tombstones}

    live = {}
    for kind in cm.KINDS:
        name = f"{kind}.jsonl"
        if name not in published:
            continue
        have = fold(out.get(name), kind)
        fresh = sorted(((k, r) for k, r in fold(end.get(name), kind).items() if have.get(k) != r),
                       key=lambda kr: (epoch_of(kr[1]), kr[0]))
        if fresh:
            out[name] = out.get(name, b"") + cm.jsonl(r for _, r in fresh).encode("utf-8")
        lines += len(fresh)
        live[kind] = len({k for k in have.keys() | {k for k, _ in fresh} if k not in dead})

    if any(p.startswith("work-view/") for p in end):
        with tempfile.TemporaryDirectory(prefix="cm-rebuild-") as tmp:
            for path, data in end.items():
                if path.startswith("work-view/"):
                    target = Path(tmp) / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
            current = cm.work_view_entities(Path(tmp) / "work-view")
        have = fold_entity_rows(out.get(cm.WORK_VIEW_LOG))
        at = int(meta["at"])
        rows = [{"at": at, "data": d, "id": i, "kind": k} for (k, i), d in sorted(current.items()) if have.get((k, i)) != d]
        rows += [{"at": at, "data": None, "id": i, "kind": k} for (k, i) in sorted(have) if (k, i) not in current]
        if rows:
            out[cm.WORK_VIEW_LOG] = out.get(cm.WORK_VIEW_LOG, b"") + cm.jsonl(rows).encode("utf-8")
        lines += len(rows)

    out["manifest.json"] = (json.dumps({
        "format": cm.FORMAT, "device_id": device, "device_name": manifest.get("device_name"),
        "claude_mem_version": manifest.get("claude_mem_version"), "counts": live,
    }, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return lines, len(fresh_tombstones), manifest.get("device_name") or device


def rebuild(repo, source, dest, replace):
    if run(repo, "rev-parse", "--verify", "--quiet", f"refs/heads/{dest}", check=False).strip() and not replace:
        raise SystemExit(f"branch {dest} exists; pass --replace to overwrite it")
    commits = history(repo, source)
    tip_names = {}
    for path in tree_files(repo, commits[-1], DEVICES):
        parts = path.split("/")
        tip_names.setdefault(parts[2], set()).add("/".join(parts[3:]))
    tree, parent = {}, None
    emitted, pending = {}, {}
    stats = {"hand": 0, "cm": 0, "skipped": 0}

    def blob(data):
        return run(repo, "hash-object", "-w", "--stdin", input=data).decode().strip()

    def commit(files, message, meta):
        nonlocal parent
        with tempfile.TemporaryDirectory(prefix="cm-rebuild-") as tmp:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(tmp) / "index"))
            info = "".join(f"{mode} {sha}\t{path}\0" for path, (mode, sha) in sorted(files.items()))
            run(repo, "update-index", "-z", "--add", "--index-info", input=info.encode("utf-8"), env=env)
            tree_sha = run(repo, "write-tree", env=env).decode().strip()
        env = dict(os.environ, GIT_AUTHOR_NAME=meta["an"], GIT_AUTHOR_EMAIL=meta["ae"], GIT_AUTHOR_DATE=meta["ai"],
                   GIT_COMMITTER_NAME=meta["cn"], GIT_COMMITTER_EMAIL=meta["ce"], GIT_COMMITTER_DATE=meta["ci"])
        args = ["commit-tree", tree_sha] + (["-p", parent] if parent else [])
        parent = run(repo, *args, input=message.encode("utf-8"), env=env).decode().strip()

    def flush(device):
        nonlocal tree
        group = pending.pop(device)
        out = emitted.setdefault(device, {})
        rows, tombstones, name = append_day(repo, out, group["last"], group["meta"], device, tip_names.get(device, set()))
        prefix = f"{DEVICES}/{device}/"
        files = {p: v for p, v in tree.items() if not p.startswith(prefix)}
        files.update({prefix + n: ("100644", blob(data)) for n, data in out.items() if data})
        if files == tree:
            stats["skipped"] += 1
            return
        tree = files
        extra = f", {tombstones} tombstones" if tombstones else ""
        commit(tree, f"cm-sync: {name} +{rows} rows{extra}\n", group["meta"])
        stats["cm"] += 1

    def flush_all():
        for device in sorted(pending, key=lambda d: pending[d]["idx"]):
            flush(device)

    for idx, sha in enumerate(commits):
        meta = commit_meta(repo, sha)
        kind, device = classify(changed_paths(repo, sha))
        if kind == "cm":
            day = meta["ai"][:10]
            if device in pending and pending[device]["day"] != day:
                flush(device)
            pending[device] = {"day": day, "last": sha, "meta": meta, "idx": idx}
            continue
        flush_all()
        files = {p: v for p, v in tree.items() if p.startswith(DEVICES + "/")}
        files.update({p: v for p, v in tree_files(repo, sha).items() if not p.startswith(DEVICES + "/")})
        tree = files
        commit(tree, meta["message"].rstrip("\n") + "\n", meta)
        stats["hand"] += 1
    flush_all()

    run(repo, "update-ref", f"refs/heads/{dest}", parent)
    print(f"{len(commits)} commits on {source} → {stats['hand']} hand + {stats['cm']} cm-sync commits on {dest} "
          f"({stats['skipped']} empty days skipped), tip {parent[:10]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("repo", type=Path)
    parser.add_argument("--source", default="main")
    parser.add_argument("--dest", default="rebuilt")
    parser.add_argument("--replace", action="store_true", help="overwrite the destination branch if it exists")
    args = parser.parse_args()
    rebuild(args.repo.expanduser(), args.source, args.dest, args.replace)


if __name__ == "__main__":
    main()
