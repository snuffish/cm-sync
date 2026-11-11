#!/usr/bin/env python3
"""Prove a rebuilt branch holds the same memory as the branch it replaces; exit 1 on any difference.

    python3 tools/verify_rebuild.py ~/.claude-memory main rebuilt

Checks, per device: live rows of every kind, tombstone keys, work-view entities and device name. Across the repo:
every file outside claude-mem/devices, every hand-made commit (message, date, files), and that no *.jsonl file ever
loses a line in the rebuilt history.
"""
import argparse
import io
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cm_sync as cm  # noqa: E402
from rebuild_history import DEVICES, classify, changed_paths, commit_meta, history, run  # noqa: E402


def export_devices(repo, ref, dest):
    data = run(repo, "archive", "--format=tar", ref, DEVICES, check=False)
    if data:
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            tar.extractall(dest, filter="data")
    root = Path(dest) / DEVICES
    return {p.name: p for p in sorted(root.iterdir())} if root.is_dir() else {}


def entities(device_path):
    if device_path is None:
        return {}
    if (device_path / cm.WORK_VIEW_LOG).is_file():
        return cm.fold_entities(device_path / cm.WORK_VIEW_LOG)
    if (device_path / "work-view").is_dir():
        return cm.work_view_entities(device_path / "work-view")
    return {}


def compare_devices(repo, old_ref, new_ref, problems):
    with tempfile.TemporaryDirectory(prefix="cm-verify-") as tmp:
        old = export_devices(repo, old_ref, Path(tmp) / "old")
        new = export_devices(repo, new_ref, Path(tmp) / "new")
        for device in sorted(old.keys() | new.keys()):
            o, n = old.get(device), new.get(device)
            if o is None or n is None:
                problems.append(f"{device}: device folder only on {'old' if n is None else 'new'}")
                continue
            for kind in cm.KINDS:
                a, b = cm.live_rows(o, kind), cm.live_rows(n, kind)
                if a != b:
                    problems.append(f"{device} {kind}: live rows differ (old {len(a)}, new {len(b)}, "
                                    f"only old {len(a.keys() - b.keys())}, only new {len(b.keys() - a.keys())}, "
                                    f"changed {sum(1 for k in a.keys() & b.keys() if a[k] != b[k])})")
            ta, tb = cm.tombstone_keys(o), cm.tombstone_keys(n)
            if ta != tb:
                problems.append(f"{device} tombstones differ (old {len(ta)}, new {len(tb)})")
            ea, eb = entities(o), entities(n)
            if ea != eb:
                problems.append(f"{device} work-view differs (old {len(ea)} entities, new {len(eb)}, "
                                f"only old {len(ea.keys() - eb.keys())}, only new {len(eb.keys() - ea.keys())}, "
                                f"changed {sum(1 for k in ea.keys() & eb.keys() if ea[k] != eb[k])})")
            ma, mb = cm.read_json(o / "manifest.json", {}), cm.read_json(n / "manifest.json", {})
            if (ma.get("device_id"), ma.get("device_name")) != (mb.get("device_id"), mb.get("device_name")):
                problems.append(f"{device} manifest identity differs")
        return len(old)


def hand_commits(repo, ref):
    return [(sha, commit_meta(repo, sha)) for sha in history(repo, ref) if classify(changed_paths(repo, sha))[0] == "hand"]


def compare_hand_commits(repo, old_ref, new_ref, problems):
    old, new = hand_commits(repo, old_ref), hand_commits(repo, new_ref)
    if len(old) != len(new):
        problems.append(f"hand-made commits: {len(old)} on {old_ref}, {len(new)} on {new_ref}")
    for (osha, o), (nsha, n) in zip(old, new):
        if (o["message"], o["ai"], o["an"]) != (n["message"], n["ai"], n["an"]):
            problems.append(f"hand-made commit {osha[:9]} differs in message, date or author")
        diff = run(repo, "diff", "--name-only", osha, nsha, "--", ".", f":(exclude){DEVICES}", check=False).decode().split()
        if diff:
            problems.append(f"hand-made commit {osha[:9]}: files differ: {', '.join(diff[:5])}")
    return len(old)


def deleted_jsonl_lines(repo, ref):
    out = run(repo, "log", ref, "--numstat", "--format=", "--", f":(glob){DEVICES}/*/*.jsonl").decode()
    return sum(int(line.split("\t")[1]) for line in out.splitlines() if line and line.split("\t")[1].isdigit())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("repo", type=Path)
    parser.add_argument("old")
    parser.add_argument("new")
    args = parser.parse_args()
    repo = args.repo.expanduser()
    problems = []

    devices = compare_devices(repo, args.old, args.new, problems)
    hand = compare_hand_commits(repo, args.old, args.new, problems)
    outside = run(repo, "diff", "--name-only", args.old, args.new, "--", ".", f":(exclude){DEVICES}").decode().split()
    if outside:
        problems.append(f"files outside {DEVICES} differ: {', '.join(outside[:8])}")
    deleted = deleted_jsonl_lines(repo, args.new)
    if deleted:
        problems.append(f"{args.new} deletes {deleted} lines from *.jsonl files")

    old_count = len(history(repo, args.old))
    new_count = len(history(repo, args.new))
    print(f"{devices} devices, {hand} hand-made commits compared; commits {old_count} → {new_count}")
    for problem in problems:
        print("✗", problem)
    if problems:
        sys.exit(1)
    print("✓ identical rows, tombstones, work-view and notes; no *.jsonl line ever deleted")


if __name__ == "__main__":
    main()
