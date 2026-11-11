# cm-sync

Keeps [claude-mem](https://github.com/thedotmack/claude-mem) memory the same on every computer, through a private git repo.

Each device publishes only the rows it created (as JSON Lines) and its work-view ledger, append-only, in `<memory repo>/claude-mem/devices/<device-id>/`, and imports the other devices' rows through its own claude-mem worker (`POST /api/import`), so search and vector indexes stay correct. The one exception is work state (see below); nothing else writes to `claude-mem.db` directly. Published files only ever grow: a changed row or ledger entity is added as a new line, and the last line per key wins on read. Python 3.9+ standard library only; macOS, Linux and Windows.

## Install

```bash
git clone https://github.com/snuffish/cm-sync ~/cm-sync
ln -sf ~/cm-sync/cm-sync ~/.local/bin/cm-sync   # or any directory on PATH
```

On Windows, clone the same way and add the clone's folder to your user `PATH`; `cm-sync.cmd` runs it from cmd and PowerShell (through `py -3`, or `python` when the launcher is missing). Git Bash can use the `cm-sync` script as on macOS and Linux.

The memory repo defaults to `~/.claude-memory`. If it doesn't exist yet, `cm-sync setup` asks for its git URL and clones it there. Override the path with `CM_SYNC_REPO` / `--repo`, and preset the URL with `CM_SYNC_REMOTE` (needed for `setup --yes` or non-interactive runs).

On a new machine:

```bash
cm-sync setup   # checks bun/node/git, the plugin and worker, clones the memory repo, turns on redaction, loads My Rules, names the device
cm-sync         # same as `cm-sync sync`
```

## Commands

| Command                                 | Does                                                                                                                                            |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `cm-sync` / `cm-sync sync`              | Commit changed notes → pull → import → export → commit this device's folder → push (sync is the default command)                               |
| `cm-sync import`                        | Pull the memory repo, then import other devices' rows and apply their deletions (`--no-pull` to skip the pull, `--full` after a database reset) |
| `cm-sync export`                        | Write this device's rows and work-view ledger to `claude-mem/devices/<device-id>/` (`--with-prompts` to include raw prompts)                    |
| `cm-sync status`                        | Worker, redaction, active My Rules, work-view status (pending rule decisions, progress groups), counts per device, last push                    |
| `cm-sync url`                           | Print the worker URL (`CLAUDE_MEM_WORKER_HOST`/`PORT` from env, then settings, then `127.0.0.1:37700 + uid % 100`, `37777` on Windows)          |
| `cm-sync hooks`                         | Print Claude Code hooks that import on session start and sync on session end                                                                    |
| `cm-sync refile <session-id> <project>` | Move a session's observations and summaries to another project (claude-mem files a session under the folder it started in)                      |

## How it works

- Work state (the lists behind claude-mem's "My Work" page) syncs too. No worker route keeps its timestamps, and claude-mem decides each task's current status by the latest timestamp, so cm-sync inserts other devices' entries into `work_state_entries` directly, the way claude-mem's own cloud sync does: original `created_at_epoch`, plus `origin_device_id`/`origin_local_id` so re-imports are no-ops and imported entries are never published again. It needs claude-mem v15+, and is skipped with a warning on older versions. The tool calls that link a list to the request it came from don't sync, so on other devices the lists can show under "work records need a linked human request" rather than as cards. Pick "All projects" and a wide enough time range to see older entries.
- My Rules sync with work state: rules you set, approve, edit, turn off or dismiss are work-state entries (list `my-rules`). claude-mem rebuilds `~/.claude-mem/MY-RULES.md` only for rules its own cloud sync applies, so after every import cm-sync asks the worker to rebuild it (`POST /api/work-view/rules/load-in-claude-code`, claude-mem 15.1+). It does that only once `~/.claude/CLAUDE.md` loads the file; `cm-sync setup` offers to add that line, so run it once on each device.
- Work-view ledger and rule suggestions sync too: `~/.claude-mem/work-view/` (`story.json`, `request-groups.json`, `progress/`, `alignment.json`) is published as `work-view.jsonl` under each device's folder: one line per changed entity (a progress group, a request group, a conversation, a rule suggestion, the alignment, the story text), and a line with null data when an entity goes away. On import, unreviewed rule suggestions ("Needs your decision") are shared across devices, and any rule resolved on any device (`on`, `off`, `dismissed`, `replaced` in `work_state_entries`) is automatically pruned from `story.json`. Request groups and conversations are additively unioned, progress entries pick the newest check-in, and alignment picks the latest.
- Deleting a row on the device that created it publishes a tombstone, and the other devices delete their copy on the next import.
- Export refuses to run while claude-mem redaction is off and skips `sensitive` observations. When a row looks like a secret it asks whether to delete it, otherwise it aborts (`--exclude-flagged` keeps those rows local instead).
- When `gitleaks` is installed, `sync` scans this device's folder before committing. Tombstone keys (cm-sync's own row hashes) are never flagged. For any other finding, a terminal run asks whether it is a false positive; a yes stores the rule and a SHA-256 of the match (never the match itself) in `gitleaks-allowed.keys` in the state dir, so that finding no longer blocks a sync on this device even when its line moves. Hook runs have no terminal, so they abort and list the findings.
- Memory notes sync too. Before each pull, `sync` commits changed notes (top-level `*.md`, `unslop-voice/`, `.gitattributes`) in their own `memory: <device> +added ~changed -deleted` commit, so the pull has none of them to stash. It first keeps `MEMORY.md` in step with the files: a line is added for an unindexed note (from its `description`) and dropped when its file is gone; lines that stay are never rewritten. Frontmatter problems (missing name, description or type, name not matching the file, a type outside user/feedback/project/reference) and duplicate index lines print a warning. A gitleaks finding in a note blocks the commit like one in a device folder. Add `MEMORY.md merge=union` to the memory repo's `.gitattributes` so two devices adding index lines don't conflict.
- Nothing else in the memory repo is staged: the device commit takes only this device's folder.
- Format 2 is append-only. A cm-sync that finds a device folder or manifest with a newer `format` skips it on import and refuses to export into it; it reads format 1 folders (including the old `work-view/` snapshot folder) as before.
- Local state (device id, imported/published/tombstoned keys) lives in `~/.local/state/cm-sync/` (`%USERPROFILE%\.local\state\cm-sync` on Windows).
- Files are written with LF line endings on every platform, so a Windows device's export is byte-identical to a Mac's.

If a secret reaches a commit anyway, deleting the row is not enough: rewrite the memory repo's history and re-clone it on every device.

```bash
git -C ~/.claude-memory filter-repo --path claude-mem/devices/<device-id>/<file> --invert-paths
git -C ~/.claude-memory push --force
```

## Rebuilding an old history

Repos written by cm-sync 1 hold a rewritten copy of every file in nearly every commit. `tools/rebuild_history.py` replays such a history onto a new branch as append-only commits: hand-made commits keep their message, dates and files, and the sync commits collapse to one per device per day. `tools/verify_rebuild.py` proves the result holds the same live rows, tombstones, work-view entities and notes as the old branch, and that no `*.jsonl` file ever loses a line. Raw prompts that the old tip no longer publishes are not replayed. Only the new branch is written, so run both on a throwaway clone first:

```bash
python3 tools/rebuild_history.py ~/.claude-memory --source main --dest rebuilt
python3 tools/verify_rebuild.py ~/.claude-memory main rebuilt   # exit 1 on any difference
```

Publishing the branch is a force-push: stop every device's sync first, then re-clone the memory repo on the other devices.

## Tests

```bash
python3 -I -m unittest discover -s ~/Projects/cm-sync -p 'test_*.py'   # `py -3` instead of python3 on Windows
```
