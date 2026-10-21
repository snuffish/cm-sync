# cm-sync

Keeps [claude-mem](https://github.com/thedotmack/claude-mem) memory the same on every computer, through a private git repo.

Each device publishes only the rows it created, as JSON Lines in `<memory repo>/claude-mem/devices/<device-id>/`, and imports the other devices' rows through its own claude-mem worker (`POST /api/import`), so search and vector indexes stay correct. The one exception is work state (see below); nothing else writes to `claude-mem.db` directly. Python 3.9+ standard library only; macOS, Linux and Windows.

## Install

```bash
git clone git@github.com:your-username/cm-sync.git ~/Projects/cm-sync
ln -sf ~/Projects/cm-sync/cm-sync ~/.local/bin/cm-sync   # or any directory on PATH
```

On Windows, clone the same way and add the clone's folder to your user `PATH`; `cm-sync.cmd` runs it from cmd and PowerShell (through `py -3`, or `python` when the launcher is missing). Git Bash can use the `cm-sync` script as on macOS and Linux.

The memory repo defaults to `~/claude-memory`. If it doesn't exist yet, `cm-sync setup` asks for its git URL and clones it there. Override the path with `CM_SYNC_REPO` / `--repo`, and preset the URL with `CM_SYNC_REMOTE` (needed for `setup --yes` or non-interactive runs).

On a new machine:

```bash
cm-sync setup   # checks bun/node/git, the plugin and worker, clones the memory repo, turns on redaction, names the device
cm-sync         # same as `cm-sync sync`
```

## Commands

| Command                                 | Does                                                                                                                                            |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `cm-sync` / `cm-sync sync`              | Pull → import → export → commit only this device's folder → push (sync is the default command)                                                  |
| `cm-sync import`                        | Pull the memory repo, then import other devices' rows and apply their deletions (`--no-pull` to skip the pull, `--full` after a database reset) |
| `cm-sync export`                        | Write this device's rows to `claude-mem/devices/<device-id>/` (`--with-prompts` to include raw prompts)                                         |
| `cm-sync status`                        | Worker, redaction, counts per device, last push                                                                                                 |
| `cm-sync url`                           | Print the worker URL (`CLAUDE_MEM_WORKER_HOST`/`PORT` from env, then settings, then `127.0.0.1:37700 + uid % 100`, `37777` on Windows)          |
| `cm-sync hooks`                         | Print Claude Code hooks that import on session start and sync on session end                                                                    |
| `cm-sync refile <session-id> <project>` | Move a session's observations and summaries to another project (claude-mem files a session under the folder it started in)                      |

## How it works

- Work state (the lists behind claude-mem's "My Work" page) syncs too. No worker route keeps its timestamps, and claude-mem decides each task's current status by the latest timestamp, so cm-sync inserts other devices' entries into `work_state_entries` directly, the way claude-mem's own cloud sync does: original `created_at_epoch`, plus `origin_device_id`/`origin_local_id` so re-imports are no-ops and imported entries are never published again. It needs claude-mem v15+, and is skipped with a warning on older versions. The tool calls that link a list to the request it came from don't sync, so on other devices the lists can show under "work records need a linked human request" rather than as cards. Pick "All projects" and a wide enough time range to see older entries.
- Deleting a row on the device that created it publishes a tombstone, and the other devices delete their copy on the next import.
- Export refuses to run while claude-mem redaction is off and skips `sensitive` observations. When a row looks like a secret it asks whether to delete it, otherwise it aborts (`--exclude-flagged` keeps those rows local instead).
- Only this device's folder is staged and committed, so other files in the memory repo are never touched.
- Local state (device id, imported/published/tombstoned keys) lives in `~/.local/state/cm-sync/` (`%USERPROFILE%\.local\state\cm-sync` on Windows).
- Files are written with LF line endings on every platform, so a Windows device's export is byte-identical to a Mac's.

If a secret reaches a commit anyway, deleting the row is not enough: rewrite the memory repo's history and re-clone it on every device.

```bash
git -C ~/claude-memory filter-repo --path claude-mem/devices/<device-id>/<file> --invert-paths
git -C ~/claude-memory push --force
```

## Tests

```bash
python3 -I -m unittest discover -s ~/Projects/cm-sync -p 'test_*.py'   # `py -3` instead of python3 on Windows
```
