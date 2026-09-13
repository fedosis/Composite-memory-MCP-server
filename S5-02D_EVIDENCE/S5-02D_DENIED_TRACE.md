# S5-02D denied-operation trace

Measured with the certified gate runner against the pristine pinned baseline.

1. SQLite scratch-path construction is `/tmp/test_outbox_<uuid>.db` at `tests/test_outbox.py:44`. The resulting SQLAlchemy URL is built at line 45 and the sandbox denies the subsequent `sqlite3.connect('/tmp/test_outbox_<uuid>.db')` operation, reported as `sqlite3.OperationalError: unable to open database file`.
2. Migration scratch-path construction is `tests/tmp_outbox_migration_<uuid>.db` at `tests/test_outbox.py:1226` and the INI path is `/tmp/cmms-baseline-da1cd0e/tmp_outbox_migration_<uuid>.ini` at line 1227. The write operation is `open(ini_path, 'w')` at line 1230 followed by `stream.write(ini_text)` at line 1231. The sandbox refuses the project-tree INI write path (and the test's project-tree database target); in the captured run the setup failures are dominated by the SQLite open denial before migration can complete.

The complete reproduction is in `S5-02D_BASELINE_REPRO.raw`; its summary is `16 failed, 5 passed, 26 errors`, with the concrete refused DB path visible as `/tmp/test_outbox_*.db` and the exception text above.
