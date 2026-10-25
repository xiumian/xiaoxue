"""Local SQLite state; no credentials or automatic fact extraction."""
import sqlite3
import time
from pathlib import Path


class Store:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA secure_delete=ON')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS turns (
                scope TEXT NOT NULL, message_id TEXT NOT NULL,
                user_text TEXT NOT NULL, reply TEXT,
                state TEXT NOT NULL DEFAULT 'queued', created REAL NOT NULL,
                PRIMARY KEY(scope,message_id));
            CREATE TABLE IF NOT EXISTS usage (
                day TEXT NOT NULL, scope TEXT NOT NULL, calls INTEGER NOT NULL,
                PRIMARY KEY(day,scope));
            CREATE INDEX IF NOT EXISTS turn_history ON turns(scope,created);
        ''')
        with self.db:
            self.db.execute("UPDATE turns SET state='interrupted' WHERE state IN ('queued','processing','sending')")
        self.cleanup()

    def claim(self, scope, mid, text):
        with self.db:
            result = self.db.execute('INSERT OR IGNORE INTO turns(scope,message_id,user_text,created) VALUES(?,?,?,?)',
                                     (scope, str(mid), text, time.time()))
        return result.rowcount == 1

    def finish(self, scope, mid, reply, state):
        with self.db:
            self.db.execute('UPDATE turns SET reply=?,state=? WHERE scope=? AND message_id=?',
                            (reply, state, scope, str(mid)))

    def state(self, scope, mid):
        row = self.db.execute('SELECT state FROM turns WHERE scope=? AND message_id=?', (scope, str(mid))).fetchone()
        return row[0] if row else None

    def history(self, scope):
        rows = self.db.execute("SELECT user_text,reply FROM turns WHERE scope=? AND state='delivered' AND created>=? ORDER BY created DESC,rowid DESC LIMIT 6",
                               (scope, time.time() - 30 * 86400)).fetchall()
        return [message for user, reply in reversed(rows) for message in
                ({'role': 'user', 'content': user}, {'role': 'assistant', 'content': reply})]

    def reserve_usage(self, day, scope, limit):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO usage VALUES(?,?,0)', (day, scope))
            result = self.db.execute('UPDATE usage SET calls=calls+1 WHERE day=? AND scope=? AND calls<?',
                                     (day, scope, limit))
        return result.rowcount == 1

    def cleanup(self):
        with self.db:
            self.db.execute('DELETE FROM turns WHERE created<?', (time.time() - 30 * 86400,))

    def close(self):
        self.db.close()
