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
            CREATE TABLE IF NOT EXISTS outgoing_parts (
                scope TEXT NOT NULL, message_id TEXT NOT NULL,
                part_no INTEGER NOT NULL, text TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'not_sent', qq_message_id TEXT,
                PRIMARY KEY(scope,message_id,part_no));
        ''')
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(turns)')}
        if 'context_user_text' not in columns:
            self.db.execute('ALTER TABLE turns ADD COLUMN context_user_text TEXT')
        with self.db:
            interrupted = self.db.execute("SELECT scope,message_id,state FROM turns WHERE state IN ('queued','processing','sending')").fetchall()
            self.db.execute("UPDATE outgoing_parts SET state='unknown' WHERE state='sending'")
            self.db.execute("UPDATE turns SET state='interrupted' WHERE state IN ('queued','processing','sending')")
            for scope, mid, previous in interrupted:
                prefix = self.confirmed_prefix(scope, mid)
                if prefix:
                    self.finish(scope, mid, prefix, 'partial')
                elif previous == 'sending':
                    self.finish(scope, mid, None, 'unknown')
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
        rows = self.db.execute("SELECT COALESCE(context_user_text,user_text),reply FROM turns WHERE scope=? AND state IN ('delivered','partial') AND created>=? ORDER BY created DESC,rowid DESC LIMIT 6",
                               (scope, time.time() - 30 * 86400)).fetchall()
        return [message for user, reply in reversed(rows) for message in
                ({'role': 'user', 'content': user}, {'role': 'assistant', 'content': reply})]

    def prepare_batch(self, scope, mids, text):
        with self.db:
            self.db.execute("UPDATE turns SET context_user_text=?,state='processing' WHERE scope=? AND message_id=?",
                            (text, scope, str(mids[0])))
            for mid in mids[1:]:
                self.db.execute("UPDATE turns SET state='merged' WHERE scope=? AND message_id=?", (scope, str(mid)))

    def prepare_parts(self, scope, mid, parts):
        with self.db:
            self.db.executemany('INSERT INTO outgoing_parts(scope,message_id,part_no,text) VALUES(?,?,?,?)',
                                ((scope, str(mid), i, part) for i, part in enumerate(parts)))

    def mark_part(self, scope, mid, number, state, qq_mid=None):
        with self.db:
            self.db.execute('UPDATE outgoing_parts SET state=?,qq_message_id=? WHERE scope=? AND message_id=? AND part_no=?',
                            (state, str(qq_mid) if qq_mid is not None else None, scope, str(mid), number))

    def delivery_parts(self, scope, mid):
        return [dict(zip(('text','state','qq_message_id'), row)) for row in self.db.execute(
            'SELECT text,state,qq_message_id FROM outgoing_parts WHERE scope=? AND message_id=? ORDER BY part_no', (scope, str(mid)))]

    def confirmed_prefix(self, scope, mid):
        parts = []
        for row in self.delivery_parts(scope, mid):
            if row['state'] != 'delivered':
                break
            parts.append(row['text'])
        return '\n'.join(parts)

    def reserve_usage(self, day, scope, limit):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO usage VALUES(?,?,0)', (day, scope))
            result = self.db.execute('UPDATE usage SET calls=calls+1 WHERE day=? AND scope=? AND calls<?',
                                     (day, scope, limit))
        return result.rowcount == 1

    def cleanup(self):
        with self.db:
            self.db.execute('DELETE FROM outgoing_parts WHERE EXISTS (SELECT 1 FROM turns WHERE turns.scope=outgoing_parts.scope AND turns.message_id=outgoing_parts.message_id AND turns.created<?)', (time.time() - 30 * 86400,))
            self.db.execute('DELETE FROM turns WHERE created<?', (time.time() - 30 * 86400,))

    def close(self):
        self.db.close()
