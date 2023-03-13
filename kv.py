import sqlite3, os.path, pathlib, json
from typing import Any

class KV:

    def _con(self):
        return sqlite3.connect(os.path.realpath(self._path))

    def __init__(self, path: str):
        statement_create_tbl = 'CREATE TABLE IF NOT EXISTS data (k text, v text)'

        statement_create_index = 'CREATE UNIQUE INDEX IF NOT EXISTS idx_k ON data (k)'

        self._path = path

        pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
        
        with self._con() as con:
            con.execute(statement_create_tbl)
            con.execute(statement_create_index)
            con.commit()

    def is_empty(self) -> bool:
        statement_select = 'SELECT COUNT(*) FROM data'
        with self._con() as con:
            cur = con.cursor()
            cur.execute(statement_select)
            entry = cur.fetchone()
            return entry[0] == 0

    def get_value(self, k: str) -> Any:
        statement_select = 'SELECT v FROM data WHERE k = ?'
        with self._con() as con:
            cur = con.cursor()
            cur.execute(statement_select, (k,))
            entry = cur.fetchone()
            if entry:
                try:
                    d = json.loads(entry[0])
                    return d
                except:
                    return entry[0]
            return None

    def get_keys(self) -> list[str]:
        statement_select = 'SELECT k FROM data'
        with self._con() as con:
            cur = con.cursor()
            cur.execute(statement_select)
            keys = [ entry[0] for entry in cur.fetchall() ]
            return keys

    def _set_value(self, con, k: str, v: Any) -> None:
        statement_update = 'UPDATE data SET v = ? WHERE k = ?'

        statement_insert = 'INSERT INTO data VALUES (?, ?)'

        if type(v) is list or type(v) is dict:
            v = json.dumps(v)
        cur = con.execute(statement_update, (v, k))
        if cur.rowcount == 0:
                con.execute(statement_insert, (k, v))

    def set_value(self, k: str, v: Any) -> None:
        with self._con() as con:
            self._set_value(con, k, v)
            con.commit()


    def set_many(self, kv_list: list[tuple[str,Any]]) -> None:
        with self._con() as con:
            for k,v in kv_list:
                self._set_value(con, k, v)
            con.commit()
