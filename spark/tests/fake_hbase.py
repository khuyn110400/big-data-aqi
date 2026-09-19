"""Bảng HBase giả trong bộ nhớ — cùng giao diện happybase.Table mà hbase_sink dùng."""
from contextlib import contextmanager


class _Batch:
    def __init__(self, table):
        self._table = table

    def put(self, key, data):
        self._table.rows.setdefault(key, {}).update(data)


class FakeTable:
    def __init__(self):
        self.rows: dict[bytes, dict[bytes, bytes]] = {}

    @contextmanager
    def batch(self, batch_size=None):
        yield _Batch(self)

    def scan(self, row_start=None, row_stop=None, columns=None, **_):
        for key in sorted(self.rows):
            if row_start is not None and key < row_start:
                continue
            if row_stop is not None and key >= row_stop:
                continue
            data = self.rows[key]
            if columns is not None:
                data = {c: v for c, v in data.items() if c in columns}
            if not data:
                continue
            yield key, dict(data)

    def row(self, key):
        return dict(self.rows.get(key, {}))
