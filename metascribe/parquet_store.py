import os, time, uuid, json
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb, json
import pandas as pd
import socket


def make_writer_id():
    host = socket.gethostname()
    return f"{time.strftime('%Y%m%dT%H%M%S')}-{host}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

class ParquetStoreWriter:
    """
    Writes two append-only Parquet datasets:
      - tables: homogeneous DF rows with (key, row, <cols...>)
      - objects: nested dicts stored as JSON strings with (key, json, kind, ts)
    Each process gets its own writer id -> no shared writable files -> multi-writer safe.
    """
    def __init__(self, store_root: str, writer_id=None,
                 table_shard_rows=5_000_000,
                 obj_shard_rows=1_000_000):
        self.root = Path(store_root)
        self.writer_id = writer_id or make_writer_id()

        self.tables_dir = self.root / "tables" / f"writer={self.writer_id}"
        self.objects_dir = self.root / "objects" / f"writer={self.writer_id}"
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)

        self.table_shard_rows = int(table_shard_rows)
        self.obj_shard_rows = int(obj_shard_rows)

        self._table_buf = []
        self._table_buf_rows = 0
        self._obj_buf = []
        self._obj_buf_rows = 0

        self._table_shard_id = 1
        self._obj_shard_id = 1

    # ---------- TABLES ----------
    def add_table(self, key: str, df: pd.DataFrame):
        """
        Store a small homogeneous table as rows in the big dataset.
        """
        df2 = df.copy()
        df2.insert(0, "key", key)
        df2.insert(1, "row", range(len(df2)))
        self._table_buf.append(df2)
        self._table_buf_rows += len(df2)

        if self._table_buf_rows >= self.table_shard_rows:
            self.flush_tables()

    def flush_tables(self):
        if not self._table_buf:
            return
        big = pd.concat(self._table_buf, ignore_index=True)
        table = pa.Table.from_pandas(big, preserve_index=False)

        tmp = self.tables_dir / f"shard-{self._table_shard_id:06d}.parquet.tmp-{os.getpid()}"
        final = self.tables_dir / f"shard-{self._table_shard_id:06d}.parquet"
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, final)  # atomic
        self._table_buf.clear()
        self._table_buf_rows = 0
        self._table_shard_id += 1

    # ---------- OBJECTS (nested dicts / JSON) ----------
    def put_object(self, key: str, obj: dict, kind: str = "json", ts: float = None):
        """
        Store a nested dict as JSON (string). You can add kind/version fields if needed.
        """
        if ts is None:
            ts = time.time()

        self._obj_buf.append({
            "key": key,
            "kind": kind,
            "ts": float(ts),
            "json": json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
        })
        self._obj_buf_rows += 1

        if self._obj_buf_rows >= self.obj_shard_rows:
            self.flush_objects()

    def flush_objects(self):
        if not self._obj_buf:
            return
        df = pd.DataFrame(self._obj_buf)
        table = pa.Table.from_pandas(df, preserve_index=False)

        tmp = self.objects_dir / f"shard-{self._obj_shard_id:06d}.parquet.tmp-{os.getpid()}"
        final = self.objects_dir / f"shard-{self._obj_shard_id:06d}.parquet"
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, final)  # atomic
        self._obj_buf.clear()
        self._obj_buf_rows = 0
        self._obj_shard_id += 1

    def close(self):
        self.flush_tables()
        self.flush_objects()
        


def parquet_read(store_root: str, prefix: str):
    con = duckdb.connect()

    # Read matching table rows
    tables_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{store_root}/tables/writer=*/shard-*.parquet')
        WHERE starts_with(key, '{prefix}')
    """).fetch_df()

    # Read matching objects
    objs_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{store_root}/objects/writer=*/shard-*.parquet')
        WHERE starts_with(key, '{prefix}')
        ORDER BY ts
    """).fetch_df()

    # Reconstruct small tables: dict[key] -> dataframe (drop key, row)
    table_map = {}
    if not tables_df.empty:
        for k, g in tables_df.groupby("key"):
            g2 = g.drop(columns=["key"]).sort_values("row").drop(columns=["row"])
            table_map[k] = g2.reset_index(drop=True)

    # Decode JSON objects: list of (key, dict)
    obj_list = []
    if not objs_df.empty:
        for _, r in objs_df.iterrows():
            obj_list.append((r["key"], json.loads(r["json"])))

    return table_map, obj_list
