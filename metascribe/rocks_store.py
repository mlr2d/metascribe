import os
import time
import json
import io
import pandas as pd
from rocksdict import Rdict, Options, AccessType
import logging
logger = logging.getLogger(__name__)

class RocksStore:
    def __init__(self, db_path, lock=False, lock_timeout=300, force_open=False):
        self.db_path = db_path
        self.lock_path = f"{db_path}.lock"
        self._store = None
        self.lock = lock
        
        parent_dir = os.path.dirname(os.path.abspath(db_path))
        if parent_dir and not os.path.exists(parent_dir):
            try:
                os.makedirs(parent_dir, exist_ok=True)
                logger.info(f"Created parent directory for RocksDB at {parent_dir}")
            except Exception as e:
                raise IOError(f"Could not create parent directory {parent_dir}: {e}")
        
        # 1. Force open logic
        if force_open and os.path.exists(self.lock_path):
            os.remove(self.lock_path)
        
        # 2. Manual Locking Logic 
        if self.lock:
            t0 = time.time()
            while True:
                try:
                    # Atomic file creation for locking
                    os.close(os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
                    break
                except FileExistsError:
                    if time.time() - t0 > lock_timeout:
                        raise TimeoutError(f"Lock timeout: {self.lock_path}")
                    time.sleep(1)  
        
        # 3. Open the DB
        try:
            self._store = Rdict(db_path)
        except Exception as e:
            if self.lock and os.path.exists(self.lock_path):
                os.remove(self.lock_path)
            raise IOError(f"Could not open RocksDB at {db_path}: {e}")

    def put_df(self, key: str, df: pd.DataFrame):
        # We use parquet for storage to keep it "object-like" and efficient
        self._store[key] = df.to_parquet()
        
    def get_df(self, key: str) -> pd.DataFrame:
        if key not in self._store:
            raise KeyError(f"Key {key} not found in RocksStore at {self.db_path}")
            
        raw_data = self._store[key]
        return pd.read_parquet(io.BytesIO(raw_data))

    def put_json(self, key: str, obj: dict):
        json_str = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        self._store[key] = json_str
        
    def get_json(self, key: str) -> dict:
        if key not in self._store:
            raise KeyError(f"Key {key} not found")
            
        return json.loads(self._store[key])
    
    def put_string(self, key: str, value: str):
        self._store[key] = value
        
    def get_string(self, key: str) -> str:
        if key not in self._store:
            raise KeyError(f"Key {key} not found")
            
        return str(self._store[key])
    
    # def keys_with_prefix(self, prefix: str):
        
    
    def close(self):
        if self._store is not None:
            self._store.close()
            if self.lock:
                if os.path.exists(self.lock_path):
                    os.remove(self.lock_path)    
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()