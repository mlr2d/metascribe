# Linnea Inspector
#
# Copyright (c) 2021-2025, High-Performance and Automatic Computing group
# at RWTH Aachen University and Umeå University.
# All rights reserved.
#
# Licensed under the BSD 3-Clause License.
# See LICENSE file in the project root for full license information.
#
# Contributors:
# - Aravind Sankaran

import pandas as pd
import time
import os
import h5py
import json


class H5Store:
    def __init__(self, h5_path, mode='a', lock=False, lock_timeout=300, force_open=False):
        self.h5_path = h5_path
        self.lock_path = f"{h5_path}.lock"  # Standard lock naming
        self._store = None
        self.lock = lock
        
        if force_open and os.path.exists(self.lock_path):
            os.remove(self.lock_path)
        
        if self.lock:
            t0 = time.time()
            while True:
                try:
                    os.close(os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
                    break
                except FileExistsError:
                    if time.time() - t0 > lock_timeout:
                        raise TimeoutError(f"Lock timeout: {self.lock_path}")
                    time.sleep(1)  
                
        try:
            self._store = pd.HDFStore(h5_path, mode=mode)
        #    self._store = h5py.File(h5_path, mode) 
        except Exception as e:
            if self.lock and os.path.exists(self.lock_path):
                os.remove(self.lock_path)
            raise IOError(f"Could not open HDF5 file at {h5_path}: {e}")

    def put_df(self, key: str, df: pd.DataFrame):
        self._store.put(key, df, format='table', data_columns=True)
        
    def get_df(self, key: str) -> pd.DataFrame:
        if not key in self._store:
            raise KeyError(f"Key {key} not found in HDF5 store at {self.h5_path}")  
        
        df = self._store.get(key)
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Data at key {key} is not a DataFrame in HDF5 store at {self.h5_path}")
        
        return df

    def put_json(self, key: str, obj: dict):
        json_str = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        self._store.put(key, pd.Series([json_str]), format='table', data_columns=True)
        
    def get_json(self, key: str) -> dict:
        if not key in self._store:
            raise KeyError(f"Key {key} not found in HDF5 store at {self.h5_path}")  
        
        series = self._store.get(key)
        
        if not isinstance(series, pd.Series):
            raise TypeError(f"Data at key {key} is not a Series in HDF5 store at {self.h5_path}")
        
        json_str = series.iloc[0]
        obj = json.loads(json_str)
        
        return obj
    
    def put_string(self, key: str, value: str):
        self._store.put(key, pd.Series([value]), format='table', data_columns=True)
        
    def get_string(self, key: str) -> str:
        if not key in self._store:
            raise KeyError(f"Key {key} not found in HDF5 store at {self.h5_path}")  
        
        series = self._store.get(key)
        
        if not isinstance(series, pd.Series):
            raise TypeError(f"Data at key {key} is not a Series in HDF5 store at {self.h5_path}")
        
        value = series.iloc[0]
        
        return value
    
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
