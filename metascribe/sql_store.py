# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

import sqlite3

class SQLStore:
    def __init__(self, sql_path):
        self.sql_path = sql_path
        try:
            self.conn = sqlite3.connect(self.sql_path)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            self.conn = None
            self.cursor = None
            
    def store(self, table_name, pk=None, **kwargs):
        if not self.cursor:
            print("No database connection.")
            return
        
        if not kwargs:
            print("No data provided to store.")
            return

        # 1. Dynamically create the table with an optional Primary Key
        col_list = []
        for k, v in kwargs.items():
            col_type = self._get_sqlite_type(v)
            # Append 'PRIMARY KEY' string if this key matches the pk argument
            if k == pk:
                col_list.append(f"{k} {col_type} PRIMARY KEY")
            else:
                col_list.append(f"{k} {col_type}")

        cols_definition = ', '.join(col_list)
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_definition})"
        
        # 2. Prepare the INSERT OR REPLACE statement
        columns = ', '.join(kwargs.keys())
        placeholders = ', '.join('?' * len(kwargs))
        values = tuple(kwargs.values())
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            self.cursor.execute(create_sql)
            self.cursor.execute(insert_sql, values)
            self.conn.commit()
            print(f"Successfully stored data in '{table_name}'")
        except sqlite3.Error as e:
            print(f"Error processing database: {e}")
            
    
    def _get_sqlite_type(self, value):
        """Helper to map Python types to SQLite types."""
        if isinstance(value, int): return "INTEGER"
        if isinstance(value, float): return "REAL"
        return "TEXT"
    
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.conn:
            self.conn.close()
