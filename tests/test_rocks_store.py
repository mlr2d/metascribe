from metascribe.rocks_store import RocksStore
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

df1 = pd.DataFrame([
    {"name": "Alice",   "age": 30, "city": "New York",    "score": 85.0243834},
    {"name": "Bob",     "age": 25, "city": "Los Angeles", "score": 90.08234},
    {"name": "Charlie", "age": 35, "city": "Chicago",     "score": 78.762342},
    {"name": "Diana",   "age": 28, "city": "Miami",       "score": 92.927324},
    {"name": "Ethan",   "age": 32, "city": "Seattle",     "score": 88.27364},
])

json1 = {
    "lr": 0.001,
    "batch_size": 64,
    "model": {
        "layers": [12,23,34],
        "type": "transformer"
    }
}
    
def test_write_df():
    db_path = "tests/files/test_store.rocks"
    with RocksStore(db_path, lock=True) as store:
        logging.info(f"Opened Rocks store at {db_path}")
        store.put_df("/activity_log/x/y", df1)
        

def test_read_df():
    db_path = "tests/files/test_store.rocks"
    with RocksStore(db_path) as store:
        logging.info(f"Opened Rocks store at {db_path}")
        df_read = store.get_df("/activity_log/x/y")
        print(df_read)

def test_write_json():
    db_path = "tests/files/test_store.rocks"
    with RocksStore(db_path, lock=True) as store:
        logging.info(f"Opened Rocks store at {db_path}")
        store.put_json("/configs/run1", json1)
        
def test_read_json():
    db_path = "tests/files/test_store.rocks"
    with RocksStore(db_path) as store:
        logging.info(f"Opened Rocks store at {db_path}")
        json_read = store.get_json("/configs/run1")
        print(json_read)
        

if __name__ == "__main__":
    test_write_df()
    test_read_df()
    test_write_json()
    test_read_json()
    