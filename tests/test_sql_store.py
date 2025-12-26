# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

from metascribe.sql_store import SQLStore
from metascribe.parser import parse_with_template

def test():
    sql_path = "tests/files/test.db"
    template_file = "tests/files/template1.py"
    actual_file = "tests/files/actual1.py"

    data = parse_with_template(template_file, actual_file)    
    add_data = {
        "test_name": "test1",
        "jobid": "12345",
    }
    
    with SQLStore(sql_path) as store:
        store.store("metadata1", pk="jobid", **data, **add_data)
        
if __name__ == "__main__":
    test()    
    