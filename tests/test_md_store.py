# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

from metascribe.cli import md_store

def test():
    import sys
    sys.argv = [
        "md-store",
        "--table", "metadata2",
        "--sql_path", "tests/files/test.db",
        "--template", "tests/files/template1.py",
        "--file", "tests/files/actual1.py",
        "--kv_jobid=12345",
        "--kv_test_name=test1",
        "--pk=jobid"
    ]
    md_store()
    
if __name__ == "__main__":
    test()