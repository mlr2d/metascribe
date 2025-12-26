# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

from metascribe.cli import md_parser

def test():
    import sys
    sys.argv = [
        "md-parser",
        "tests/files/template1.py",
        "tests/files/actual1.py",
    ]
    md_parser()
    
if __name__ == "__main__":
    test()