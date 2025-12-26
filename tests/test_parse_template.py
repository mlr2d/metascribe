# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

import sys
from metascribe.parser import parse_with_template
from pathlib import Path

def test():
    if len(sys.argv) != 3:
        print("Usage: python ___.py <template_file> <actual_file>")
        sys.exit(1)
    template_file = sys.argv[1]
    actual_file = sys.argv[2]
    
    if not Path(template_file).is_file():
        print(f"Template file '{template_file}' does not exist.")
        sys.exit(1)
        
    if not Path(actual_file).is_file():
        print(f"Actual file '{actual_file}' does not exist.")
        sys.exit(1)
        
    result = parse_with_template(template_file, actual_file)       

    for k, v in result.items():
        print(f"{k}: {v}")
        
if __name__ == "__main__":
    test()