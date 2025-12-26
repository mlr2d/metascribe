# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

import sys
from pathlib import Path
import argparse
from metascribe.parser import parse_with_template
from metascribe.sql_store import SQLStore

def md_parser():
    if len(sys.argv) != 3:
        print("Usage: ms-parser <template_file> <actual_file>")
        sys.exit(1)

    template_file = sys.argv[1]
    actual_file = sys.argv[2]

    if not Path(template_file).is_file():
        print(f"Template file '{template_file}' does not exist.")
        sys.exit(1)

    if not Path(actual_file).is_file():
        print(f"Actual file '{actual_file}' does not exist.")
        sys.exit(1)

    try:
        result = parse_with_template(template_file, actual_file)
        for k, v in result.items():
            print(f"{k}: {v}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
        
def md_store():
    #e.g.,  md-store --md_table=xxx --kv_jobid=111 --kv_iked=123 --md_template ./tests/files/template1.py --md_file ./tests/files/actual1.py --sql_path=test.db --pk=jobid
    
    parser = argparse.ArgumentParser(description="Parse cmd key value pairs and store them on SQLite db.")
    
    # 1. Define Fixed Arguments
    parser.add_argument("--template", help="Path to template file")
    parser.add_argument("--file", help="Path to the script file")
    parser.add_argument("--table", default="job_metadata", help="Target SQL table", required=True)
    parser.add_argument("--sql_path", help="Path to SQLite database", required=True)
    parser.add_argument("--pk", help="Primary key column name", default=None)
    
    # 2. Capture all other arguments (the ones we don't know yet)
    args, unknown = parser.parse_known_args()
    
    md_template = args.template
    md_file = args.file
    md_table = args.table
    sql_path = args.sql_path
    pk = args.pk
    
    md_parsed = {}
    if md_template and md_file:
        try:
            md_parsed = parse_with_template(md_template, md_file)
        except Exception as e:
            print(f"Error parsing files: {e}")
            sys.exit(1)
    
    # 3. Parse dynamic kv_ arguments into a dictionary
    kv_dict = {}
    for kv in unknown:
        if kv.startswith("--kv_"):
            if "=" in kv:
                tmp = kv.split("=")
                key = tmp[0].replace("--kv_", "").strip()
                val = tmp[1].strip()
                try:
                    val = int(val)
                except ValueError:
                    pass
                kv_dict[key] = val        
    with SQLStore(sql_path) as store:
        store.store(md_table, pk=pk, **md_parsed, **kv_dict)
        
          

                

    