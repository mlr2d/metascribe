# Copyright (c) 2025, Aravind Sankaran, MLR2D
# 
# This software is licensed under the BSD 3-Clause "New" or "Revised" License.
# A copy of the license should have been distributed with this software in 
# the LICENSE file. If not, see <https://opensource.org/licenses/BSD-3-Clause>.

from jinja2 import Template
import re
from pathlib import Path
import sys


def jinja_to_regex(template_text):
    """
    Convert a Jinja2 template into a regex that can extract values,
    even if placeholders repeat multiple times.
    """
    # Step 1: find all variable occurrences (ordered)
    vars_found = re.findall(r"{{\s*(\w+)\s*}}", template_text)

    # Step 2: replace each occurrence with a unique token
    tmp_text = template_text
    for i, var in enumerate(vars_found, 1):
        tmp_text = tmp_text.replace(f"{{{{ {var} }}}}", f"___VAR_{var}__{i}___", 1)

    # Step 3: escape everything else
    escaped = re.escape(tmp_text)

    # Step 4: replace placeholders with unique named groups
    for i, var in enumerate(vars_found, 1):
        escaped = escaped.replace(
            f"___VAR_{var}__{i}___",
            f"(?P<{var}__{i}>[\\s\\S]+?)"
        )

    # Step 5: make spaces/newlines flexible
    escaped = escaped.replace(r"\ ", r"\s+").replace(r"\n", r"\s*")

    return re.compile(escaped, re.MULTILINE)

def parse_with_template(template_path, result_path):
    """Extract variables from a rendered SLURM script."""
    template_text = Path(template_path).read_text()
    result_text = Path(result_path).read_text()
    # result_text = "\n".join(line for line in Path(result_path).read_text().splitlines() if not line.startswith("WARNING:"))
    # print(result_text)

    regex = jinja_to_regex(template_text)
    # print(f"Using regex pattern:\n{regex.pattern}\n{'-'*40}")
    match = regex.search(result_text)
    if not match:
        raise ValueError("Could not match template with rendered file.")

    # strip whitespace and handle repeated vars consistently
    data_ =  {k.strip(): v.strip() for k, v in match.groupdict().items()}
    data = {}
    for k, v in data_.items():
        key = k.split("__")[0]  # remove occurrence suffix
        data[key] = v 
    return data


if __name__ == "__main__":
    #get template file and actual file from command line arguments 1 and 2
    
    if len(sys.argv) != 3:
        print("Usage: python parse.py <template_file> <actual_file>")
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
