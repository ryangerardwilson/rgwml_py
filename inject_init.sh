#!/bin/bash

# Directory of the package (relative to the script)
PACKAGE_DIR="src/rgwml"

# Paths to the __init__.py and docs.py files
INIT_FILE="$PACKAGE_DIR/__init__.py"
DOCS_FILE="$PACKAGE_DIR/docs.py"

# Clear the existing __init__.py file or create a new one
> "$INIT_FILE"

# Clear the existing docs.py file or create a new one
> "$DOCS_FILE"

# Function to extract function names and signatures from a Python file
extract_functions_and_signatures() {
    local file=$1
    python3 - << EOF
import ast

with open('$file', 'r') as f:
    tree = ast.parse(f.read(), filename='$file')

functions = []
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef):
        func_name = node.name
        args = [arg.arg for arg in node.args.args]
        signature = f"({', '.join(args)})"
        functions.append(f"{func_name}{signature}")

for func in functions:
    print(func)
EOF
}

# Initialize an array to store function names and signatures for docs.py
declare -a function_list

# Iterate through all Python files in the package directory
for py_file in "$PACKAGE_DIR"/*.py; do
    if [[ -f "$py_file" && "$py_file" != "$INIT_FILE" && "$py_file" != "$DOCS_FILE" ]]; then
        # Get the base name of the Python file without extension
        module_name=$(basename "$py_file" .py)

        # Extract function names and signatures and write import statements to __init__.py
        while read -r function_signature; do
            function_name=$(echo $function_signature | cut -d '(' -f 1)
            echo "from .${module_name} import ${function_name}" >> "$INIT_FILE"
            function_list+=("${function_signature}")
        done < <(extract_functions_and_signatures "$py_file")
    fi
done

# Write the show_docs function to docs.py
echo "def show_docs():" >> "$DOCS_FILE"
echo "    \"\"\"Prints out all methods in rgwml package with their signatures.\"\"\"" >> "$DOCS_FILE"
echo "    methods = [" >> "$DOCS_FILE"
for func in "${function_list[@]}"; do
    echo "        \"$func\"," >> "$DOCS_FILE"
done
echo "    ]" >> "$DOCS_FILE"
echo "    for method in methods:" >> "$DOCS_FILE"
echo "        print(method)" >> "$DOCS_FILE"

echo "Setup of __init__.py and docs.py is complete."

