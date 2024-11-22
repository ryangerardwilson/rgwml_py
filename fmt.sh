#!/bin/bash

# Set the directory to search for Python files
SRC_DIR="src"

# Find all .py files in the src directory, excluding __init__.py and the specific exclude file
find $SRC_DIR -name "*.py" ! -name "__init__.py" | while read -r file; do

    # Execute autopep8
    autopep8 --ignore E501 --in-place --aggressive --aggressive "$file"
    
    # Execute flake8 and filter out 'F403' and 'F401' errors
    flake8 --ignore=E501,E403,E401 "$file" | grep -Ev '(F403|F401)'
done

