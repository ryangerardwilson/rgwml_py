#!/bin/bash

# Get the current working directory
base_dir=$(pwd)

# Define the base directories
src_dir="${base_dir}/next-js-ui/src"
output_dir="${base_dir}/src/rgwml/resources"
output_file="${output_dir}/frontend_assets.py"

# Ensure the output directory exists
mkdir -p "$output_dir"

# Start writing to the Python file
cat <<EOL > "$output_file"
# Generated file: frontend_assets.py

EOL

# Function to convert camel case to uppercase with underscores
camel_to_upper() {
    echo "$1" | sed -r 's/([a-z])([A-Z])/\1_\2/g' | tr '[:lower:]' '[:upper:]'
}

# Function to process files
process_files() {
    local dir=$1
    local prefix=$2
    local extension=$3
    local ext_upper=$(echo "$extension" | tr '[:lower:]' '[:upper:]')
    local files=$(find "$dir" -type f -name "*.$extension")

    for file in $files; do
        # Extract relative path and create the variable name
        relative_path="${file#$src_dir/}"
        base_name=$(basename "$file" .$extension)
        var_name="${prefix}__FILE__${base_name//[^a-zA-Z0-9]/__}__${ext_upper}"
        var_name=$(camel_to_upper "$var_name")

        # Escape {, }, and \n in the file content
        file_content=$(sed 's/{/{{/g; s/}/}}/g; s/\\n/\\\\n/g' "$file")

        # Write to the Python file
        echo "${var_name} = '''$file_content'''" >> "$output_file"
        echo "" >> "$output_file"
    done
}

# Process each directory for .tsx and .css files
process_dir() {
    local dir=$1
    local prefix=$2
    process_files "$dir" "$prefix" "tsx"
    process_files "$dir" "$prefix" "css"
}

# Process the specific directories
process_dir "$src_dir/app" "DIR__APP"
process_dir "$src_dir/components" "DIR__COMPONENTS"
process_dir "$src_dir/pages" "DIR__PAGES"

# Process the middleware.tsx file in the root of the src directory
middleware_file="$src_dir/middleware.tsx"
if [ -f "$middleware_file" ];then
    base_name=$(basename "$middleware_file" .tsx)
    var_name="ROOT__FILE__${base_name}__TSX"
    var_name=$(camel_to_upper "$var_name")

    # Escape {, }, and \n in the file content
    file_content=$(sed 's/{/{{/g; s/}/}}/g; s/\\n/\\\\n/g' "$middleware_file")

    # Write to the Python file
    echo "${var_name} = '''$file_content'''" >> "$output_file"
    echo "" >> "$output_file"
fi

echo "frontend_assets.py has been generated at $output_file"

