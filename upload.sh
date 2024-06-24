#!/bin/bash

# Step 1: Increment the version number in pyproject.toml and setup.cfg
increment_version() {
    local version_file="pyproject.toml"
    local setup_file="setup.cfg"

    # Extract the current version from pyproject.toml
    local version_line=$(grep -E 'version = "[0-9]+\.[0-9]+\.[0-9]+"' "$version_file")
    local current_version=$(echo "$version_line" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')

    # Increment the patch version
    IFS='.' read -r -a version_parts <<< "$current_version"
    version_parts[2]=$((version_parts[2] + 1))
    local new_version="${version_parts[0]}.${version_parts[1]}.${version_parts[2]}"

    # Update the version in pyproject.toml
    sed -i "s/version = \"$current_version\"/version = \"$new_version\"/" "$version_file"

    # Update the version in setup.cfg
    sed -i "s/version = $current_version/version = $new_version/" "$setup_file"

    echo "$new_version"
}

# Step 2: Clean the dist directory and rebuild the package
rebuild_package() {
    python3 -m pip install --upgrade build twine
    rm -rf dist/*
    python3 -m build
}

# Step 3: Upload the new version
upload_package() {
    python3 -m twine upload dist/*
}

# Step 4: Verify by installing the specific version
verify_package() {
    local new_version=$1
    echo "Execute this command after a minute to verify the new version $new_version: pip3 install --upgrade rgwml"
}

# Execute the steps
new_version=$(increment_version)
rebuild_package
upload_package
verify_package "$new_version"

