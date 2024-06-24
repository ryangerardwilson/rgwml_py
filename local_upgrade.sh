#!/bin/bash

# Step 1: Clean the dist directory and rebuild the package
rebuild_package() {
    python3 -m pip install --upgrade build
    rm -rf dist/*
    python3 -m build
}

# Step 2: Uninstall the local library
uninstall_local_library() {
    local package_name=$(grep -E '^name\s*=\s*".*"' pyproject.toml | sed -E 's/name\s*=\s*"(.*)"/\1/')
    pip3 uninstall -y "$package_name"
}

# Step 3: Install the local version to verify
install_local_version() {
    pip3 install .
}

# Execute the steps
rebuild_package
uninstall_local_library
install_local_version

echo "The local version of the package has been reinstalled."

