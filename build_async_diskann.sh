#!/bin/bash

# Build and test script for Coroutine DiskANN

set -e

echo "=== Building Coroutine DiskANN ==="

# Check if liburing is installed
if ! pkg-config --exists liburing; then
    echo "liburing is not installed. Installing..."
    echo "Please run: sudo apt install liburing-dev"
    exit 1
fi

# Create build directory
mkdir -p build_async
cd build_async

# Configure with coroutines enabled
echo "Configuring CMake with coroutine support..."
cmake .. -DENABLE_COROUTINES=ON -DCMAKE_BUILD_TYPE=Release

# Build the project
echo "Building..."
make -j$(nproc)

echo "=== Build completed successfully ==="

# Run basic test
if [ -f "./apps/test_coroutine_diskann" ]; then
    echo "=== Running coroutine tests ==="
    ./apps/test_coroutine_diskann
    echo "=== Tests completed ==="
else
    echo "Test executable not found. Build may have failed."
    exit 1
fi

echo "=== Coroutine DiskANN setup complete ==="
echo "You can now use ./apps/async_search_disk_index for async searches"

cd ..
