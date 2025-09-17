#!/bin/bash

# 简化版构建脚本 - 只构建协程演示程序

set -e

echo "=== 构建DiskANN协程演示程序 ==="

# 检查编译器支持
if command -v g++ >/dev/null 2>&1; then
    GCC_VERSION=$(g++ --version | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    echo "检测到GCC版本: $GCC_VERSION"
    if [[ $(echo "$GCC_VERSION >= 10.0" | bc -l 2>/dev/null || echo "0") -eq 1 ]]; then
        echo "GCC版本支持C++20协程"
    else
        echo "警告: GCC版本可能不支持C++20协程，需要GCC 10.0+"
    fi
elif command -v clang++ >/dev/null 2>&1; then
    CLANG_VERSION=$(clang++ --version | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    echo "检测到Clang版本: $CLANG_VERSION"
    echo "请确保Clang版本>=14.0以支持C++20协程"
else
    echo "错误: 未找到C++编译器"
    exit 1
fi

# 直接编译演示程序（不依赖完整的DiskANN库）
echo "编译协程演示程序..."

if command -v g++ >/dev/null 2>&1; then
    g++ -std=c++20 -fcoroutines -O2 -pthread \
        apps/demo_coroutine_diskann.cpp \
        -o demo_coroutine_diskann
elif command -v clang++ >/dev/null 2>&1; then
    clang++ -std=c++20 -stdlib=libc++ -O2 -pthread \
        apps/demo_coroutine_diskann.cpp \
        -o demo_coroutine_diskann
fi

if [ -f "demo_coroutine_diskann" ]; then
    echo "✓ 编译成功!"
    echo ""
    echo "运行演示程序:"
    ./demo_coroutine_diskann
else
    echo "✗ 编译失败"
    exit 1
fi

echo ""
echo "=== 演示完成 ==="
echo ""
echo "这个演示程序展示了协程化DiskANN的核心思想："
echo "1. 使用co_await挂起协程等待异步IO"
echo "2. 在IO等待期间CPU可以处理其他协程"  
echo "3. 多个查询可以并发执行，提高整体吞吐量"
echo ""
echo "要构建完整的协程版本DiskANN，请运行:"
echo "  ./build_async_diskann.sh"
