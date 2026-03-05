from setuptools import setup, find_packages, Extension
import os
import sys

# ---------------------------------------------------------
# LQFT BUILD SYSTEM - V0.9.4 (Billion-Scale Stability Release)
# ---------------------------------------------------------
# Architect: Parjad Minooei
# Status: Production Hardened
# Performance: 13.3M Search ops/s | 1B-Snapshot Memory Folding

# Systems Architect Logic: Cross-Platform Compiler Routing
extra_compile_args = []

if os.name == 'nt':
    # Windows (MSVC or MinGW)
    if 'gcc' in sys.version.lower() or 'mingw' in sys.executable.lower():
        # Aggressive GCC optimization for the Slab Allocator
        extra_compile_args = ['-O3']
    else:
        # Microsoft Visual C++ optimizations
        extra_compile_args = ['/O2', '/D_CRT_SECURE_NO_WARNINGS']
else:
    # macOS/Linux: POSIX optimizations
    extra_compile_args = ['-O3']

# Load README for PyPI long_description
long_description = "Log-Quantum Fractal Tree Engine"
if os.path.exists("README.md"):
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()

# Define the Native C-Extension
# Source: lqft_engine.c contains the v0.9.3+ memory leak fixes and vectorized hashing
lqft_extension = Extension(
    'lqft_c_engine',
    sources=['lqft_engine.c'],
    extra_compile_args=extra_compile_args,
)

setup(
    name="lqft-python-engine",
    version="0.9.4", 
    description="LQFT Engine: Billion-Scale Persistence & Vectorized 13M-Search (v0.9.4 Stable)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Parjad Minooei",
    url="https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-",
    ext_modules=[lqft_extension],
    packages=find_packages(),
    py_modules=["lqft_engine"], 
    install_requires=['psutil'],
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: C",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Database Engines/Servers",
    ],
    python_requires='>=3.10',
)