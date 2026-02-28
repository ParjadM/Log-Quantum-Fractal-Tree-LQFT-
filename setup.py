from setuptools import setup, find_packages, Extension
import os
import sys

# Systems Architect Logic: Cross-Platform Compiler Detection
# We handle the MSVC vs GCC flag difference dynamically.
# Since you are using MSYS2/MinGW, we target GCC flags (-O3) to avoid the /O2 error.
extra_compile_args = []

if os.name == 'nt':
    # Check if the environment is MinGW/GCC (MSYS2) vs standard Windows (MSVC)
    # This prevents the 'linker input file not found: /O2' error seen in previous builds.
    if 'gcc' in sys.version.lower() or 'mingw' in sys.executable.lower():
        extra_compile_args = ['-O3']
    else:
        extra_compile_args = ['/O2']
else:
    # Default to high-performance GCC/Clang optimization for Unix-like systems
    extra_compile_args = ['-O3']

lqft_extension = Extension(
    'lqft_c_engine', # This MUST match the PyInit function name in the Canvas file
    sources=['lqft_engine.c'],
    extra_compile_args=extra_compile_args,
    # Define this macro to handle Windows security warnings professionally
    define_macros=[('_CRT_SECURE_NO_WARNINGS', '1')]
)

setup(
    name="lqft-python",
    version="0.1.0",
    description="Log-Quantum Fractal Tree: Pattern-Aware Deduplicating Data Structure with C-Engine",
    author="Parjad Minooei",
    author_email="parjad@example.com",
    url="https://github.com/ParjadM/lqft-python",
    ext_modules=[lqft_extension],
    packages=find_packages(),
    py_modules=["lqft_engine"],
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)