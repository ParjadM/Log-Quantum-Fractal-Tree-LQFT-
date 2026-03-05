from setuptools import setup, find_packages, Extension
import os
import sys

# Systems Architect Logic: OS-Specific Compiler Routing
extra_compile_args = []

if os.name == 'nt':
    # Windows (MSVC or MinGW)
    if 'gcc' in sys.version.lower() or 'mingw' in sys.executable.lower():
        extra_compile_args = ['-O3']
    else:
        extra_compile_args = ['/O2', '/D_CRT_SECURE_NO_WARNINGS']

elif sys.platform == 'darwin':
    # macOS (Apple Clang 15+): 
    # Requires explicit POSIX definitions and warning-to-error downgrades.
    extra_compile_args = [
        '-O3', 
        '-D_DARWIN_C_SOURCE',
        '-D_POSIX_C_SOURCE=200809L',
        '-Wno-error=implicit-function-declaration',
        '-Wno-error=incompatible-function-pointer-types',
        '-Wno-error=int-conversion'
    ]

else:
    # Linux (GCC): 
    # Standard POSIX enforcement without Clang-specific warning flags.
    extra_compile_args = [
        '-O3', 
        '-D_POSIX_C_SOURCE=200809L'
    ]

# Load README for PyPI long_description
long_description = "Log-Quantum Fractal Tree Engine"
if os.path.exists("README.md"):
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()

lqft_extension = Extension(
    'lqft_c_engine',
    sources=['lqft_engine.c'],
    extra_compile_args=extra_compile_args,
)

setup(
    name="lqft-python-engine",
    version="0.8.5", 
    description="LQFT Engine: Zero-Copy Buffer Protocol & Hardware Saturation (v0.8.5 Stable)",
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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.10',
)