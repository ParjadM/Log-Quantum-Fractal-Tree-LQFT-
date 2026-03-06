from setuptools import setup, find_packages, Extension
import os
import sys

# ---------------------------------------------------------
# LQFT BUILD SYSTEM - V1.1.3
# Architect: Parjad Minooei
# ---------------------------------------------------------

# Aggressive hardware optimizations based on platform
extra_compile_args = []

if sys.platform == 'darwin':
    # FIX: macOS cross-compilation on M1/M2/M3 runners crashes with -march=native
    # because it tries to compile x86_64 wheels using Apple Silicon native instructions.
    extra_compile_args = ['-O3']
elif os.name == 'nt':
    if 'gcc' in sys.version.lower() or 'mingw' in sys.executable.lower():
        extra_compile_args = ['-O3']
    else:
        extra_compile_args = ['/O2', '/GL', '/D_CRT_SECURE_NO_WARNINGS']
else:
    # POSIX / Linux
    if os.environ.get('CIBUILDWHEEL') == '1':
        extra_compile_args = ['-O3'] 
    else:
        extra_compile_args = ['-O3', '-march=native']

long_description = "Log-Quantum Fractal Tree Engine"
if os.path.exists("README.md"):
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()

# Define the Native C-Extension
lqft_extension = Extension(
    'lqft_c_engine',
    sources=['lqft_engine.c'],
    extra_compile_args=extra_compile_args,
)

setup(
    name="lqft-python-engine",
    version="1.1.3",
    description="LQFT Engine: native C extension with structural sharing and improved unique-value write batching",
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
        "Programming Language :: C",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Database Engines/Servers",
    ],
    python_requires='>=3.10',
)