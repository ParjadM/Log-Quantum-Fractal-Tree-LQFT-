from setuptools import setup, find_packages, Extension
import os
import sys

# Systems Architect Logic: Cross-Platform Compiler Detection
extra_compile_args = []
if os.name == 'nt':
    if 'gcc' in sys.version.lower() or 'mingw' in sys.executable.lower():
        extra_compile_args = ['-O3']
    else:
        # MSVC specific optimization and security flags
        extra_compile_args = ['/O2', '/D_CRT_SECURE_NO_WARNINGS']
else:
    extra_compile_args = ['-O3']

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
    version="0.5.0", 
    description="LQFT Engine: Native Disk Persistence & Cold Start Deserialization (v5.0 Stable)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Parjad Minooei",
    url="https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-",
    ext_modules=[lqft_extension],
    packages=find_packages(),
    py_modules=["lqft_engine", "pure_python_ds"],
    install_requires=['psutil'],
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.8',
)