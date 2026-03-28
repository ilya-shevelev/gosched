"""Setup script for the GoSched Python SDK."""

from setuptools import setup, find_packages

setup(
    name="gosched",
    version="0.1.0",
    description="Python SDK for the GoSched distributed resource scheduler",
    author="Ilya Shevelev",
    url="https://github.com/ilya-shevelev/gosched",
    packages=find_packages(),
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
