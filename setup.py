import os
from setuptools import setup, find_packages

# Find all packages in the project
found_packages = find_packages()

print("Found packages:")
for pkg in found_packages:
    # Convert the package name to a path relative to the project root
    pkg_path = os.path.join(os.path.dirname(__file__), pkg.replace(".", os.sep))
    print(f"  {pkg} located in {pkg_path}")

setup(
    name="quant_pipeline",
    version="0.1",
    packages=found_packages,
    install_requires=[
        "pandas",
        "yfinance",
        "matplotlib",
        "seaborn",
        "fastapi",
        "uvicorn",
    ],
)