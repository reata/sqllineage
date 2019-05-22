import setuptools

from sqllineage import name, version

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name=name,
    version=version,
    author="Reata",
    author_email="reddevil.hjw@gmail.com",
    description="SQL Lineage Analysis Tool powered by Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/reata/sqllineage",
    packages=setuptools.find_packages(exclude=("tests",)),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.5",
)
