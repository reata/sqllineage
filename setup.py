import shlex
import subprocess
from distutils.command.build_py import build_py

from setuptools import find_packages, setup

from sqllineage import NAME, VERSION

with open("README.md", "r") as f:
    long_description = f.read()


class BuildPYWithJS(build_py):
    def run(self) -> None:
        js_path = "sqllineagejs"
        subprocess.check_call(shlex.split(f"npm install --prefix {js_path}"))
        subprocess.check_call(shlex.split(f"npm run build --prefix {js_path}"))
        super().run()


setup(
    name=NAME,
    version=VERSION,
    author="Reata",
    author_email="reddevil.hjw@gmail.com",
    description="SQL Lineage Analysis Tool powered by Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/reata/sqllineage",
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    python_requires=">=3.6",
    install_requires=["sqlparse>=0.3.0", "networkx>=2.4", "flask", "flask_cors"],
    entry_points={"console_scripts": ["sqllineage = sqllineage.cli:main"]},
    extras_require={
        "ci": [
            "bandit",
            "black",
            "flake8",
            "flake8-blind-except",
            "flake8-builtins",
            "flake8-import-order",
            "flake8-logging-format",
            "mypy",
            "pytest",
            "pytest-cov",
            "tox",
            "twine",
            "wheel",
        ],
        "docs": ["Sphinx>=3.2.0", "sphinx_rtd_theme>=0.5.0"],
    },
    cmdclass={"build_py": BuildPYWithJS},
)
