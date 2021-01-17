import os
import platform
import shlex
import shutil
import subprocess

from setuptools import find_packages, setup
from setuptools.command.egg_info import egg_info

from sqllineage import NAME, STATIC_FOLDRE, VERSION

with open("README.md", "r") as f:
    long_description = f.read()


class EggInfoWithJS(egg_info):
    """
    egginfo is a hook both for
        1) building source code distribution (python setup.py sdist)
        2) building wheel distribution (python setup.py bdist_wheel)
        3) installing from source code (python setup.py install) or pip install from GitHub
    In this step, frontend code will be built to match MANIFEST.in list so that later the static files will be copied to
    site-packages correctly as package_data. When building a distribution, no building process is needed at install time
    """

    def run(self) -> None:
        static_path = os.path.join(NAME, STATIC_FOLDRE)
        if os.path.exists(static_path) or "READTHEDOCS" in os.environ:
            pass
        else:
            js_path = "sqllineagejs"
            use_shell = True if platform.system() == "Windows" else False
            subprocess.check_call(
                shlex.split("npm install"), cwd=js_path, shell=use_shell
            )
            subprocess.check_call(
                shlex.split("npm run build"), cwd=js_path, shell=use_shell
            )
            shutil.move(os.path.join(js_path, STATIC_FOLDRE), static_path)
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
    package_data={"": [f"{STATIC_FOLDRE}/*", f"{STATIC_FOLDRE}/**/**/*"]},
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
    cmdclass={"egg_info": EggInfoWithJS},
)
