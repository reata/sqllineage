"""
Hatch build hook for building frontend assets.
"""

import os
import platform
import shlex
import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class FrontendBuildHook(BuildHookInterface):
    """
    A Hatch build hook to build frontend assets using npm.
    This hook runs during the build process to ensure that the frontend assets are built and included in the package.
    """

    PLUGIN_NAME = "frontend"

    def initialize(self, version, build_data):
        """
        This hook builds frontend assets in the following scenarios:
        1. Building wheel distribution (python -m build --wheel)
        2. Installing from GitHub (pip install git+https://github.com/reata/sqllineage.git). pip will build sdist first,
           then install from sdist. sdist itself does not include built static files, same as source code repo.
           Installation from sdist will trigger this hook. (by building a wheel from sdist)
        """
        if "READTHEDOCS" in os.environ:
            return
        py_path = Path("sqllineage")
        static_folder = "build"
        static_path = py_path / static_folder
        js_path = Path("sqllineagejs")
        use_shell = True if platform.system() == "Windows" else False
        try:
            # install npm dependencies
            subprocess.check_call(
                shlex.split("npm install"), cwd=str(js_path), shell=use_shell
            )
            # build the frontend assets
            subprocess.check_call(
                shlex.split("npm run build"), cwd=str(js_path), shell=use_shell
            )
            # move the built assets to the Python package directory
            source_build_path = js_path / static_folder
            if static_path.exists():
                shutil.rmtree(str(static_path))
            shutil.move(str(source_build_path), str(static_path))
            # add all files in the static_path to build_data artifacts
            for file_path in static_path.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(Path("."))
                    build_data.setdefault("artifacts", []).append(str(relative_path))
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Frontend build failed: {e}") from e
        except Exception as e:
            raise RuntimeError(
                f"An unexpected error occurred during frontend build: {e}"
            ) from e
