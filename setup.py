import os
import platform
import shlex
import shutil
import subprocess

from setuptools import setup
from setuptools.command.egg_info import egg_info


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
        py_path = "sqllineage"
        static_folder = "build"
        static_path = os.path.join(py_path, static_folder)
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
            shutil.move(os.path.join(js_path, static_folder), static_path)
        super().run()


setup(
    cmdclass={"egg_info": EggInfoWithJS},
)
