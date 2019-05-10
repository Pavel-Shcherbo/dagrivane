"""Include reviewed top-level examples without copying runtime output."""

from pathlib import Path
from shutil import copy2

from setuptools.command.build_py import build_py


class PackagedExamples(build_py):
    pass
