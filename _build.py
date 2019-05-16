"""Include reviewed top-level examples without copying runtime output."""

from pathlib import Path
from shutil import copy2

from setuptools.command.build_py import build_py


class PackagedExamples(build_py):
    def run(self):
        super().run()
        root = Path(__file__).resolve().parent
        names = self.distribution.packages
        for package in names:
            target = Path(self.build_lib).joinpath(*package.split("."), "_examples")
            target.mkdir(parents=True, exist_ok=True)
            for source in (root / "examples").iterdir():
                if source.is_file() and source.suffix in {".csv", ".json", ".py"}:
                    copy2(source, target / source.name)
