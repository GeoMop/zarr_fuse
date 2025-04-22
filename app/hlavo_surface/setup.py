from setuptools import setup

setup(
    name="hlavo_surface",           # The name of your package
    version="0.1.0",             # Your package version
    description="HALVO project - collect, harmonize, vizualize surface measurement data.",
    author="Jan Brezina",
    author_email="jan.brezina@tul.cz",
    install_requires = ["PyYAML", "attrs", "numpy"],
    packages=["hlavo_surface"],
    package_dir={"hlavo_surface": "."},  # Map the package name to the current directory
)
