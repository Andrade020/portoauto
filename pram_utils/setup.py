# setup.py
from setuptools import setup, find_packages

setup(
    name="pram_utils",
    version="0.1.0",
    description="Utilitários Porto Real AM",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
    ],
    author="Porto Real Asset Management",
    author_email="lucas.andrade@portorealasset.com.br",

    # ===  license ===
    license="Proprietary",
    license_file="LICENSE", 
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
    ],
)
