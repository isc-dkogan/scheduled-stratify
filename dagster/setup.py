from setuptools import find_packages, setup

setup(
    name="dagster_stratify",
    packages=find_packages(exclude=["dagster_stratify_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
