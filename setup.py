from setuptools import setup, find_packages

requires = ["psycopg2-binary >= 2.7.4"]

setup(
    name="housekeeper",
    description="A Zabbix housekeeper for Modio",
    long_description="",
    author="D.S. Ljungmark",
    author_email="spider@modio.se",
    url="https://www.modio.se",
    packages=find_packages(),
    zip_safe=True,
    include_package_data=True,
    install_requires=requires,
    use_scm_version={"write_to": "version.txt"},
    entry_points={
        "console_scripts": [
            "housekeeper = housekeeper.housekeeper:main",
            "retention = housekeeper.retention:main",
            "partition = housekeeper.partition:main",
            "archiver = housekeeper.archiver:main",
        ]
    },
)
