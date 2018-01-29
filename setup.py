from setuptools import setup, find_packages

requires = [
    'pytz',
    'monthdelta',
    'psycopg2',
]

setup_requires = [
    'flake8',
]

setup(
    name='housekeeper',
    version='0.1',
    description='A Zabbix housekeeper for Modio',
    long_description='',
    author='D.S. Ljungmark',
    author_email='spider@modio.se',
    url='https://www.modio.se',
    packages=find_packages(),
    zip_safe=True,
    include_package_data=True,
    install_requires=requires,
    setup_requires=setup_requires,
    entry_points={
        'console_scripts': [
            'housekeeper = housekeeper.housekeeper:main',
            'retention = housekeeper.retention:main',
            'partition = housekeeper.partition:main',
        ]
    },
)
