import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

requires = [
    'psycopg2-binary',
]

setup_requires = [
    'flake8',
]

tests_require = [
    'pytest',
]


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        super().initialize_options()
        self.pytest_args = []

    def finalize_options(self):
        super().finalize_options()
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, so the egg files have been loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


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
    extras_require={
        'testing': tests_require,
    },
    cmdclass={'test': PyTest},
    entry_points={
        'console_scripts': [
            'housekeeper = housekeeper.housekeeper:main',
            'retention = housekeeper.retention:main',
            'partition = housekeeper.partition:main',
        ]
    },
)
