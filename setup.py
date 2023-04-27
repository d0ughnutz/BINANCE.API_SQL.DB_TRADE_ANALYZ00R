from setuptools import setup, find_packages

setup(
    name='api2sqldb',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    url='https://github.com/d0ughnutz/api2sqlbd.py',
    license='MIT',
    author='d0ughnutz',
    author_email='126280474+d0ughnutz@users.noreply.github.com',
    description='Get crypto API data and store in a PosgreSQL DB'
)
