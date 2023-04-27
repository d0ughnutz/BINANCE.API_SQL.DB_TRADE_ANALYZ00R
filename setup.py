from setuptools import setup, find_packages

setup(
    name='api2sqldb',
    version='0.1.0',
    # install_requires=['Your-Library'],
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    url='https://github.com/d0ughnutz/api2sqlbd.py',
    license='MIT',
    author='d0ughnutz',
    author_email='dough.go00@email.com',
    description='api2sqldb'
)
