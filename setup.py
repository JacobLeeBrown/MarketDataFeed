
from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

setup(
    name='MarketDataFeed',
    version='1.0.0',
    description='Simple Market Feed Web App',
    long_description=readme,
    author='Jacob Brown',
    packages=find_packages(exclude='tests')
)
