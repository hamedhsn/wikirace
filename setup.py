from setuptools import setup, find_packages

__author__ = 'Hamed'

requirements = [
    'pymongo>=3.2.1',
    'pykafka'
]

dependency_links = [
    'git+https://github.com/jaybaird/python-bloomfilter.git',
]

setup(
    name='wikirace',
    version='1.0.0',
    description='wiki race',
    author='Hamed',
    maintainer='Hamed',
    maintainer_email='hamed@gmail.com',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True
)
