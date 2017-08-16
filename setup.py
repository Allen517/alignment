from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()


setup(
    name='Portrait',
    version='1.0.0.dev',
    url='https://github.com/Allen517/alignment',
    description='User portrait on Internet',
    long_description=long_description,
    keywords='internet portrait alignment',
    author='Portrait developers',
    maintainer='King Wang',
    maintainer_email='wangyongqing.casia@gmail.com',
    license='BSD',
    packages=find_packages(exclude=('tests', 'tests.*')),
    package_data={
        'portrait': ['config.ini'],
    },
    entry_points={
        'console_scripts': [
            'data_import2neo = portrait.import.DataImport2Neo:main',
        ],
    },
    classifiers=[
        'Framework :: Portrait',
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Framework :: Flask'
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: Chinese (Simplified)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'py2neo>=3.1.2',
        'pymongo',
        'uuid'
    ],
)