""" Setup file.
"""
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

install_requires=[
    'cornice',
    'waitress',
    'passlib',
    'pymongo',
    ]


setup(name='weatherdatarest',
    version=0.1,
    description='weatherdatarest',
    long_description=README,
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pylons",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
    ],
    keywords="web services",
    author='',
    author_email='',
    url='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points = """\
    [paste.app_factory]
    main = weatherdatarest:main
    """,
    paster_plugins=['pyramid'],
)
