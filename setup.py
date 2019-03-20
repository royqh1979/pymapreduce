#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = []

test_requirements = [
    'pyautogui', 'pyautogui',  # TODO: put package test requirements here
]

setup(
    name='mapreduce-py',
    version='0.5.1',
    description='"A simple multi-processing based MapReduce framework"',
    long_description=readme + '\n\n' + history,
    author='Roy Qu',
    author_email='royqh1979@gmail.com',
    url='https://github.com/royqh1979/pymapreduce',
    packages=[
        'mapreduce'],
    package_dir={'mapreduce':
                     'mapreduce'},
    # package_data={},
    include_package_data=True,
    install_requires=[''],
    license="MIT",
    zip_safe=False,
    keywords=['MapReduce', "functional"],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    #    test_suite='tests',
    #    tests_require=test_requirements
)
