from distutils.core import setup
from setuptools import find_packages
from glob import glob

data = dict()
app = 'trivup'
data[app] = list()
# Find Apps data
for d in glob('trivup/apps/*App'):
    data[app] += [x[x.find('apps/'):] for x in glob('%s/*' % d)
                  if x[-1:] != '~']

setup(name='trivup',
      version='0.10.0',
      description='Trivially Up a cluster of programs, such as a Kafka cluster',  # noqa: E501
      author='Magnus Edenhill',
      author_email='magnus@edenhill.se',
      url='https://github.com/edenhill/trivup',
      license_files=['LICENSE'],
      packages=find_packages(),
      package_data=data,
      install_requires=[
          'requests',
          'jwcrypto',
          'python_jwt'
      ],
      classifiers=[
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 2",
          "License :: OSI Approved :: BSD License",
          "Operating System :: OS Independent",
      ])
