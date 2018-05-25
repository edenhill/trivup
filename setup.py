from distutils.core import setup
from setuptools import find_packages
from glob import glob
import os

data=dict()
app='trivup'
data[app] = list()
# Find Apps data
for d in glob('trivup/apps/*App'):
    data[app] += [x[x.find('apps/'):] for x in glob('%s/*' % d) if x[-1:] != '~']

setup(name='trivup',
      version='0.3',
      description='Trivially Up a cluster of programs, such as a Kafka cluster',
      author='Magnus Edenhill',
      author_email='magnus@edenhill.se',
      url='https://github.com/edenhill/trivup',
      packages=find_packages(),
      package_data=data
     )
