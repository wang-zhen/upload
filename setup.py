# Copyright (c) 2013 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# THIS FILE IS MANAGED BY THE GLOBAL REQUIREMENTS REPO - DO NOT EDIT
import setuptools
from setuptools import find_packages
from setuptools import setup

from cloudsync import __version__
from cloudsync import __prog__
from cloudsync import __keywords__

try:
    import multiprocessing  # noqa
except ImportError:
    pass

setup(
    name=__prog__,
    version=__version__,
    keywords=__keywords__,
    description='gluster CloudArchival Scanner/uploader tools',
    long_description='This tool will scan the file system and based on a policy, will upload the data to a predecided Cloud Storage. The policy can be user defined. A simple example would be, upload any file that has not been accessed for one month.',
    license='LGPLv2+',

    url = "http://www.taocloudx.cn/",
    author='WangZhen',
    author_email='wangz@taocloudx.com',

    packages=find_packages(),
    include_package_data=True,

    install_requires=[],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Utilities',
    ],
    data_files=[],
    entry_points={
        'console_scripts': [
            'scanner_uploader = bin.scanner_uploader:main',
        ],
    },
)
