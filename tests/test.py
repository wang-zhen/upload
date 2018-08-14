#!/usr/bin/python 
#*-*coding:utf8*-* 

import os
import sys 
import stat
import time
import xattr
import getopt
import socket
import shutil
import sqlite3
import logging
import commands

#minio
from minio import Minio
from minio.error import *
from minio.error import ResponseError
from minio.error import BucketAlreadyOwnedByYou 
from minio.error import BucketAlreadyExists
from minio.error import InvalidAccessKeyId


def _s3upload(filelist, volinfo, volume):
    """
    'buketid':'bin',
    'volname':'bin',
    'uppath':'/home/test',
    'message':'bin',
    'totalsize':100,
    'filenu':10,
    'success':100,
    'failed':30
    """

    upfileinfo = {}

    if (volinfo['s3plugin-https'] == 'on'):
        https = True
    else:
        https = False

    minioClient = Minio(volinfo['s3plugin-hostname'],
                        access_key=volinfo['s3plugin-keyid'],
                        secret_key=volinfo['s3plugin-seckey'],
                        secure=https)

    try:
        bucket = minioClient.bucket_exists(volinfo['s3plugin-bucketid'])
    #except InvalidAccessKeyId as err:
    #except SignatureDoesNotMatch as err:
    #except ResponseError as err:
    except Exception as err:
        return -1

    if not bucket:
        try:
            minioClient.make_bucket(volinfo['s3plugin-bucketid'], location="us-east-1")
        except BucketAlreadyOwnedByYou as err:
            pass
        except BucketAlreadyExists as err:
            pass
        except ResponseError as err:
            raise

    upfileinfo['buketid'] = volinfo['s3plugin-bucketid']
    upfileinfo['volname'] = volume
    upfileinfo['uptime'] = time.time()
    upfileinfo['uppath'] = volume
    upfileinfo['totalsize'] = 0
    upfileinfo['filenu'] = 0
    upfileinfo['success'] = 0
    upfileinfo['failed'] = 0

    for file in filelist:
        print file
        try:
            mp = "/upload/%s" % volume.strip()
            f = file.split(mp)[1]
            cms = format(time.time(), '.6f')
            f = f + "_" + cms

            minioClient.fput_object(volinfo['s3plugin-bucketid'], f.lstrip('/'), file)
            #minioClient.fput_object(volinfo['s3plugin-bucketid'], "etst/wtee/t.py", "/home/upload/cloudsync/bin/test.py")

            xattr.setxattr(file, 'trusted.glusterfs.csou.complete', f)

            upfileinfo['success'] = upfileinfo['success'] + 1
        except ResponseError as err:
            print err 
            upfileinfo['failed'] = upfileinfo['failed'] + 1
            continue

    return 0, upfileinfo

if __name__ == "__main__":
   filelist = ['/upload/test1/glusterfs/xlators/features/selinux/src/selinux.h']

   volinfo = {'s3plugin-https': 'off', 's3plugin-bucketid': 'test1', 'cs-storetype': 'cloudsyncs3', 's3plugin-keyid': 'SIWSWX33WD7K8HADNYVU', 'cs-worm': 'on', 's3plugin-hostname': '192.168.2.95:9000', 'cloudsync': 'on', 's3plugin-seckey': 'u1fIgYyWlyNjJC3XjM8gH+i908EQk/OBkb9Cxjmk', 'cs-watermark-thread-frequency': '3', 'cs-max-files': '1000', 'cs-max-mb': '4000'}

   volume = 'test1'

   _s3upload(filelist, volinfo, volume)
 
