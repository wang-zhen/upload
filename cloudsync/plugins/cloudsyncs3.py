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


sq3dbpath = '/var/lib/glusterd/xdfs-cloudsyncs3-upload.db'

logger = logging.getLogger(__name__)

def _getxattr(file, key):
    try:
        val = xattr.getxattr(file, key)
    except Exception as err:
        return False,err

    return True,val


def _setxattr(file, key, val):
    try:
        xattr.setxattr(file, key, val)
    except Exception as err:
        return False,err

    return True,''


def _removexattr(file, key):
    try:
        xattr.removexattr(file, key)
    except Exception as err:
        return False,err

    return True,''


def _lockfile(file):
    return _setxattr(file,'trusted.glusterfs.cs.remoting', 'lock')


def _unlockfile(file):
    return _removexattr(file,'trusted.glusterfs.cs.remoting')


def _s3_upload(filelist, volinfo, volume):
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

    logger.debug(volinfo)
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
        logger.error(err)
        return -1

    if not bucket:
        try:
            minioClient.make_bucket(volinfo['s3plugin-bucketid'], location="us-east-1")
        except BucketAlreadyOwnedByYou as err:
            logger.error(err)
            pass
        except BucketAlreadyExists as err:
            logger.error(err)
            pass
        except ResponseError as err:
            logger.error(err)
            raise

    upfileinfo['buketid'] = volinfo['s3plugin-bucketid']
    upfileinfo['volname'] = volume
    upfileinfo['uptime'] = time.time()
    upfileinfo['uppath'] = volume
    upfileinfo['totalsize'] = 0
    upfileinfo['filenu'] = 0
    upfileinfo['success'] = 0
    upfileinfo['failed'] = 0

    filenu = 0
    mp = "/cloudsync/gluster/%s" % volume.strip()
    for src in filelist:
        filenu = filenu + 1
        try:
            dest = src.split(mp)[1]
            cms = format(time.time(), '.6f')
            dest = dest + "_" + cms

            logger.debug("%s:%s:%s",volinfo['s3plugin-bucketid'],dest,src)

            stat,msg =  _getxattr(src, 'trusted.glusterfs.cs.remote')
            if stat:
                upfileinfo['failed'] = upfileinfo['failed'] + 1 
                logger.error("%s has been upload", src)
                continue

            #before upload file,lock file
            stat,msg =  _lockfile(src)
            if not stat:
                upfileinfo['failed'] = upfileinfo['failed'] + 1 
                logger.error("%s:%s", src, msg)
                continue

            minioClient.fput_object(volinfo['s3plugin-bucketid'], dest.lstrip('/'), src)

            #uoload end,unlock file
            stat,msg =  _unlockfile(src)
            if not stat:
                logger.error("%s:%s", src, msg)

            stat,msg =  _setxattr(src, 'trusted.glusterfs.csou.complete', dest.lstrip('/'))
            if not stat:
                logger.error("%s:%s", src, msg)

            upfileinfo['success'] = upfileinfo['success'] + 1
            #upfileinfo['totalsize'] = upfileinfo['totalsize'] + file[4]
            logger.info("[success:%d]: %s", filenu, src)
        except ResponseError as err:
            upfileinfo['failed'] = upfileinfo['failed'] + 1
            logger.error(err)
            continue
        except InvalidArgumentError as err:
            upfileinfo['failed'] = upfileinfo['failed'] + 1 
            logger.error("%s:%s",src,err)
            continue
        except Exception as err:
            upfileinfo['failed'] = upfileinfo['failed'] + 1 
            logger.error("%s:%s",src,err)
            continue

    return 0, upfileinfo


def _sqlite3_update(upinfo):
    sqinfo = {}
    conn = sqlite3.connect(sq3dbpath)  
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS cloudsync
           (id            integer primary key autoincrement,
           buketid        CHAR(50),
           volname        CHAR(50),
           uppath         CHAR(50),
           totalsize      INT     NOT NULL,
           filenu         INT     NOT NULL,
           success        INT     NOT NULL,
           failed         INT     NOT NULL)''')
        conn.commit()
    except Exception as err:
        sqinfo['message'] = err
        conn.close()
        return -1, sqinfo
    
    try:
        sqlcmd = "SELECT * from cloudsync WHERE buketid = '%s'" % upinfo['buketid']
        cursor = c.execute(sqlcmd)
        sqlinfo = c.fetchone()
    except Exception as err:
        sqinfo['message'] = err
        conn.close()
        return -1, sqinfo
 
    if not sqlinfo:
        sqlcmd = \
        "INSERT INTO cloudsync\
        (buketid, volname, uppath, totalsize, filenu, success, failed) \
        VALUES ('%s','%s','%s',%f,%d,%d,%d) " % (\
        upinfo['buketid'], \
        upinfo['volname'], \
        upinfo['uppath'], \
        upinfo['totalsize'], \
        upinfo['filenu'], \
        upinfo['success'], \
        upinfo['failed'])

        try:
            cursor = c.execute(sqlcmd)
            conn.commit()
        except Exception as err:
            sqinfo['message'] = err
            conn.close()
            return -1, sqinfo

    sqinfo['message'] = 'ok'
    conn.close()
    return 0, sqinfo

def _fileter(files, volume):
    localfiles = []
    for f in files:
        path = "/cloudsync/gluster/%s/%s" % (volume, f.strip().strip('/'))

        status,val = _getxattr(path, 'trusted.glusterfs.cs.remote')
        if not status:
            logger.debug("%s trusted.glusterfs.cs.remote not set",path)
            localfiles.append(path)
            continue
        
    logger.debug("local files for upload...%s",localfiles)
    return localfiles


def s3_upload(files, volinfo, volume):
     
    localfiles = _fileter(files, volume)

    logger.info("All files:%d,Fileter files:%d",len(files),len(localfiles))

    if not localfiles:
        logger.debug("localfiles is null")
        return 0
    
    upinfo = _s3_upload(localfiles, volinfo, volume)

    logger.info(upinfo)

    #_sqlite3_update(upinfo)

    return 0
