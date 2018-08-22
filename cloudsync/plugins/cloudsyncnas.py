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

sq3dbpath = '/var/lib/glusterd/xdfs-cloudsyncnas-upload.db'

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


def _upload_file(dest, src):
    if (not dest) or (not src):
        return -1,'null'

    d = os.path.dirname(dest)
    logger.debug("dest:%s src:%s",dest,src)
    try:
        if not os.path.exists(d):
            os.makedirs(d)
    except Exception as err:
        return -1,err

    try:
        shutil.copyfile(src, dest)
    except Exception as err:
        return -1,err

    return 0,''

def _nas_upload(filelist, volinfo, volume):
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

    upfileinfo['cs-storetype'] = volinfo['cs-storetype']
    upfileinfo['nasplugin-share'] = volinfo['nasplugin-share']
    upfileinfo['volname'] = volume
    upfileinfo['uptime'] = time.time()
    upfileinfo['uppath'] = volume
    upfileinfo['totalsize'] = 0 
    upfileinfo['filenu'] = 0 
    upfileinfo['success'] = 0 
    upfileinfo['failed'] = 0 

    filenu = 0 
    nasmountpoint = "/cloudsync/nas/%s" % volume
    for src in filelist:
        filenu = filenu + 1 
        try:
            mp = "/cloudsync/gluster/%s" % volume.strip()
            dest = src.split(mp)[1]
            cms = format(time.time(), '.6f')
            fxattr = dest + "_" + cms
            dest = nasmountpoint + fxattr

            logger.debug("%s:%s:%s",volinfo['nasplugin-share'],dest,src)

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

            stat,msg =  _upload_file(dest, src)
            if stat:
                logger.error(msg)

            #upload end,unlock file
            stat,msg =  _unlockfile(src)
            if not stat:
                logger.error("%s:%s", src, msg)

            stat,msg =  _setxattr(src, 'trusted.glusterfs.csou.complete', fxattr.lstrip('/'))
            if not stat:
                logger.error("%s:%s", src, msg)

            upfileinfo['success'] = upfileinfo['success'] + 1
            #upfileinfo['totalsize'] = upfileinfo['totalsize'] + file[4]
            logger.info("[success:%d]: %s", filenu, src)
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


def nas_upload(files, volinfo, volume):
     
    localfiles = _fileter(files, volume)

    logger.info("All files:%d,Fileter files:%d",len(files),len(localfiles))

    if not localfiles:
        logger.debug("localfiles is null")
        return 0
    
    upinfo = _nas_upload(localfiles, volinfo, volume)

    logger.info(upinfo)

    #_sqlite3_update(upinfo)

    return 0
