#!/usr/bin/python 
#*-*coding:utf8*-* 

import os
import sys
import stat
import time
import xattr
import shutil
import logging
import commands

from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from daemon.daemon import *
from glusters.glfstats import *
from plugins.cloudsyncs3 import s3_upload
from plugins.cloudsyncnas import nas_upload

progname = __file__.split('.')[0]
default_logfile = '/var/log/glusterfs/cloudsync/%s.log' % progname
default_pidfile = '/var/run/%s.pid' % progname
default_frequency = 2


class Csagentd(Daemon):
    def __init__(self, volinfo, pidfile, stdout, stderr, name):
        self.volinfo = volinfo
        self.logger = logging.getLogger(__name__)
        self.gluster_point = '/cloudsync/gluster/%s' % name
        self.nas_point = '/cloudsync/nas/%s' % name

        try:
            if not os.path.exists(self.gluster_mountpoint):
                os.makedirs(self.gluster_mountpoint)
            if not os.path.exists(self.nas_mountpoint):
                os.makedirs(self.nas_mountpoint)
        except Exception as err:
            return -1,err

        super(Csagentd, self).__init__(
            pidfile = pidfile, stdout=stdout, stderr=stderr, name=name)

    def update_volinfo(self):
        stats = GlusterStats()
        self.volinfo = stats.get_one_volume_infos(self.name)

    def active_watermark_thread(self):
        path = self.gluster_mountpoint

        xattr.setxattr(path, 'trusted.glusterfs.cs.active_watermark_thread', '9')

        time.sleep(2)
        times = 2
        while True:
            if(times > 30):
                self.logger.debug("active status: overtime")
                break

            filexattr = xattr.getxattr(path, 'trusted.glusterfs.cs.active_watermark_thread')

            if(filexattr[0] == '0'):
                self.logger.debug("active status:0")
                return True
            else:
                self.logger.debug("active status:%s",filexattr[0])
                time.sleep(2)
                times = times + 2
                continue

        return False

    def mount_gluster_vol(self):
        cmd = "mount |grep %s" % self.gluster_mountpoint
        status,message = commands.getstatusoutput(cmd)
        if not status:
            self.logger.debug("%s has been mounted",self.gluster_mountpoint)
            return status

        cmd = "mount.glusterfs  127.0.0.1:/%s %s" % (self.name, self.gluster_mountpoint)
        self.logger.info(cmd)
        status,message = commands.getstatusoutput(cmd)
        if status:
            self.logger.info("%s  failed", cmd)

        self.logger.info("%s  successfully", cmd)
        
        return status

    def mount_nas_vol(self):
        cmd = "mount |grep %s" % self.nas_mountpoint
        status,message = commands.getstatusoutput(cmd)
        if not status:
            self.logger.debug("%s has been mounted",self.nas_mountpoint)
            return status

        cmd = "mount  %s:/%s" % (self.nashostname, self.nasshare)
        self.logger.info(cmd)
        status,message = commands.getstatusoutput(cmd)
        if status:
            self.logger.info("%s  failed", cmd)

        self.logger.info("%s  successfully", cmd)
        
        return status

    def _get_allfiles(self):
        filedir = '/var/run/gluster/%s-cloudsync/' % self.name

        self.logger.debug(filedir)

        try:
            filenames = os.listdir(filedir)
        except Exception as err:
            self.logger.error(err)
            return []
        
        file_context = []
        for filename in filenames:
            self.logger.debug("%s: %s",self.name, filename)
            fname, ftype = os.path.splitext(filename)
            if (ftype != '.qf'):
                continue

            path = filedir + filename
            file_context = file_context + open(path.strip()).readlines() 
        
        return file_context
          
    def nas_upload_files(self):
        files = self._get_allfiles()
        if not files:
            return False
       
        #for i in range(len(files)):
        #    files[i] = files[i].strip('\n')

        #print files
        nas_upload(files, self.volinfo, self.name)
        return True

    def s3_upload_files(self):
        files = self._get_allfiles()
        if not files:
            return False
       
        #for i in range(len(files)):
        #    files[i] = files[i].strip('\n')

        #print files
        s3_upload(files, self.volinfo, self.name)
        return True

    def run(self):
        self.logger.info("pidid:%s is running...",os.getpid())

        while True:
            if not self.volinfo:
                time.sleep(default_frequency)
                self.logger.warning("please check %s 'cloudsync' status!",self.name)
                continue

            f = (int)(self.volinfo['cs-watermark-thread-frequency'])
            self.logger.info("cs-watermark-thread-frequency:%d",f)

            if self.volinfo['cloudsync'] != 'on':
                self.logger.warning("please check %s 'cloudsync' status!",self.name)
                time.sleep(f)
                self.update_volinfo()
                continue

            self.mount_gluster_vol()

            '''
            status = self.active_watermark_thread()
            if not status:
                self.logger.warning("active status is false")
                time.sleep(f)
                self.update_volinfo()
                continue
            '''
            
            if self.volinfo['cloudsync'] != 'cloudsyncnas':
                self.logger.debug("cs-storetype : cloudsyncnas")
                self.nasshare = volinfo['nasplugin-share'].strip('/')
                self.nashostname = volinfo['nasplugin-hostname']
                self.mount_nas_vol()
                self.nas_upload_files()
            elif self.volinfo['cloudsync'] != 'cloudsyncs3':
                self.logger.debug("cs-storetype : cloudsyncs3")
                self.s3_upload_files()
            else:
                self.logger.error("cs-storetype : null")
                time.sleep(f)
                continue

            self.update_volinfo()

            time.sleep(f)

def main():
    agentd = Csagentd(pidfile=default_pidfile, 
                    volinfo='test', 
                    stdout=default_logfile, 
                    stderr=default_logfile,
                    name=progname)

    #agentd.start()
    agentd.run()
   

if __name__ == "__main__": 
    main()
