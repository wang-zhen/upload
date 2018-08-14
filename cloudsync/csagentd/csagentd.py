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
from utils.cloudsync_upload import cloudsync_upload

progname = __file__.split('.')[0]
default_logfile = '/var/log/glusterfs/cloudsync/%s.log' % progname
default_pidfile = '/var/run/%s.pid' % progname
default_frequency = 2


class Csagentd(Daemon):
    def __init__(self, volinfo, pidfile, stdout, stderr, name):
        self.volinfo = volinfo
        self.logger = logging.getLogger(__name__)
        self.mount_point = '/upload/%s' % name

        try:
            if not os.path.exists(self.mount_point):
                os.makedirs(self.mount_point)
        except Exception as err:
            return -1,err

        super(Csagentd, self).__init__(
            pidfile = pidfile, stdout=stdout, stderr=stderr, name=name)

    def update_volinfo(self):
        stats = GlusterStats()
        self.volinfo = stats.get_one_volume_infos(self.name)

    def active_watermark_thread(self):
        path = self.mount_point

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

    def mount_local_vol(self):
        cmd = "mount.glusterfs  127.0.0.1:/%s %s" % (self.name, self.mount_point)
        self.logger.info(cmd)
        status,message = commands.getstatusoutput(cmd)
        if not status:
            self.logger.error("mount.glusterfs err")
        
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
          
    def upload_allfiles(self):
        files = self._get_allfiles()
        if not files:
            return False
       
        #for i in range(len(files)):
        #    files[i] = files[i].strip('\n')

        #print files
        cloudsync_upload(files, self.volinfo, self.name)
        return 0

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

            self.mount_local_vol()

            status = self.active_watermark_thread()
            if not status:
                self.logger.warning("active status is false")
                time.sleep(f)
                self.update_volinfo()
                continue
            
            self.upload_allfiles()
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
