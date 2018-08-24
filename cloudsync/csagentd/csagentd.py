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
        self.gluster_mountpoint = '/cloudsync/gluster/%s' % name
        self.nas_mountpoint = '/cloudsync/nas/%s' % name
        self.status = True

        try:
            if not os.path.exists(self.gluster_mountpoint):
                os.makedirs(self.gluster_mountpoint)
            if not os.path.exists(self.nas_mountpoint):
                os.makedirs(self.nas_mountpoint)
        except Exception as err:
            self.logger.error(err)
            self.status = False
            return None

        super(Csagentd, self).__init__(
            pidfile = pidfile, stdout=stdout, stderr=stderr, name=name)

    def update_volinfo(self):
        stats = GlusterStats()
        self.volinfo = stats.get_one_volume_infos(self.name)

    def active_watermark_thread(self):

        self.logger.debug("active_watermark_thread")

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
        else:
            self.logger.info("%s  successfully", cmd)
        
        return status

    def mount_nas_vol(self):
        cmd = "mount |grep %s" % self.nas_mountpoint
        status,message = commands.getstatusoutput(cmd)
        if not status:
            self.logger.debug("%s has been mounted",self.nas_mountpoint)
            return status

        if self.nastype == 'cifs':
            if self.nasuser and self.naspasswd:
                cmd = "mount -t cifs -o username='%s',password='%s' //%s/%s %s" % \
                      (self.nasuser, self.naspasswd, self.nashostname, \
                       self.nasshare, self.nas_mountpoint)
            else:
                cmd = "mount -t cifs //%s/%s %s" % \
                      (self.nashostname, self.nasshare, self.nas_mountpoint)

        if self.nastype == 'nfs':
            cmd = "mount  %s:/%s %s" % \
                  (self.nashostname, self.nasshare, self.nas_mountpoint)

        self.logger.info(cmd)

        status,message = commands.getstatusoutput(cmd)
        if status:
            self.logger.error(message)
        else:
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
            os.remove(path)
 
        return file_context
          
    def nas_upload_files(self):
        files = self._get_allfiles()
        if not files:
            self.logger.debug("scanner: files in null")
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

    def stop(self):
        cmd = "umount %s" % self.nas_mountpoint
        status,message = commands.getstatusoutput(cmd)
        if status:
            self.logger.info(message)
        else:
            self.logger.info("%s  successfully", cmd)

        cmd = "umount %s" % self.gluster_mountpoint
        status,message = commands.getstatusoutput(cmd)
        if status:
            self.logger.info(message)
        else:
            self.logger.info("%s  successfully", cmd)

        self.logger.info("volume(%s) pid(%s) is about to stop",self.name, self.getpid())
        super(Csagentd, self).stop()

    def run(self):
        self.logger.info("volume(%s) pid(%s) is running...",self.name,os.getpid())
        self.logger.debug(self.volinfo)

        f1 = 1
        f2 = default_frequency
        f3 = default_frequency

        while True:
            time.sleep(f1)
            self.update_volinfo()

            if (not self.volinfo):
                self.logger.warning("volume %s infos in %s is null!",self.name)
                continue

            if (self.volinfo.has_key('cs-watermark-thread-frequency')):
                f1 = (int)(self.volinfo['cs-watermark-thread-frequency'])
            else:
                f1 = default_frequency

            self.logger.info("cs-watermark-thread-frequency:%d",f1)

            if self.volinfo['status'] == 'stop':
                self.logger.warning("Volume %s is not started!",self.name)
                continue

            if not self.volinfo.has_key('cloudsync'):
                self.logger.warning("Volume:%s  cloudsync feature is not supported!",self.name)
                continue

            if self.volinfo['cloudsync'] != 'on':
                self.logger.warning("Volume:%s  cloudsync feature is off!",self.name)
                continue

            if self.mount_gluster_vol():
                self.logger.error("mount_gluster_vol failed")
                continue

            status = self.active_watermark_thread()
            if not status:
                self.logger.error("active watermark failed")
                continue
            
            if self.volinfo['cs-storetype'] == 'cloudsyncnas':
                self.logger.debug("cs-storetype : cloudsyncnas, nastype : cifs")
                self.nasshare = self.volinfo['nasplugin-share'].strip('/')
                self.nashostname = self.volinfo['nasplugin-hostname'].strip('/')
                self.nastype = 'cifs' 

                self.nasuser = ''
                self.naspasswd = ''
                if (self.volinfo.has_key('nasplugin-user')):
                    self.nasuser = self.volinfo['nasplugin-user']
                if (self.volinfo.has_key('nasplugin-user')):
                    self.naspasswd = self.volinfo['nasplugin-passwd']

                if self.mount_nas_vol():
                    self.logger.error("mount_nas_vol failed")
                    continue

                if self.nas_upload_files():
                    self.logger.error("nas_upload_files failed")
                    continue
            elif self.volinfo['cs-storetype'] == 'cloudsyncs3':
                self.logger.debug("cs-storetype : cloudsyncs3")
                if self.s3_upload_files():
                    self.logger.error("s3_upload_files failed")
                    continue
            else:
                self.logger.error("cs-storetype : null")


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
