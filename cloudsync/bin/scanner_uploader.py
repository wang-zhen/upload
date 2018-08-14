#!/usr/bin/python 
#*-*coding:utf8*-* 

"""
Written by: WangZhen linux_wz@163.com
Written on: 18 Jul 2018
Description: Upload .
"""

import os
import sys
import stat
import time
import glob
import xattr
import getopt
import socket
import shutil
import logging
import commands
import ConfigParser

from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from daemon.daemon import *
from glusters.glfstats import *
from csagentd.csagentd import *

progname = 'scanner_uploader'
#progname = __file__.split('.')[0]
logfile = '/var/log/glusterfs/cloudsync/%s.log' % progname
pidfile = '/var/run/cloudsync/%s' % progname
loglevel = logging.INFO
glcmd = ''
runmode = 'daemon'

shortargs = 'L:p:l:nc:h'
longargs = ['log-level=', 'pid-file=', 'log-file=', 'no-daemon', 'glcmd', 'help']


def usage(val):
    print "Usage: python " +  sys.argv[0] + " [OPTION] [VALUE]"
    print "\t-L --log-level             Logging severity. Valid options are DEBUG, INFO,"
    print "\t                           NOTICE, WARNING, ERROR, CRITICAL and ALERT [default:"
    print "\t                           INFO]"
    print "\t-p, --pid-file=PIDFILE     File to use as pid file"
    print "\t-l, --log-file=LOGFILE     File to use as log file"
    print "\t-c, --cmd                  start, stop, restart"
    #print "\t-n, --no-daemon            Run in foreground"
    print "\t-h, --help"

    print "Example:"
    print "\t" "python " + sys.argv[0]
    sys.exit(val)


def process_parameters():

    try:
        opts, args = getopt.getopt( sys.argv[1:], shortargs, longargs )
    except getopt.GetoptError as err:
        print('\033[1;31;40m%s!\033[0m' % str(err))
        usage(-1)

    global loglevel
    global logfile
    global pidfile
    global runmode
    global glcmd

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(0)
        elif o in ("-L", "--log-level"):
            try:
                loglevel = getattr(logging, a)
            except AttributeError as err:
                return -1,err            
        elif o in ("-p", "--pid-file"):
            pidfile = a
        elif o in ("-l", "--log-file"):
            logfile = a
        elif o in ("-n", "--no-daemon"):
            runmode = 'no-daemon'
        elif o in ("-c", "--cmd"):
            glcmd = a

    return 0,''

def log_init():
    f = logfile
    try:
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        if not os.path.exists(f):
            f = open(f, 'a')
            f.truncate(0)
            f.close()
    except Exception as err:
        return -1,err

    log_format = "[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)d]: %(message)s"
    date_format = "%Y/%d/%m %H:%M:%S"
    logging.basicConfig(filename=logfile, \
                       level = loglevel, \
                       format = log_format, \
                       datefmt=date_format)

    return 0,''

def _start_csagent(volume, volinfo):
 
    if (runmode == 'daemon'):
        try:
            pid = os.fork()
            if pid > 0:
                return 0
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno,
                                                           e.strerror))
            sys.exit(1)

    f = pidfile + volume + '.pid'
    try:
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        if not os.path.exists(f):
            h = open(f, 'a')
            h.truncate(0)
            h.close()
    except Exception as err:
        return -1,err

    csagentd = Csagentd(pidfile=f, 
                    volinfo=volinfo, 
                    stdout=logfile, 
                    stderr=logfile,
                    name=volume)

    if runmode == 'daemon':
        csagentd.start()
    else:
        csagentd.run()
 
    return 0,csagentd

   
def _stop_csagent(volume, volinfo):
    f = pidfile + volume + '.pid'

    csagentd = Csagentd(pidfile=f, 
                    volinfo=volinfo, 
                    stdout=logfile, 
                    stderr=logfile,
                    name=progname)

    csagentd.stop()

    return 0,''


def start_csagent(glfstatus):

    volinfos = glfstatus['volume_infos']

    for vol in glfstatus['volumes']:
        _start_csagent(vol, volinfos[vol])
        time.sleep(0.1)


def stop_csagent(glfstatus):

    volinfos = glfstatus['volume_infos']

    for vol in glfstatus['volumes']:
        _stop_csagent(vol, volinfos[vol])
        time.sleep(0.1)


def restart_csagent(glfstatus):
    stop_csagent(glfstatus)
    time.sleep(1)
    start_csagent(glfstatus)


def main():
    # args
    status, val = process_parameters();
    if status:
        print val
        sys.exit(status)

    status, val = log_init()
    if status:
        print val
        sys.exit(status)

    stats = GlusterStats()
    glfstatus = stats.get_stats()
    #volinfos = stats.get_one_volume_infos('test1')
    #print volinfos

    if(glfstatus['glusterd'] == 0):
        sys.exit(-1)
    if(glfstatus['glusterfsd'] == 0):
        print "check glusterfsd status!" 
        sys.exit(-1)

    if glcmd == 'start':
        start_csagent(glfstatus)
    elif glcmd == 'stop':
        stop_csagent(glfstatus)
    elif glcmd == 'restart':
        restart_csagent(glfstatus)
    else:
        usage(-1)


if __name__ == "__main__": 
    main()
    sys.exit(0)

