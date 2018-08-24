#!/usr/bin/python 
#*-*coding:utf8*-* 

import os
import sys 
import stat
import time
import xattr
import threading

def test():
    filedir = '/var/run/gluster/vol3-cloudsync/'
    try:
        filenames = os.listdir(filedir)
    except Exception as err:
        return []

    file_context = []
    for filename in filenames:
        fname, ftype = os.path.splitext(filename)
        if (ftype != '.qf'):
            continue

        path = filedir + filename
        file_context = file_context + open(path.strip()).readlines() 
        os.remove(path) 
          
    return file_context

def test1():
    while(1):
     print "111"
     time.sleep(0)

if __name__ == "__main__":
    #print test()
    test1()
