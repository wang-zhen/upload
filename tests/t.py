#!/usr/bin/python 
#*-*coding:utf8*-* 

import os
import sys 
import stat
import time
import xattr
import threading


def fun_timer():
    print('hello timer')
    global timer
    timer = threading.Timer(2,fun_timer)

    timer.start()

def test():
    timer = threading.Timer(2,fun_timer)
    timer.start()
    while(True):
        print('test')
        time.sleep(2)

if __name__ == "__main__":
    test()
