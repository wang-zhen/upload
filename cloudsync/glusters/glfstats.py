#!/usr/bin/python 
#*-*coding:utf8*-* 

import os
import re
import sys 
import json
import time
import shlex
import signal
import argparse
import subprocess

from threading import Timer

class GlusterStats(object):
    """ Collect stats related to gluster from localhost """
    
    def __init__(self, timeout=None, test_file=False,
                 record_mode=False):
        self.test_commands = []
        if test_file:
            self.test_commands = self._load_test_file(test_file)

        self.timeout = timeout
        self.record_mode=record_mode
        if self.record_mode:
            self.responses = []

        self.gluster_version = self.get_gluster_version()
        self.volumes = self.get_volumes()

    def get_gluster_version(self):
        return self._execute("gluster --version")['stdout'].split()[1]
    
    def get_volumes(self):
        return self._execute("gluster volume list")['stdout'].strip().split()

    def get_glusterd(self):
        return self._execute("pidof glusterd")['stdout'].strip().split()

    def get_glusterfsd(self):
        return self._execute("pidof glusterfsd")['stdout'].strip().split()

    def get_number_peers(self):
        output = self._execute("gluster peer status")['stdout'].strip()
        return output.count("Peer in Cluster (Connected)")

    def _parse_brick_entries(self, volume, all_entries):
        """ 
        volume vol1-cloudsync
            type features/cloudsync
            option cs-watermark-thread-frequency 15
            option cs-max-mb 4000
            option cs-max-files 1000
            option s3plugin-https off 
            option s3plugin-hostname 192.168.2.95:9000
            option s3plugin-bucketid glvol2
            option s3plugin-keyid SIWSWX33WD7K8HADNYVU
            option s3plugin-seckey u1fIgYyWlyNjJC3XjM8gH+i908EQk/OBkb9Cxjmk
            option cs-storetype cloudsyncs3
            option cs-worm on
            option cloudsync on
            subvolumes vol1-dht
        end-volume
        """
        flag = 0
        infos = {}
        current_volume = {}
        xlator = "volume %s-cloudsync" % volume
        for line in all_entries.split('\n'):
            if line == xlator:
                flag = 1
                continue
            if flag and (line == "end-volume"):
                break;
            if flag:
                fields = line.strip().split(' ')
                if fields[0] == 'option':
                    key = fields[1]
                    current_volume[key] = fields[2]

        infos[volume] = current_volume

        return infos

    def get_one_volume_infos(self, volume):
        """ 
            Get one gluster volume infos from /var/lib/glusterd/vols/
        """
        volpath = "cat /var/lib/glusterd/vols/%s/%s.tcp-fuse.vol"
        infos = {}

        cmd = volpath % (volume,volume)
        all_entries = self._execute(cmd)
        info = self._parse_brick_entries(volume, all_entries['stdout'])

        for k,v in info.items():
            infos = v.copy()

        cmd = "gluster vol status %s" % (volume)
        if self._execute(cmd)['return_code']:
            infos['status'] = 'stop'
        else:
            infos['status'] = 'start'

        return infos

    def get_volume_infos(self):
        """ 
            Get all gluster volume infos from /var/lib/glusterd/vols/
        """
        infos = {}
        for volume in self.volumes:
            infos[volume] = self.get_one_volume_infos(volume)

        return infos

    def get_stats(self):
        self.glusterd = self.get_glusterd()
        self.glusterfsd = self.get_glusterfsd()
        self.peers = self.get_number_peers()
        self.volume_infos = self.get_volume_infos()

        return self._format_stats()
    
    def _format_stats(self):
        out = {}
        out['volume_count'] = len(self.volumes)
        out['volumes'] = self.volumes
        out['glusterd'] = len(self.glusterd)
        out['glusterfsd'] = len(self.glusterfsd)
        out['peers'] = self.peers
        out['volume_infos'] = self.volume_infos
        out['gluster_version'] = self.gluster_version
        return out
   
    def _kill_process_tree(self, process, timeout_happened):
        timeout_happened["value"] = True
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGTERM)

    def _execute(self, cmd):
        p = subprocess.Popen(shlex.split(cmd),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=os.setpgrp)

        timeout_happened = {"value": False}
        if self.timeout:
            timer = Timer(self.timeout, self._kill_process_tree, [p, timeout_happened])
            timer.start()
        stdout, stderr = p.communicate()
        if self.timeout:
            timer.cancel()

        if p.returncode > 0:
            error = "ERROR: command '%s' failed with:\n%s%s" % (cmd, stdout, stderr)
            #sys.stderr.write(error)
            #sys.exit(p.returncode)
        response = {'command': cmd,
                    'stdout': stdout,
                    'stderr': stderr,
                    'timeout_happened': timeout_happened['value'],
                    'return_code': p.returncode,
                   }
                   
        if self.record_mode:
            if 'heal' in cmd:
                response['stdout'] = self._strip_filenames_from_response(stdout)
            self.responses.append(response)
        return response


def main():
    stats = GlusterStats()
    
    #stats.get_volume_infos()
    print (stats.get_stats())
    #print (stats.get_one_volume_infos('test1'))
    #print (stats.get_volume_infos())

if __name__ == '__main__':
    main()
