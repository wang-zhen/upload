#!/usr/bin/env python2

import sys, os, time, atexit
import fcntl, errno
from signal import SIGTERM


def _lock_file(key):
    key = os.path.abspath(key)
    parent = os.path.dirname(key)
    if not os.path.exists(parent):
        os.makedirs(parent)

    lock_fd = open(key, 'a')

    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as err:
        if err.errno == errno.EAGAIN:
            exit(err.errno)
        else:
            raise

    lock_fd.truncate(0)
    return lock_fd


def _try_lock_file(key):
    key = os.path.abspath(key)
    if not os.path.exists(key):
        raise Exception(key + ' not exist')

    lock_fd = open(key, 'a')

    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)


class Daemon(object):
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """

    def __init__(self,
                 pidfile,
                 stdin=os.devnull,
                 stdout=os.devnull,
                 stderr=os.devnull,
                 name="noname"):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.name = name

        #piddir = os.path.dirname(self.pidfile)
        #if not os.path.exists(piddir):
        #    os.makedirs(piddir)

    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno,
                                                            e.strerror))
            sys.exit(1)

        os.chdir("/")
        os.setsid()
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno,
                                                            e.strerror))
            sys.exit(1)

        print('start: %s @ %s' % (self.name, self.pidfile))

        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r') if type(self.stdin) != file else self.stdin
        so = file(self.stdout, 'a+',
                  0) if type(self.stdout) != file else self.stdout
        se = file(self.stderr, 'a+',
                  0) if type(self.stderr) != file else self.stderr

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        open(self.pidfile, 'w').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def getpid(self):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except:
            pid = None

        return pid

    def start(self):
        # Check for a pidfile to see if the daemon already runs
        pid = _lock_file(self.pidfile)

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        # Get the pid from the pidfile
        pid = self.getpid()

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return

        # Try killing the daemon process
        try:
            while True:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                sys.stderr.write(err)
                sys.exit(1)

    def stat(self):
        if not os.path.exists(self.pidfile):
            return False

        try:
            _try_lock_file(self.pidfile)
        except Exception, e:
            return True

        return False

    def restart(self):
        self.stop()
        self.start()

    def run(self):
        """
            You should override this method when you subclass Daemon. It will be called after the process has been
            daemonized by start() or restart().
        """


class TestAgentd(Daemon):
    def __init__(self):
        name = 'agentd'
        pidfile = '/tmp/run/%s.pid' % name
        logfile = '/tmp/log/%s.log' % name
        for f in pidfile, logfile:
            d = os.path.dirname(f)
            if not os.path.exists(d):
                os.makedirs(d)
            if not os.path.exists(f):
                f = open(f, 'a')
                f.truncate(0)
                f.close()
        super(Daemon, self).__init__(
            pidfile, stdout=logfile, stderr=logfile, name=name)

    def run(self):
        from datetime import datetime
        while True:
            print 'running: %s' % datetime.now()
            time.sleep(1)


if __name__ == "__main__":
    agentd = TestAgentd()

    args = sys.argv
    if 'start' in args:
        agentd.start()
    elif 'stop' in args:
        agentd.stop()
    elif 'status' in args:
        print 'running pid: %s' % agentd.getpid() if agentd.stat(
        ) else 'stoped'
    elif 'restart' in args:
        agentd.restart()
    elif 'test' in args:
        agentd.run()
    else:
        print 'usage: %s <start|stop|status|restart|test>' % args[0]
