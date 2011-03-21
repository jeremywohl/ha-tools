#!/usr/bin/env python

#
# mysql_haproxy_check.py -- Verify MySQL is responsive and, if a slave, is active and not too far behind
#
#  Expects to receive the following URI from HAProxy.
#
#     /check?port=xx&max_seconds_behind=xx
#
#  Vends 500 if URI is missing parameters
#  Vends 503 if mysql connection or slave status is off
#  Vends 200 if everything is happy
#
#  This is a self-hosting HTTP server (see PORT below).
#
#    https://github.com/jeremywohl/ha-tools/mysql_haproxy_check.py
#    jeremywohl@gmail.com
#
#    Public domain.
#

import os, sys, traceback, atexit, time, re, cgi, MySQLdb
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

PORT = 9200
CONF = '/etc/my.cnf'

USER = 'root'
PASS = 'myrootpass'

# Parse CONF for port/socket pairs; support Pythons without ConfigParser (2.6+)
class MysqlConfig(object):
    last_parse = 0
    sockets    = {}

    def __parse_mysql_conf(self):

        # if it's been at least 5 minutes since last parse, reread
        if time.time() - self.last_parse < 5 * 60:
            return
        
        port = socket = None
        
        for line in open(CONF, 'r').readlines():
            if line[0] == '[':
                if socket and not port:
                    self.sockets['3306'] = socket
                port = socket = None
                continue
            if line.find('=') > -1:
                key, value = re.split(r'\s*=\s*', line, 1)
                if key == 'port':
                    port = value.strip()
                if key == 'socket':
                    socket = value.strip()
                if port and socket:
                    self.sockets[port] = socket

        self.last_parse = time.time()

    def socket_file(self, port):
        self.__parse_mysql_conf()
        return self.sockets[port]

# Respond to queries with mysql checks
class MysqlCheckHandler(BaseHTTPRequestHandler):
    def respond(self, code):
        self.wfile.write("HTTP/1.0 %d %s\r\n" % (code, self.responses[code][0]))
        self.wfile.write("\r\n")

    def do_GET(self):
        status = 0

        #print "-- new request"
        #print "Path: %s" % self.path

        if self.path.find('?') == -1:  # no query params?
            self.respond(500)
            return

        dummy, self.query_string = self.path.split('?', 1)
        params = cgi.parse_qs(self.query_string)

        #print "Params: %s" % params

        try:
            global config
            
            db = MySQLdb.connect(host='localhost', user=USER, passwd=PASS, db='mysql', port=int(params['port'][0]),
                                 unix_socket=config.socket_file(port=params['port'][0]), connect_timeout=1)
            cursor = db.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute("show slave status")
            row = cursor.fetchone()

            status = 200

            if row == None:
                # assume master
                pass
            else:
                # assume slave
                if row['Slave_IO_Running'] != 'Yes' or row['Slave_SQL_Running'] != 'Yes' or row['Seconds_Behind_Master'] > int(params['max_seconds_behind'][0]):
                    status = 503

            cursor.close()
            db.close()
        except Exception, e:
            traceback.print_exc()
            status = 503
        
        self.respond(status)
        return

class Server:
    def __init__(self):
        self.stdin   = '/dev/null'
        self.stdout  = '/dev/null'
        self.stderr  = '/dev/null'
        self.pidfile = '/var/run/mysql_haproxy_check.pid'
        
    # see http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed")
            sys.exit(1)

        os.chdir('/')
        os.setsid()
        os.umask(0)

        # second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed")
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def run(self):
        self.daemonize()  # comment out this line to debug in the foreground
        
        global config
        config = MysqlConfig()
        http   = HTTPServer(('', PORT), MysqlCheckHandler)

        http.serve_forever()

Server().run()
