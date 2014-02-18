__author__ = 'sharvey'

import json
import os, os.path
import requests
import shutil
import socket
import sys
import tarfile
import tempfile
import threading
import time
import SocketServer
import parser
from store import Store
import creds
from reddit import Reddit

datestart = '20131201'
dateend   = '20131231'
tmpdir = '/tmp/reddit'
debug = False
crawl_author = False

class DP:
    # list of authors that are remembered
    author_list = []
    # list of authors to skip crawling for various reasons
    skip_list = ['[deleted]']
    # internal sqlite3 store
    store = None
    
    def write_to_queue(self, data, prefix='tmp'):
        fh, filename = tempfile.mkstemp(dir=os.path.join(tmpdir, 'dp', 'queue'), prefix=prefix)
        os.close(fh)
        fp = open(filename, 'w')
        fp.write(data)
        fp.close()
        return os.path.split(filename)[1]

    def seed(self):
        self.store = Store('/collection/sharvey/reddit/')
        self.store.open()
        print 'Created seed queue'
        return self.write_to_queue('a,t3_1u4kuf', 'tmp_a_')

    def process_author(self, abspath, filename):
        filetype = filename.split('_')
        fp = open(os.path.join(abspath, filename))
        blob = json.load(fp)
        fp.close()
        elements = parser.extract_listing_elements(blob)
        self.store.store_author(elements)
        return []

    def process_snapshot(self, abspath, filename):
        filetype = filename.split('_')
        fp = open(os.path.join(abspath, filename))
        blob = json.load(fp)
        fp.close()
        if filetype[0] == 'a':
            posts = blob['posts']
            nav = blob['nav']
            start_hit = False
            queue_file_list = []
            queue_list = []
            for sube in posts:
                utctime = int(sube['created_utc'])
                sttime = time.strftime('%Y%m%d', time.gmtime(utctime))
                if (int(sttime) > int(dateend)):
                    continue
                elif (int(sttime) < int(datestart)):
                    start_hit = True
                    break
                else:
                    queue_list.append('p,'+sube['id'])
            queue_file_list.append(self.write_to_queue('\n'.join(queue_list), 'tmp_p_'))
            if start_hit is not True:
                if nav['after'] is not None:
                    queue_file_list.append(self.write_to_queue('a,'+nav['after'], 'tmp_a_'))
            return queue_file_list
        elif filetype[0] == 'p':
            post = blob['post']
            comments = blob['comments']
            self.store.store_snapshot(post, comments)
            
            if crawl_author:
                queue_file_list = []
                if post['author'] not in self.author_list and post['author'] not in self.skip_list:
                    queue_file_list.append(self.write_to_queue('u,'+post['author'], 'tmp_u_'))
                    self.author_list.append(post['author'])
                for comment in comments:
                    if comment['author'] not in self.author_list and comment['author'] not in self.skip_list:
                        queue_file_list.append(self.write_to_queue('u,'+comment['author'], 'tmp_u_'))
                        self.author_list.append(comment['author'])
                return queue_file_list
        return []

    def process_snapshots(self, abspath, filename_list):
        post_tuples = []
        for filename in filename_list:
            filetype = filename.split('_')
            fp = open(os.path.join(abspath, filename))
            blob = json.load(fp)
            fp.close()
            post_tuples.append( (blob['post'], blob['comments']) )
        self.store.store_batch_snapshot(post_tuples)
            
        if crawl_author:
                queue_file_list = []

    def run(self):
        seedfile = self.seed()
        os.rename(os.path.join(tmpdir, 'dp', 'queue', seedfile), os.path.join(tmpdir, 'server', 'queue', seedfile))
        sleepcount = 0
        while True:
            for filename in os.listdir(os.path.join(tmpdir, 'dp', 'staging')):
                sleepcount = 0
                self.store.open()
                prefix = filename.split('.')[0]
                absfilename = os.path.join(tmpdir, 'dp', 'staging', filename)
                abspath = os.path.join(tmpdir, 'dp', 'staging', prefix)

                os.mkdir(abspath)
                tar = None
                try:
                    tar = tarfile.open(absfilename)
                    tar.extractall(abspath)
                    tar.close()
                except:
                    print 'Probably empty tar'
                    os.unlink(absfilename)
                    shutil.rmtree(abspath)
                    continue

                if debug:
                    os.rename(absfilename, os.path.join(tmpdir, 'dp', 'archive', filename))
                else:
                    os.unlink(absfilename)

                post_snapshots = []
                print 'Server >> '+str(os.listdir(abspath))
                for jsonfile in os.listdir(abspath):
                    filetype = jsonfile.split('_')
                    # format of request:
                    # | a | <pid>
                    # | p | <pid>
                    # | u | <username> | <after>
                    queue_list = ''
                    if filetype[0] == 'a':
                        queue_list = self.process_snapshot(abspath, jsonfile)
                    elif filetype[0] == 'p':
                        post_snapshots.append(jsonfile)
                    elif filetype[0] == 'u':
                        queue_list = self.process_author(abspath, jsonfile)
                    for queue_file in queue_list:
                        print queue_file
                        print os.path.join(tmpdir, 'dp', 'queue', queue_file), os.path.join(tmpdir, 'server', 'queue', queue_file)
                        os.rename(os.path.join(tmpdir, 'dp', 'queue', queue_file), os.path.join(tmpdir, 'server', 'queue', queue_file))
                        print 'Server << '+queue_file
                self.process_snapshots(abspath, post_snapshots)
                # cleanup dir
                shutil.rmtree(abspath)
            else:
                time.sleep(0.2)
                if sleepcount < 10:
                    sleepcount += 1
                else:
                    self.store.close()

class ServerHandler(SocketServer.StreamRequestHandler):
    def dump_to_staging(self, data):
        fh, filename = tempfile.mkstemp(dir=os.path.join(tmpdir, 'server', 'staging'))
        os.close(fh)
        fp = open(filename, 'w')
        fp.write(data)
        fp.close()
        os.renames(filename, filename+'.tar.gz')
        return os.path.split(filename)[1]

    def handle(self):
        client = self.rfile.readline().strip()
        print 'Connection from '+str(self.client_address)+', self-reported '+client
        queue_dir = os.path.join(tmpdir, 'server', 'queue')
        local_dir = os.path.join(tmpdir, 'client', client)

        try:
            os.mkdir(local_dir)
        except OSError:
            pass
        
        if debug:
            print 'Created local dir '+local_dir
        
        dirlist = os.listdir(queue_dir)
        if len(dirlist) == 0:
            if debug:
                print 'No data for client '+client
            self.wfile.write('\n\n')
            self.wfile.flush()
            self.wfile.close()
            return
        
        if debug:
            print 'Building up queue for '+client
        if type(dirlist) == 'list':
            dirlist = sorted(dirlist, reverse=True)
        for filename in dirlist:
            try:
                os.rename(os.path.join(queue_dir, filename), os.path.join(local_dir, filename))
                break
            except:
                pass

        dirlist = os.listdir(local_dir)
        for filename in dirlist:
            fp = open(os.path.join(local_dir, filename), 'r')
            for line in fp:
                line = line.strip()
                self.wfile.write(line+'\n')
                self.wfile.flush()
            fp.close()
            print client+' << '+filename
            os.unlink(os.path.join(local_dir, filename))
        print client+' << EOF'
        self.wfile.write('\n\n')
        self.wfile.flush()
        if debug:
            print 'Flushed data to '+client
        result = self.rfile.read()
        self.wfile.close()
        filename = self.dump_to_staging(result)
        print client+' >> '+filename
        os.rename(os.path.join(tmpdir, 'server', 'staging', filename+'.tar.gz'), os.path.join(tmpdir, 'dp', 'staging', filename+'.tar.gz'))
        print 'DP << '+filename

class Server(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    def run(self):
        self.serve_forever()

class Client:
    sock = None
    reddit = None
    
    def download_a(self, after=None):
        download_dir = os.path.join(tmpdir, 'client', 'staging')
        posts, nav = self.reddit.getListing('all', after)
        blob = { 'posts': posts, 'nav': nav }
        fp = open(os.path.join(download_dir, 'a_'+after), 'w')
        fp.write(json.dumps(blob))
        fp.close()
        return True

    def download_p(self, pid):
        download_dir = os.path.join(tmpdir, 'client', 'staging')
        self.reddit.updateToken()
        post, comments = self.reddit.getPost(pid)
        blob = { 'post': post, 'comments': comments }
        fp = open(os.path.join(download_dir, 'p_'+pid), 'w')
        fp.write(json.dumps(blob))
        fp.close()
        return True

    def download_u(self, user):
        download_dir = os.path.join(tmpdir, 'client', 'staging')
        url = 'http://www.reddit.com/user/'+user+'.json'
        filename = 'u_'+user
        data = self.download_get(url, os.path.join(download_dir, filename), True)
        if data == '':
            return False
        blob = json.loads(data)
        nav = parser.extract_listing_nav(blob)
        while nav['after'] is not None:
            newurl = url+'?after='+nav['after']
            filename = 'u_'+user+'_'+nav['after']
            data = self.download_get(newurl, os.path.join(download_dir, filename), True)
            if data == '':
                return False
            blob = json.loads(data)
            nav = parser.extract_listing_nav(blob)
        return True
    
    def download_req(self, req):
        # format of request:
        # | a | <pid>
        # | p | <pid>
        # | u | <username> | <after>
        res = True
        if req[0] == 'a':
            res = self.download_a(req[1])
        elif req[0] == 'p':
            res = self.download_p(req[1])
        elif req[0] == 'u':
            res = self.download_u(req[1])
        return res

    def download_data(self, reqlist):
        for req in reqlist:
            self.download_req(req)
            print '  -- '+str(req)
    
    def connect(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        self.sock = sock
        fp = sock.makefile('rb+')
        print 'Connected to '+host+':'+str(port)
        return fp
    
    def close(self):
        self.sock.close()
        self.sock = None
    
    def cleanup(self):
        download_dir = os.path.join(tmpdir, 'client', 'staging')
        for entry in os.listdir(download_dir):
            os.unlink(os.path.join(download_dir, entry))
        os.unlink(os.path.join(tmpdir, 'client', 'archive.tar.gz'))

    def targz(self):
        return shutil.make_archive(os.path.join(tmpdir, 'client', 'archive'), 'gztar', os.path.join(tmpdir, 'client', 'staging'))

    def run(self, host, port):
        self.reddit = Reddit(creds.key, creds.secret, creds.username, creds.password, creds.redirect_uri)
        self.reddit.updateToken()
        self.reddit.testAccess()
        sleeptime = 0
        while True:
            if sleeptime > 10:
                time.sleep(10)
            elif sleeptime > 1:
                time.sleep(1)
            # Connect to host:port, get the fp
            fp = self.connect(host, port)
        
            # Send hostname of client over initially
            hostname = socket.getfqdn()
            fp.write(hostname+'\n')
            fp.flush()
            if debug:
                print 'Sent hostname'
        
            # Recv all the urls
            reqlist = []
            newline = False
            while True:
                line = fp.readline()
                line = line.strip()
                if line != '':
                    reqlist.append(line.split(','))
                else:
                    if newline == True:
                        break
                    newline = True
                fp.flush()
        
            print host+' >> '+str(reqlist)
            # See if any urls were sent, close if zero
            if len(reqlist) == 0:
                if debug:
                    print 'No requests'
                self.close()
                sleeptime += 1
                continue
            sleeptime = 0
        
            if debug:
                print 'Downloading requests'
            # Download all the urls otherwise
            self.download_data(reqlist)
        
            # targzip the data
            targz = self.targz()
        
            # Send the data
            targz_fp = open(targz, 'rb')
            targz_data = targz_fp.read()
            fp.write(targz_data)
            fp.flush()
            print host+' << archive.tar.gz'
            self.close()
            self.cleanup()

def setup_server(port):
    print 'Running server component on port '+str(port)
    data_setup()
    server = Server(('0.0.0.0', port), ServerHandler)
    print 'Setup server'
    server.run()

def setup_client(hostname, port):
    print 'Running client component'
    data_setup()
    client = Client()
    client.run(hostname, port)

def setup_dp():
    print 'Running data processing component'
    data_setup()
    dp = DP()
    dp.run()

def data_setup():
    dirs = {
        'dp': {
            'staging',
            'archive',
            'queue',
        },
        'server': {
            'staging',
            'snapshot',
            'author',
            'queue',
            'client',
        },
        'client': {
            'staging'
        }
    }
    try:
        os.mkdir(tmpdir)
    except OSError:
        pass
    for d1 in dirs:
        try:
            os.mkdir(os.path.join(tmpdir, d1))
        except OSError:
            pass
        for d2 in dirs[d1]:
            try:
                os.mkdir(os.path.join(tmpdir, d1, d2))
            except OSError:
                pass

def run(params=dict()):
    if 'server' in params:
        setup_server(params['server']['port'])
    elif 'client' in params:
        setup_client(params['client']['hostname'], params['client']['port'])
    elif 'dp' in params:
        setup_dp()
    else:
        print 'Nothing to do'
