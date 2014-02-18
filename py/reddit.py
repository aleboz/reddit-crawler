import creds
import rauth
import requests
import re
import json
import pprint
from hashlib import sha1
from random import random
import parser
import time
import sys

user_agent = '/u/worldwise001\'s reddit crawler (github.com/worldwise001/reddit-crawler)'

auth_url = 'https://ssl.reddit.com/api/v1/authorize'
access_url = 'https://ssl.reddit.com/api/v1/access_token'
base_url = 'https://oauth.reddit.com/api/'

class Reddit:
    service = None
    cookies = None
    key = None
    secret = None
    username = None
    password = None
    state = None
    code = None
    refresh_token = None
    access_token = None

    def __init__(self, key=None, secret=None, username=None, password=None, redirect_uri=None):
        self.key = key
        self.secret = secret
        self.username = username
        self.password = password
        self.redirect_uri = redirect_uri
        if self.login() == 200:
            self.authorize()

    def request(self, httptype, url, params):
        status_code = 0
        data = None
        while (status_code != 200):
            if status_code == 404:
                print 'Page %s not found!' % (url)
                return data
            t = int(time.time())
            if t % 2 == 0:
                print '  %s %s %s' % (httptype, url, str(params))
                headers = { 'user-agent': user_agent }
                if httptype == 'POST':
                    r = requests.post(url, data=params, headers=headers)
                elif httptype == 'GET':
                    r = requests.get(url, params=params, headers=headers)
                status_code = r.status_code
                try:
                    data = r.text
                except:
                    data = r.content
                print '    '+str(status_code)+' - '+str(len(data))
            time.sleep(1)
            sys.stdout.flush()
        return data

    def api_request(self, httptype, endpoint, params):
        status_code = 0
        data = None
        while (status_code != 200):
            if status_code == 404:
                print 'Page %s%s not found!' % (base_url, endpoint)
                return data
            t = int(time.time())
            if t % 2 == 0:
                print '  %s %s%s %s' % (httptype, base_url, endpoint, str(params))
                headers = { 'user-agent': user_agent }
                s = self.service.get_session(self.access_token)
                if httptype == 'POST':
                    r = s.post(endpoint, data=params, headers=headers)
                elif httptype == 'GET':
                    r = s.get(endpoint, data=params, headers=headers)
                status_code = r.status_code
                try:
                    data = r.text
                except:
                    data = r.content
                print '    '+str(status_code)+' - '+str(len(data))
            time.sleep(1)
            sys.stdout.flush()
        return data

    def getPost(self, pid):
        url = 'http://www.reddit.com/comments/%s.json' % (pid)
        data = self.request('GET', url, dict())
        if data == None:
            return None
        blob = json.loads(data)
        post = parser.extract_post(blob)
        comments = parser.extract_post_comments(blob)
        comments_list = parser.extract_post_comments_missing(blob)
        while len(comments_list) > 0:
            comments_sublist = []
            if len(comments_list) > 20:
                comments_sublist = comments_list[:20]
                comments_list = comments_list[20:]
            else:
                comments_sublist = comments_list
                comments_list = []
            comments_extra = self.getComments(post['id'], comments_sublist)
            if comments_extra is not None:
                comments.extend(comments_extra)
        return (post, comments)

    def getComments(self, pid, cids):
        params = { 'api_type': 'json', 'children': ','.join(cids), 'link_id': 't3_'+pid }
        data = self.api_request('POST', 'morechildren', params)
        if data == None:
            return None
        blob = json.loads(data)
        comments = parser.extract_api_comments(blob)
        return comments

    def getListing(self, subreddit, after=None, count=100):
        url = 'http://www.reddit.com/r/%s/new.json' % (subreddit)
        params = { 'limit': count }
        if after is not None and after != '':
            params['after'] = after
        data = self.request('GET', url, params)
        if data == None:
            return None
        blob = json.loads(data)
        posts = parser.extract_listing_elements(blob)
        nav = parser.extract_listing_nav(blob)
        return (posts, nav)

    def clearState(self):
        self.service = None
        self.cookies = None
        self.code = None
        self.access_token = None
        self.refresh_token = None

    def login(self):
        data = { 'op': 'login-main',
                 'user': self.username,
                 'passwd': self.password
               }
        headers = { 'User-Agent': user_agent }
        r = requests.post('https://ssl.reddit.com/post/login', data=data, headers=headers)
        if r.status_code == 200 and len(r.history) > 0:
            self.cookies = requests.utils.dict_from_cookiejar(r.history[0].cookies)
        else:
            self.clearState()
        return r.status_code

    def authorize(self):
        if self.cookies == None:
            return 0
        oauth = rauth.OAuth2Service(self.key,
                                     self.secret,
                                     authorize_url=auth_url,
                                     access_token_url=access_url,
                                     base_url=base_url)
        self.state = sha1(str(random())).hexdigest()

        params = {'scope': 'identity,read',
                  'response_type': 'code',
                  'redirect_uri': self.redirect_uri,
                  'state': self.state,
                  'duration': 'permanent'}
        headers = { 'User-Agent': user_agent }
    
        authorize_url = oauth.get_authorize_url(**params)
        r = requests.get(authorize_url, headers=headers, cookies=self.cookies)
        if r.status_code != 200:
            self.clearState()
            return r.status_code

        data = { 'uh': re.search('name="uh" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'client_id': re.search('name="client_id" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'redirect_uri': re.search('name="redirect_uri" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'scope': re.search('name="scope" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'state': re.search('name="state" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'duration': re.search('name="duration" value="([^"]*)', r.text).group(1).encode('ascii'),
                 'authorize': 'Allow'
               }

        r = requests.post('https://ssl.reddit.com/api/v1/authorize', data=data, headers=headers, cookies=self.cookies, verify=False)
        if r.status_code != 200:
            self.clearState()
            return r.status_code

        url_split1 = r.url.split('?')
        domain = url_split1[0]
        url_split2 = url_split1[1].split('&')
        params = dict()
        for entry in url_split2:
            url_split3 = entry.split('=')
            params[url_split3[0].encode('ascii')] = url_split3[1].encode('ascii')
        if domain != self.redirect_uri or params['state'] != self.state:
            self.clearState()
            return 0
        self.code = params['code']
        self.service = oauth
        return r.status_code

    def updateToken(self):
        if self.cookies == None or self.state == None or self.code == None:
            return 0
        if self.refresh_token == None:
            data = { 'code': self.code,
                     'grant_type': 'authorization_code',
                     'redirect_uri': self.redirect_uri }
            r = self.service.get_raw_access_token(data=data, auth=(self.key, self.secret))
            blob = json.loads(r.text)
            self.access_token = blob['access_token']
            self.refresh_token = blob['refresh_token']
        else:
            data = { 'code': self.code,
                     'grant_type': 'refresh_token',
                     'refresh_token': self.refresh_token,
                     'redirect_uri': self.redirect_uri }
            r = self.service.get_raw_access_token(data=data, auth=(self.key, self.secret))
            blob = json.loads(r.text)
            self.access_token = blob['access_token']

    def testAccess(self):
        s = self.service.get_session(self.access_token)
        user = s.get('v1/me').json()
        pprint.pprint(user)

if __name__ == '__main__':
    reddit = Reddit(creds.key, creds.secret, creds.username, creds.password, creds.redirect_uri)
    reddit.updateToken()
    reddit.testAccess()
    print reddit.access_token
    reddit.testAccess()
    print reddit.access_token
    reddit.updateToken()
    posts, nav = reddit.getListing('all')
    print len(posts), nav
    #post, comments = reddit.getPost('1y28lk')

