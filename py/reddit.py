import creds
import rauth
import requests
import re
import json
import pprint
from hashlib import sha1
from random import random

user_agent = '/u/worldwise001\'s reddit crawler (github.com/worldwise001/reddit-crawler)'

auth_url = 'https://ssl.reddit.com/api/v1/authorize'
access_url = 'https://ssl.reddit.com/api/v1/access_token'
base_url = 'https://oauth.reddit.com/api/v1/'

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

    def getPost(self, pid):
        pass

    def getComments(self, pid, cids):
        pass

    def getListing(self, subreddit, after=None, count=100):
        pass

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
        user = s.get('me').json()
        pprint.pprint(user)

reddit = Reddit(creds.key, creds.secret, creds.username, creds.password, 'https://metaether.net')
reddit.updateToken()
reddit.testAccess()
print reddit.access_token
reddit.updateToken()
reddit.testAccess()
print reddit.access_token
