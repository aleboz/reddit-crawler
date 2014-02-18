__author__ = 'sharvey'

import sys
import crawler
import shutil
import os


if __name__ == '__main__':
    if sys.argv[1] == 'client':
        crawler.setup_client('localhost', 8000)
    elif sys.argv[1] == 'server':
        crawler.setup_server(8000)
    elif sys.argv[1] == 'dp':
        crawler.setup_dp()
    else:
        shutil.rmtree('/collection/sharvey/reddit')
        os.mkdir('/collection/sharvey/reddit')
        for entry in ['client', 'dp', 'server']:
            shutil.rmtree('/tmp/reddit/'+entry)
