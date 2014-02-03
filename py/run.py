__author__ = 'sharvey'

import sys
import crawler


if __name__ == '__main__':
    if sys.argv[1] == 'client':
        crawler.setup_client('localhost', 8000)
    elif sys.argv[1] == 'server':
        crawler.setup_server(8000)
    else:
        crawler.setup_dp()
